#!/usr/bin/env python3
"""
Compare equivalent MapReduce and Hive results for the e-commerce project.

The script recomputes expected results directly from the PREPARED dataset
(the single source used by both engines), then compares:
  - MapReduce NET sales (SalesByCategory uses Total_Amount / Net_Amount)
  - Hive GROSS sales (the required query uses Quantity * Price = Gross_Amount)
  - Both engines' order counts

MapReduce part-r-00000 files and Hive TSV exports use key <TAB> value.
No result is forced to match, and no direct MapReduce-vs-Hive sales equality
claim is made: the two sales measures are deliberately different. A mismatch
is reported with the differing keys and typical causes to investigate
(parsing, data types, gross vs net sales, row count vs parent-order count).

Usage:
    python Validation/compare_results.py

Optional overrides:
    --project DIR        project root (default: parent of this script's folder)
    --mr-root DIR        MapReduce output root (default: <project>/MapReduce_Output)
    --hive-root DIR      Hive output root (default: <project>/Hive_Output)

Hadoop/Hive execution is NOT performed by this script. It only validates
outputs that already exist in a separate Hadoop/Hive environment.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

SALES_TOLERANCE = 0.01


def expected_results(prepared_csv: Path) -> dict[str, dict[str, float]]:
    df = pd.read_csv(prepared_csv)
    net = pd.to_numeric(df["Net_Amount"], errors="raise")
    total = pd.to_numeric(df["Total_Amount"], errors="raise")
    gross = pd.to_numeric(df["Gross_Amount"], errors="raise")
    unit_price = pd.to_numeric(df["Unit_Price"], errors="raise")
    quantity = pd.to_numeric(df["Quantity"], errors="raise")

    # The prepared dataset was validated in the notebook; assert the invariants
    # so a wrong file cannot silently produce wrong expected values.
    if not ((net - total).abs() <= SALES_TOLERANCE).all():
        raise ValueError("Net_Amount does not match Total_Amount in the prepared file.")
    if not ((gross - unit_price * quantity).abs() <= SALES_TOLERANCE).all():
        raise ValueError("Gross_Amount does not equal Unit_Price * Quantity in the prepared file.")

    return {
        # MapReduce SalesByCategory reports NET sales.
        "sales_by_category": df.groupby("Product_Category")
                               .apply(lambda g: round(float(g["Net_Amount"].sum()), 2),
                                      include_groups=False)
                               .to_dict(),
        # Hive's required "Total sales" query reports GROSS sales.
        "gross_sales_by_category": df.groupby("Product_Category")
                               .apply(lambda g: round(float(g["Gross_Amount"].sum()), 2),
                                      include_groups=False)
                               .to_dict(),
        "order_count_by_category": df.groupby("Product_Category")["Order_ID"]
                                     .nunique().to_dict(),
        "order_count_by_payment": df.groupby("Payment_Method")["Order_ID"]
                                    .nunique().to_dict(),
    }


def read_key_value(path: Path) -> dict[str, float] | None:
    if path is None or not path.exists():
        return None
    result: dict[str, float] = {}
    with path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.rstrip("\r\n")
            if not line.strip() or "\t" not in line:
                continue
            key, value = line.split("\t", 1)
            key = key.strip()
            if key.lower() in {"category", "product_category", "payment_method", "key"}:
                continue  # optional Hive header
            try:
                result[key] = float(value.strip())
            except ValueError:
                continue
    return result or None


def compare(name: str, expected: dict[str, float],
            actual: dict[str, float] | None, source: str,
            tolerance: float = 0.0) -> tuple[str, str]:
    if actual is None:
        return "NOT AVAILABLE", f"{name}: no output file found at {source}."
    keys = sorted(set(expected) | set(actual))
    diffs = []
    for key in keys:
        exp = expected.get(key)
        act = actual.get(key)
        if exp is None or act is None:
            diffs.append(f"{key}: expected={exp}, actual={act}")
        elif abs(float(exp) - float(act)) > tolerance:
            diffs.append(f"{key}: expected={exp}, actual={act}")
    if diffs:
        detail = f"{name}: MISMATCH\n    " + "\n    ".join(diffs[:15])
        detail += ("\n    Check: net vs gross sales, numeric parsing/data types, "
                   "row count vs parent-order count, row filtering.")
        return "FAIL", detail
    return "PASS", f"{name}: {len(expected)} keys agree (tolerance={tolerance})."


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path,
                        default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--mr-root", type=Path, default=None)
    parser.add_argument("--hive-root", type=Path, default=None)
    args = parser.parse_args()

    project = args.project.resolve()
    prepared = project / "Dataset" / "Prepared" / "cleaned_ecommerce.csv"
    mr_root = args.mr_root or (project / "MapReduce_Output")
    hive_root = args.hive_root or (project / "Hive_Output")

    if not prepared.exists():
        print("ERROR: prepared dataset not found:", prepared)
        return 2

    expected = expected_results(prepared)

    # Save the independently computed expected values for manual inspection.
    expected_dir = project / "Validation" / "expected"
    expected_dir.mkdir(parents=True, exist_ok=True)
    for name, values in expected.items():
        table = pd.Series(values, name="value").rename_axis("key").reset_index()
        if "sales_by_category" in name:
            table["value"] = table["value"].map(lambda v: f"{v:.2f}")
        else:
            table["value"] = table["value"].map(lambda v: str(int(v)))
        table.to_csv(expected_dir / f"{name}.tsv", sep="\t", index=False)
    print("Expected results written to:", expected_dir)

    # MapReduce output paths (produced only in a Hadoop environment).
    mr_paths = {
        "sales_by_category": mr_root / "sales_by_category" / "part-r-00000",
        "order_count_by_category": mr_root / "category_count" / "part-r-00000",
        "order_count_by_payment": mr_root / "payment_count" / "part-r-00000",
    }
    # Hive TSV export paths (produced only in a Hive environment).
    hive_paths = {
        "sales_by_category": hive_root / "sales_by_category.tsv",
        "order_count_by_category": hive_root / "category_count.tsv",
        "order_count_by_payment": hive_root / "payment_count.tsv",
    }

    checks = []

    # Sales are compared against different baselines: MapReduce is net,
    # Hive is gross. A PASS from both does NOT mean the figures are equal.
    checks.append(compare("MapReduce sales_by_category (NET)",
                          expected["sales_by_category"],
                          read_key_value(mr_paths["sales_by_category"]),
                          "MapReduce_Output/" + mr_paths["sales_by_category"].parent.name +
                          " -- run the Hadoop MapReduce stage first", SALES_TOLERANCE))
    checks.append(compare("Hive sales_by_category (GROSS)",
                          expected["gross_sales_by_category"],
                          read_key_value(hive_paths["sales_by_category"]),
                          "Hive_Output/" + hive_paths["sales_by_category"].name +
                          " -- run the Hive stage first", SALES_TOLERANCE))

    for name in ("order_count_by_category", "order_count_by_payment"):
        checks.append(compare("MapReduce " + name, expected[name],
                              read_key_value(mr_paths[name]),
                              "MapReduce_Output/" + mr_paths[name].parent.name +
                              " -- run the Hadoop MapReduce stage first", 0.0))
        checks.append(compare("Hive " + name, expected[name],
                              read_key_value(hive_paths[name]),
                              "Hive_Output/" + hive_paths[name].name +
                              " -- run the Hive stage first", 0.0))

    print("\n================ VALIDATION REPORT ================")
    any_fail = False
    any_available = False
    for status, detail in checks:
        print(f"[{status:13s}] {detail}")
        any_fail |= status == "FAIL"
        any_available |= status != "NOT AVAILABLE"

    if not any_available:
        print("\nNo Hadoop or Hive outputs are present locally. This is EXPECTED:")
        print("Java/Hive jobs require a separate Hadoop environment and were not")
        print("executed here. The expected results above were computed from the")
        print("prepared CSV and can be used as the comparison baseline.")
    print("===================================================")
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
