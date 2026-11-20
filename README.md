# E-Commerce Big Data Analysis

Hadoop MapReduce and Apache Hive analysis of 17,049 e-commerce order lines.

Java 11 · Hadoop 3.4.3 · Hive 3.1.3 · Maven · Python 3

## Contents

| Path | Description |
|---|---|
| `Dataset/` | Source CSV and prepared 29-column CSV files |
| `Notebook/` | Preprocessing notebook |
| `HadoopMapReduce/` | Java sources and `pom.xml` |
| `Hive/` | SQL, result exporter, setup notes |
| `MapReduce_Output/` | Three job outputs |
| `Hive_Output/` | Three exported result TSVs |
| `Validation/` | Validator and expected baselines |
| `Screenshots/` | Execution evidence |
| `Report/` | Assignment report |

## Results

| Check | Value |
|---|---|
| Rows loaded | 17,049 |
| Gross sales (`Quantity × Price`) | 22,968,870.51 |
| Discounts | 1,189,817.92 |
| Net sales | 21,779,052.59 |
| Validation | 6 / 6 PASS |

MapReduce `SalesByCategory` reports **net** sales; the Hive required query reports **gross** sales. Both are checked against their own baselines and are not claimed to be equal.

## Run

```powershell
mvn -f HadoopMapReduce\pom.xml clean package
hdfs dfs -put -f Dataset/Prepared/cleaned_ecommerce_noheader.csv /ecommerce/

& 'C:\apache-hive-3.1.3-bin\bin\hive.cmd' -f 'Hive\hive_queries.sql'
.\Hive\export_hive_results.ps1
python Validation\compare_results.py
```

Paths and Hive setup are machine-specific; see `Hive/HIVE_SETUP_NOTES.md`.
