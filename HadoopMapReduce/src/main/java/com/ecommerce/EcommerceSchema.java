package com.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Shared schema and safe parsing helpers for all e-commerce MapReduce jobs.
 *
 * The column order below MUST match the prepared dataset produced by
 * Ecommerce_BigData_Preprocessing.ipynb (cleaned_ecommerce*.csv). Keeping a
 * single definition avoids the hard-coded, inconsistent column indices used
 * by the original implementation.
 */
public final class EcommerceSchema {

    /** Exact column order of the prepared CSV (29 columns). */
    public static final String[] COLUMN_NAMES = {
        "Order_ID", "Customer_ID", "Date", "Age", "Gender", "City",
        "Product_Category", "Unit_Price", "Quantity", "Discount_Amount",
        "Total_Amount", "Payment_Method", "Device_Type",
        "Session_Duration_Minutes", "Pages_Viewed", "Is_Returning_Customer",
        "Delivery_Time_Days", "Customer_Rating", "Customer_Order_Group_ID",
        "Order_Line_Number", "Gross_Amount", "Net_Amount", "Year", "Month",
        "Year_Month", "Is_Unit_Price_Outlier", "Is_Discount_Outlier",
        "Is_Total_Amount_Outlier", "Is_Delivery_Time_Outlier"
    };

    public static final int IDX_ORDER_ID = 0;
    public static final int IDX_CUSTOMER_ID = 1;
    public static final int IDX_DATE = 2;
    public static final int IDX_CITY = 5;
    public static final int IDX_PRODUCT_CATEGORY = 6;
    public static final int IDX_UNIT_PRICE = 7;
    public static final int IDX_QUANTITY = 8;
    public static final int IDX_DISCOUNT_AMOUNT = 9;
    public static final int IDX_TOTAL_AMOUNT = 10;   // NET sales (validated)
    public static final int IDX_PAYMENT_METHOD = 11;
    public static final int IDX_NET_AMOUNT = 21;     // derived alias of Total_Amount

    /**
     * RFC-4180-style parsing with an explicit header mapping. Quoted values
     * containing commas (e.g. "Home, Garden") are handled correctly, unlike
     * String.split(",").
     */
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.builder()
            .setHeader(COLUMN_NAMES)
            .setSkipHeaderRecord(false)
            .setIgnoreSurroundingSpaces(true)
            .setIgnoreEmptyLines(true)
            .get();

    /** Counter names shared by every job for consistent monitoring. */
    public enum Counters {
        RECORDS_READ,
        RECORDS_PROCESSED,
        RECORDS_INVALID,
        INVALID_NUMERIC,
        FINANCIAL_MISMATCH
    }

    private EcommerceSchema() {
        // utility class
    }

    /**
     * Parse one input line into a validated record.
     * Returns null for blank lines, the CSV header, malformed records and
     * records with an unexpected number of fields. Callers count these as
     * invalid instead of failing the whole job.
     */
    public static CSVRecord parseLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }
        try (CSVParser parser = CSVParser.parse(line, CSV_FORMAT)) {
            for (CSVRecord record : parser) {
                if (record.size() != COLUMN_NAMES.length) {
                    return null;
                }
                if (isHeaderRecord(record)) {
                    return null;
                }
                return record;
            }
        } catch (IOException | RuntimeException e) {
            // Malformed CSV must not terminate a MapReduce task.
            return null;
        }
        return null;
    }

    private static boolean isHeaderRecord(CSVRecord record) {
        return "Order_ID".equals(record.get(IDX_ORDER_ID).trim());
    }

    /** Return a trimmed, non-empty text value or null when the field is blank. */
    public static String requiredText(CSVRecord record, int index) {
        String value = record.get(index);
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    /** Parse a non-negative integer field. Returns null when invalid. */
    public static Long parseNonNegativeInteger(CSVRecord record, int index) {
        String value = requiredText(record, index);
        if (value == null) {
            return null;
        }
        try {
            long parsed = Long.parseLong(value);
            return parsed >= 0 ? parsed : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parse a non-negative monetary field into integer cents. Using integer
     * arithmetic avoids floating-point drift when summing sales.
     */
    public static Long parseNonNegativeMoneyToCents(CSVRecord record, int index) {
        String value = requiredText(record, index);
        if (value == null) {
            return null;
        }
        try {
            BigDecimal amount = new BigDecimal(value);
            if (amount.signum() < 0) {
                return null;
            }
            return amount.setScale(2, RoundingMode.HALF_UP)
                         .movePointRight(2)
                         .longValueExact();
        } catch (NumberFormatException | ArithmeticException e) {
            return null;
        }
    }

    /** Format integer cents as a plain two-decimal string for output. */
    public static String formatCents(long cents) {
        return BigDecimal.valueOf(cents, 2).toPlainString();
    }
}
