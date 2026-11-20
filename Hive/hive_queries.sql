-- ============================================================================
-- ITS66904 - Big Data Technologies
-- Applying Hadoop Technologies to E-Commerce Order Data
-- Apache Hive implementation for the PREPARED dataset
--
-- Source: Dataset/Prepared/cleaned_ecommerce.csv (29 columns, produced by
--         Notebook/Ecommerce_BigData_Preprocessing.ipynb)
--
-- IMPORTANT CONVENTIONS (established in the preprocessing notebook):
--   1. One row-level Order_ID = one transaction/order. The Order_ID prefix is
--      a customer order-group (1:1 with Customer_ID) and is NOT an order.
--      All order counts therefore use COUNT(DISTINCT `Order_ID`).
--   2. ASSIGNMENT TOTAL SALES = Quantity * Price = `Gross_Amount`.
--      The required "Total sales by product category" query therefore uses
--      `Gross_Amount`. `Net_Amount` / `Total_Amount` (gross minus discount)
--      remain available for the separate net-sales supplementary queries.
--   3. NOTE: the current MapReduce SalesByCategory job still uses NET sales.
--      Hive gross-sales results are therefore NOT expected to match that job
--      until the MapReduce job is also changed to gross sales.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Table definition — matches the prepared CSV schema EXACTLY
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    `Order_ID`                  STRING,
    `Customer_ID`               STRING,
    `Date`                      STRING,          -- ISO YYYY-MM-DD text
    `Age`                       INT,
    `Gender`                    STRING,
    `City`                      STRING,
    `Product_Category`          STRING,
    `Unit_Price`                DECIMAL(12,2),
    `Quantity`                  INT,
    `Discount_Amount`           DECIMAL(12,2),
    `Total_Amount`              DECIMAL(14,2),   -- NET sales
    `Payment_Method`            STRING,
    `Device_Type`               STRING,
    `Session_Duration_Minutes`  INT,
    `Pages_Viewed`              INT,
    `Is_Returning_Customer`     BOOLEAN,         -- exported as true/false
    `Delivery_Time_Days`        INT,
    `Customer_Rating`           INT,
    `Customer_Order_Group_ID`   STRING,          -- derived: identifier prefix
    `Order_Line_Number`         INT,             -- derived: identifier suffix
    `Gross_Amount`              DECIMAL(14,2),   -- derived: price * quantity
    `Net_Amount`                DECIMAL(14,2),   -- derived alias of Total_Amount
    `Year`                      INT,             -- derived
    `Month`                     INT,             -- derived
    `Year_Month`                STRING,          -- derived YYYY-MM
    `Is_Unit_Price_Outlier`     BOOLEAN,         -- derived, rows retained
    `Is_Discount_Outlier`       BOOLEAN,
    `Is_Total_Amount_Outlier`   BOOLEAN,
    `Is_Delivery_Time_Outlier`  BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load the SAME prepared file used by the MapReduce jobs.
-- Adjust the local path, or omit LOCAL after placing the file in HDFS.
LOAD DATA LOCAL INPATH
    'D:/Ecommerce_BigData_Project/Ecommerce_BigData_Project/Dataset/Prepared/cleaned_ecommerce.csv'
    OVERWRITE INTO TABLE orders;
-- LOAD DATA INPATH '/user/hive/warehouse/input/cleaned_ecommerce.csv'
--     OVERWRITE INTO TABLE orders;

-- Quick schema/row check (expected: 17049 rows, 29 columns).
SELECT COUNT(*) AS row_count FROM orders;
DESCRIBE orders;


-- ----------------------------------------------------------------------------
-- 1. REQUIRED: Total orders
-- ----------------------------------------------------------------------------
SELECT COUNT(DISTINCT `Order_ID`) AS total_orders FROM orders;
-- COUNT(*) returns the same value because row-level Order_ID is unique.
-- This confirms the definition used by OrderCountByCategory / OrderCountByPayment.


-- ----------------------------------------------------------------------------
-- 2. REQUIRED: Total sales by product category
--    Assignment formula: Total Sales = Quantity * Price = Gross_Amount
-- ----------------------------------------------------------------------------
SELECT
    `Product_Category`,
    ROUND(SUM(`Gross_Amount`), 2) AS total_sales
FROM orders
GROUP BY `Product_Category`
ORDER BY total_sales DESC;


-- ----------------------------------------------------------------------------
-- 3. REQUIRED: Orders by city
-- ----------------------------------------------------------------------------
SELECT
    `City`,
    COUNT(DISTINCT `Order_ID`) AS total_orders
FROM orders
GROUP BY `City`
ORDER BY total_orders DESC;


-- ----------------------------------------------------------------------------
-- 4. REQUIRED: Most frequent payment method
-- ----------------------------------------------------------------------------
SELECT
    `Payment_Method`,
    COUNT(DISTINCT `Order_ID`) AS total_orders
FROM orders
GROUP BY `Payment_Method`
ORDER BY total_orders DESC
LIMIT 1;


-- ----------------------------------------------------------------------------
-- 5. SUPPLEMENTARY: Monthly net sales
-- ----------------------------------------------------------------------------
SELECT
    `Year_Month`,
    ROUND(SUM(`Net_Amount`), 2) AS monthly_net_sales,
    COUNT(DISTINCT `Order_ID`) AS monthly_orders
FROM orders
GROUP BY `Year_Month`
ORDER BY `Year_Month`;


-- ----------------------------------------------------------------------------
-- 6. SUPPLEMENTARY: Average transaction (net) amount by category
-- ----------------------------------------------------------------------------
SELECT
    `Product_Category`,
    ROUND(AVG(`Net_Amount`), 2) AS avg_net_transaction_amount
FROM orders
GROUP BY `Product_Category`
ORDER BY avg_net_transaction_amount DESC;


-- ----------------------------------------------------------------------------
-- 7. SUPPLEMENTARY: Quantity sold by category
-- ----------------------------------------------------------------------------
SELECT
    `Product_Category`,
    SUM(`Quantity`) AS units_sold
FROM orders
GROUP BY `Product_Category`
ORDER BY units_sold DESC;


-- ----------------------------------------------------------------------------
-- 8. SUPPLEMENTARY: Net sales by city
-- ----------------------------------------------------------------------------
SELECT
    `City`,
    ROUND(SUM(`Net_Amount`), 2) AS net_sales
FROM orders
GROUP BY `City`
ORDER BY net_sales DESC;


-- ----------------------------------------------------------------------------
-- 9. SUPPLEMENTARY: Customer distribution by device type
-- ----------------------------------------------------------------------------
SELECT
    `Device_Type`,
    COUNT(DISTINCT `Customer_ID`) AS distinct_customers,
    COUNT(DISTINCT `Order_ID`) AS orders,
    ROUND(100.0 * COUNT(DISTINCT `Customer_ID`) /
          (SELECT COUNT(DISTINCT `Customer_ID`) FROM orders), 2) AS pct_of_customers
FROM orders
GROUP BY `Device_Type`
ORDER BY distinct_customers DESC;
-- NOTE: a customer may use more than one device, so the percentages may sum
-- to more than 100%. This is expected and should be reported as such.


-- ----------------------------------------------------------------------------
-- 10. SUPPLEMENTARY: Average customer rating by product category
-- ----------------------------------------------------------------------------
SELECT
    `Product_Category`,
    ROUND(AVG(`Customer_Rating`), 3) AS avg_rating,
    COUNT(*) AS rated_transactions
FROM orders
GROUP BY `Product_Category`
ORDER BY avg_rating DESC;


-- ----------------------------------------------------------------------------
-- 11. SUPPLEMENTARY: Overall net business summary
-- ----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT `Order_ID`) AS total_orders,
    COUNT(DISTINCT `Customer_ID`) AS total_customers,
    COUNT(DISTINCT `Customer_Order_Group_ID`) AS customer_order_groups,
    SUM(`Quantity`) AS units_sold,
    ROUND(SUM(`Gross_Amount`), 2) AS gross_sales,
    ROUND(SUM(`Discount_Amount`), 2) AS total_discounts,
    ROUND(SUM(`Net_Amount`), 2) AS net_sales
FROM orders;
-- gross_sales - total_discounts must equal net_sales. This is a consistency check.


-- ----------------------------------------------------------------------------
-- 12. VALIDATION QUERIES — compare against MapReduce outputs
-- ----------------------------------------------------------------------------
-- The order-count queries below ARE equivalent to the MapReduce jobs.
-- The sales query uses GROSS sales per the assignment formula. The current
-- MapReduce SalesByCategory job uses NET sales, so that one comparison is
-- EXPECTED TO FAIL until the MapReduce job is changed to Gross_Amount too.
-- Export each result to TSV, then run Validation/compare_results.py.

-- Assignment gross-sales baseline (NOT equivalent to current SalesByCategory)
SELECT `Product_Category` AS category, ROUND(SUM(`Gross_Amount`), 2) AS value
FROM orders GROUP BY `Product_Category` ORDER BY category;

-- Equivalent to OrderCountByCategory
SELECT `Product_Category` AS category,
       COUNT(DISTINCT `Order_ID`) AS value
FROM orders GROUP BY `Product_Category` ORDER BY category;

-- Equivalent to OrderCountByPayment
SELECT `Payment_Method` AS category,
       COUNT(DISTINCT `Order_ID`) AS value
FROM orders GROUP BY `Payment_Method` ORDER BY category;

-- Financial-accuracy re-check inside Hive (expected result: 0 rows).
SELECT `Order_ID`
FROM orders
WHERE ABS(`Total_Amount` - (`Gross_Amount` - `Discount_Amount`)) > 0.01;
