CREATE OR REPLACE TABLE restaurant_merchant_sum AS
WITH src AS (
SELECT * FROM read_parquet('lake/silver/transactions.parquet')
),
cleaned AS (
SELECT merchant_id, SUM(amount) AS total_amount
FROM src
WHERE mcc = 'RESTAURANT'
GROUP BY merchant_id
)
SELECT * FROM cleaned;

COPY (SELECT * FROM restaurant_merchant_sum)
TO 'lake/gold/restaurant_merchant_sum.parquet' (FORMAT PARQUET, COMPRESSION 'zstd');
