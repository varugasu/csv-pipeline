CREATE OR REPLACE TABLE silver_tx AS
WITH src AS (
SELECT * FROM read_parquet('lake/bronze/transactions.parquet')
),
cleaned AS (
SELECT txn_id, account_id, merchant_id,
       UPPER(channel) AS channel,
       ABS(amount) AS amount,
       UPPER(txn_type) AS txn_type,
       UPPER(status) AS status,
       UPPER(mcc) AS mcc,
       UPPER(country) AS country,
       strftime(txn_ts, '%Y-%m-%dT%H:%M:%S.%fZ') as created_at
 FROM src
)
SELECT * FROM cleaned;

COPY (SELECT * FROM silver_tx) TO 'lake/silver/transactions.parquet' (FORMAT PARQUET);
