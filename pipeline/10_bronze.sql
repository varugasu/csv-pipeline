COPY (
 SELECT *
  FROM read_csv("lake/landing/transactions.csv", header = true)
)
TO 'lake/bronze/transactions.parquet'
(FORMAT 'parquet', COMPRESSION 'zstd');
