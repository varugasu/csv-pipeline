seed:
	 duckdb -c ".read scripts/seed.sql" \
	 	-c "COPY (SELECT * FROM bank_tx) TO 'lake/landing/transactions.csv' (HEADER, DELIMITER ',');"

bronze:
	duckdb -c ".read pipeline/10_bronze.sql"

silver:
	duckdb dev.duckdb -c ".read pipeline/20_silver.sql"

gold:
	duckdb dev.duckdb -c ".read pipeline/30_gold.sql"

pipeline: seed bronze silver gold
