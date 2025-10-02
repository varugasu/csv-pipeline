bronze:
	duckdb -c ".read pipeline/10_bronze.sql"

silver:
	duckdb dev.duckdb -c ".read pipeline/20_silver.sql"

gold:
	duckdb dev.duckdb -c ".read pipeline/30_gold.sql"
