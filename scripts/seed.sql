PRAGMA threads=8;                 -- tune to your CPU cores
SET memory_limit='8GB';           -- ~50–70% of RAM is fine

-- You can tweak these knobs:
CREATE OR REPLACE TABLE _params AS SELECT
  5000000   AS n_accounts,       -- 5M accounts
  100000    AS n_merchants,      -- 100k merchants
  24        AS n_months,         -- ~last 24 months
  TIMESTAMP '2023-10-01' AS start_ts;

-- Helper to pick from finite sets via hashed modulo (cheap & deterministic)
CREATE OR REPLACE MACRO pick10(h, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9) AS (
  CASE ABS(HASH(h)) % 10
    WHEN 0 THEN a0 WHEN 1 THEN a1 WHEN 2 THEN a2 WHEN 3 THEN a3 WHEN 4 THEN a4
    WHEN 5 THEN a5 WHEN 6 THEN a6 WHEN 7 THEN a7 WHEN 8 THEN a8 ELSE a9 END
);

-- Build transactions (vectorized; no Python loops)
CREATE OR REPLACE TABLE bank_tx AS
WITH P AS (SELECT * FROM _params),
R AS (
  SELECT rn
  FROM range(1000000) t(rn)            -- 100M rows
),
S AS (
  SELECT
    rn + 1                                   AS txn_id,
    1 + (ABS(HASH(rn))         % (SELECT n_accounts  FROM P)) AS account_id,
    1 + (ABS(HASH(rn*37))      % (SELECT n_merchants FROM P)) AS merchant_id,

    -- Timestamp across the last N months, minute resolution
    (SELECT start_ts FROM P)
      + ((rn % ((SELECT n_months FROM P) * 30 * 24 * 60))::BIGINT) * INTERVAL 1 MINUTE AS txn_ts,

    -- YYYY-MM for easy CSV sharding later
    strftime(
      (SELECT start_ts FROM P)
      + ((rn % ((SELECT n_months FROM P) * 30 * 24 * 60))::BIGINT) * INTERVAL 1 MINUTE,
      '%Y-%m'
    ) AS ym,

    -- Currency skew: BRL 40%, USD 40%, EUR 20%
    CASE (ABS(HASH(rn*11)) % 10)
      WHEN 0 THEN 'EUR' WHEN 1 THEN 'EUR'
      WHEN 2 THEN 'BRL' WHEN 3 THEN 'BRL' WHEN 4 THEN 'BRL' WHEN 5 THEN 'BRL'
      WHEN 6 THEN 'USD' WHEN 7 THEN 'USD' WHEN 8 THEN 'USD' ELSE 'USD'
    END AS currency,

    -- Channel distribution
    pick10(rn*13,'pos','pos','pos','pos','online','online','online','transfer','atm','atm') AS channel,

    -- Transaction type (mostly debits)
    pick10(rn*17,'debit','debit','debit','debit','debit','debit','debit','debit','debit','credit') AS txn_type,

    -- Status: posted 97%, pending 2.8%, reversed 0.2%
    CASE
      WHEN (ABS(HASH(rn*19)) % 1000) < 2   THEN 'reversed'
      WHEN (ABS(HASH(rn*19)) % 1000) < 30  THEN 'pending'
      ELSE 'posted'
    END AS status,

    -- Merchant Category Code-ish label
    pick10(rn*23,'grocery','restaurant','fuel','electronics','clothing','pharmacy','ride_hail','subscription','utilities','travel') AS mcc,

    -- Amount distribution: lots of small POS, some larger outliers; debits negative
    CASE
      WHEN (ABS(HASH(rn*29)) % 100) < 96
        THEN ROUND(((random()*90.0)+5.0)::DECIMAL(18,2), 2)       -- 5–95
      ELSE
        ROUND(((random()*4900.0)+100.0)::DECIMAL(18,2), 2)        -- 100–5000
    END AS amt_abs,

    -- Country skew
    pick10(rn*31,'BR','BR','BR','US','US','US','GB','DE','JP','BR') AS country
  FROM R
),
T AS (
  SELECT
    txn_id,
    account_id,
    merchant_id,
    txn_ts,
    ym,
    currency,
    channel,
    txn_type,
    status,
    mcc,
    -- Signed amount: debits negative, credits positive
    CASE WHEN txn_type='debit' THEN -amt_abs ELSE amt_abs END AS amount,
    -- Some lightweight strings
    ('Merchant ' || CAST(merchant_id AS VARCHAR)) AS merchant_name,
    ('POS PURCHASE - ' || mcc || ' - ' || CAST(merchant_id AS VARCHAR)) AS description,
    country
  FROM S
)
SELECT * FROM T;

-- Indexes in DuckDB are mostly for constraint semantics; we keep it simple.
-- Optionally persist distinct yms for export scripts:
CREATE OR REPLACE TABLE _months AS
SELECT DISTINCT ym FROM bank_tx ORDER BY ym;
