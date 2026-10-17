-- FraudLens: PaySim fraud analysis using Apache Hive

CREATE DATABASE IF NOT EXISTS fraudlens;
USE fraudlens;

-- External table over the cleaned PaySim data in HDFS.
CREATE EXTERNAL TABLE IF NOT EXISTS paysim_cleaned (
    step INT,
    type STRING,
    amount DOUBLE,
    nameOrig STRING,
    oldbalanceOrg DOUBLE,
    newbalanceOrig DOUBLE,
    originBalanceChange DOUBLE,
    nameDest STRING,
    oldbalanceDest DOUBLE,
    newbalanceDest DOUBLE,
    destBalanceChange DOUBLE,
    isFraud INT,
    isFlaggedFraud INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/fraudlens/processed/paysim_cleaned';

-- 1. Overall transaction and fraud statistics.
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legitimate_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / COUNT(*),
        4
    ) AS fraud_percentage
FROM paysim_cleaned;

-- 2. Transaction volume and fraud rate by transaction type.
SELECT
    type,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / COUNT(*),
        4
    ) AS fraud_percentage
FROM paysim_cleaned
GROUP BY type
ORDER BY fraud_transactions DESC;

-- 3. Fraudulent transaction amount statistics.
SELECT
    COUNT(*) AS fraud_count,
    ROUND(MIN(amount), 2) AS minimum_amount,
    ROUND(MAX(amount), 2) AS maximum_amount,
    ROUND(AVG(amount), 2) AS average_amount,
    ROUND(SUM(amount), 2) AS total_fraud_amount
FROM paysim_cleaned
WHERE isFraud = 1;

-- 4. Compare flagged and unflagged transactions.
SELECT
    isFlaggedFraud,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legitimate_transactions
FROM paysim_cleaned
GROUP BY isFlaggedFraud
ORDER BY isFlaggedFraud;

-- 5. Fraud statistics by transaction type and flagged status.
SELECT
    type,
    isFlaggedFraud,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count
FROM paysim_cleaned
GROUP BY type, isFlaggedFraud
ORDER BY type, isFlaggedFraud;

-- 6. Fraudulent transactions with no observed origin balance remaining.
SELECT
    COUNT(*) AS fraud_count,
    SUM(
        CASE
            WHEN newbalanceOrig = 0 THEN 1
            ELSE 0
        END
    ) AS frauds_with_zero_origin_balance,
    SUM(
        CASE
            WHEN newbalanceDest = 0 THEN 1
            ELSE 0
        END
    ) AS frauds_with_zero_destination_balance
FROM paysim_cleaned
WHERE isFraud = 1;
