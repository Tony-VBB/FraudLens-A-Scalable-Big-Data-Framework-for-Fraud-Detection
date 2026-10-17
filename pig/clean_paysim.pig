-- FraudLens: PaySim data cleaning using Apache Pig

-- Load the raw PaySim CSV from HDFS.
raw_data = LOAD '/fraudlens/raw/PS_20174392719_1491204439457_log.csv'
USING PigStorage(',')
AS (
    step:chararray,
    type:chararray,
    amount:chararray,
    nameOrig:chararray,
    oldbalanceOrg:chararray,
    newbalanceOrig:chararray,
    nameDest:chararray,
    oldbalanceDest:chararray,
    newbalanceDest:chararray,
    isFraud:chararray,
    isFlaggedFraud:chararray
);

-- Remove the header and records with invalid numeric values.
valid_data = FILTER raw_data BY
    step matches '[0-9]+' AND
    amount matches '[0-9]+(\\.[0-9]+)?' AND
    oldbalanceOrg matches '[0-9]+(\\.[0-9]+)?' AND
    newbalanceOrig matches '[0-9]+(\\.[0-9]+)?' AND
    oldbalanceDest matches '[0-9]+(\\.[0-9]+)?' AND
    newbalanceDest matches '[0-9]+(\\.[0-9]+)?' AND
    isFraud matches '[01]' AND
    isFlaggedFraud matches '[01]' AND
    type IS NOT NULL AND
    nameOrig IS NOT NULL AND
    nameDest IS NOT NULL;

-- Convert numeric strings and derive balance-change features.
enriched_data = FOREACH valid_data GENERATE
    (int)step AS step,
    type AS type,
    (double)amount AS amount,
    nameOrig AS nameOrig,
    (double)oldbalanceOrg AS oldbalanceOrg,
    (double)newbalanceOrig AS newbalanceOrig,
    ((double)newbalanceOrig - (double)oldbalanceOrg) AS originBalanceChange,
    nameDest AS nameDest,
    (double)oldbalanceDest AS oldbalanceDest,
    (double)newbalanceDest AS newbalanceDest,
    ((double)newbalanceDest - (double)oldbalanceDest) AS destBalanceChange,
    (int)isFraud AS isFraud,
    (int)isFlaggedFraud AS isFlaggedFraud;

-- Remove exact duplicate records.
clean_data = DISTINCT enriched_data;

-- Store cleaned data in HDFS.
STORE clean_data
INTO '/fraudlens/processed/paysim_cleaned'
USING PigStorage(',');
