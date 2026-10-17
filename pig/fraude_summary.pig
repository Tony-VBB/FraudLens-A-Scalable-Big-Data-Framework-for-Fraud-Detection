-- FraudLens Part 1
-- PaySim fraud-class summary using Apache Pig

clean_data = LOAD '/fraud/processed/paysim_cleaned/part-r-00000'
USING PigStorage(',')
AS (
    step:int,
    type:chararray,
    amount:double,
    nameOrig:chararray,
    oldbalanceOrg:double,
    newbalanceOrig:double,
    originBalanceChange:double,
    nameDest:chararray,
    oldbalanceDest:double,
    newbalanceDest:double,
    destBalanceChange:double,
    isFraud:int,
    isFlaggedFraud:int
);

fraud_groups = GROUP clean_data BY isFraud;

fraud_summary = FOREACH fraud_groups GENERATE
    group AS isFraud,
    COUNT(clean_data) AS transaction_count;

STORE fraud_summary
INTO '/fraud/processed/paysim_fraud_summary'
USING PigStorage(',');