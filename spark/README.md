# FraudLens — Part 3: Spark MLlib Fraud Prediction

## Purpose
Train and evaluate a fraud-classification model using the cleaned PaySim
transactions produced by the Pig ETL stage and registered for Hive analysis.

## Model
Weighted Logistic Regression using Apache Spark MLlib. Class weights compensate
for the imbalance between legitimate and fraudulent transactions. Account IDs
and the existing isFlaggedFraud field are excluded from model features.

## Features
Transaction step, amount, origin and destination balances, balance-change
features, and one-hot encoded transaction type.

## Evaluation
The script calculates accuracy, fraud precision, fraud recall, fraud F1-score,
ROC-AUC, PR-AUC, and the test-set confusion matrix.

## Outputs
- HDFS model: `/fraudlens/models/spark_fraud_model`
- HDFS predictions: `/fraudlens/results/spark_fraud_predictions`
- Local metrics: `~/FraudLens/spark/results/metrics.txt`
- Run log: `~/FraudLens/spark/results/spark_run.log`

## Run
`bash ~/FraudLens/spark/run_fraud_model.sh`

The runner refuses to overwrite existing model or prediction output paths.
