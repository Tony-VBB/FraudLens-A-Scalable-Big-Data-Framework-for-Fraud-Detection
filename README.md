# FraudLens — Big Data Fraud Detection

## Project overview
FraudLens analyzes financial transactions from the PaySim dataset using
Apache Hadoop, Pig, Hive, and Apache Spark MLlib.

## Project pipeline

### Part 1 — Data cleaning with Pig
- Input: PaySim CSV dataset in HDFS.
- Script: pig/clean_paysim.pig
- Cleaned HDFS output: /fraudlens/processed/paysim_cleaned
- Cleaned records: 6,101,255
- Fraudulent records: 7,820

### Part 2 — Analytics with Hive
- Script: hive/fraud_analysis.hql
- Database: fraudlens
- Table: paysim_cleaned
- Results log: hive/fraud_analysis_output.log

### Part 3 — Fraud classification with Spark MLlib
- Model: weighted Logistic Regression
- Script: spark/fraud_model.py
- Runner: spark/run_fraud_model.sh
- Input: hdfs:///fraudlens/processed/paysim_cleaned
- Saved HDFS model: /fraudlens/models/spark_fraud_model
- Saved HDFS predictions: /fraudlens/results/spark_fraud_predictions
- Local metrics: spark/results/metrics.txt
- Exported model and predictions: artifacts/

## Spark model evaluation

| Metric | Result |
|---|---:|
| Accuracy | 86.0727% |
| Fraud precision | 0.8317% |
| Fraud recall | 90.4762% |
| Fraud F1-score | 1.6482% |
| ROC-AUC | 0.962220 |
| PR-AUC | 0.300007 |

Confusion matrix:
- True negatives: 1,049,613
- False positives: 169,917
- False negatives: 150
- True positives: 1,425

## Interpretation
The model identifies a high proportion of fraudulent test transactions,
but its low precision means many legitimate transactions are flagged.
This reflects the severe class imbalance in the dataset and the trade-off
between detecting fraud and generating false alerts.

## Main software environment used
- Ubuntu Linux
- Hadoop 3.3.6
- Apache Pig 0.17.0
- Apache Hive 4.0.1
- Apache Spark 3.5.9
- Java 8
- Python 3.12

The working Spark environment required a compatibility patch for
PySpark's use of distutils.version.LooseVersion. See the Spark setup
notes before recreating the environment on another computer.

## Data requirements
The raw PaySim dataset is not included in the portable project archive.
Obtain the dataset separately and upload it to HDFS at the path expected
by the Pig cleaning script before rerunning the full pipeline.

The exported model and prediction files are included as artifacts when
the HDFS export step has completed successfully.
