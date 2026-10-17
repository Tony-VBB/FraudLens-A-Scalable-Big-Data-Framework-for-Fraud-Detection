import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from pyspark.storagelevel import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

INPUT_PATH = "hdfs:///fraudlens/processed/paysim_cleaned"
PREDICTIONS_PATH = "hdfs:///fraudlens/results/spark_fraud_predictions"
MODEL_PATH = "hdfs:///fraudlens/models/spark_fraud_model"
LOCAL_RESULTS = os.path.expanduser("~/FraudLens/spark/results/metrics.txt")

schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("originBalanceChange", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("destBalanceChange", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True)
])

spark = (
    SparkSession.builder
    .appName("FraudLens-SparkMLlib")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def require_new_hdfs_path(path):
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    hpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(hpath):
        raise RuntimeError(
            "Output already exists; refusing to overwrite: " + path
        )

try:
    print("\n=== FRAUDLENS PART 3: SPARK MLlib ===")
    print("Reading cleaned PaySim data from:", INPUT_PATH)

    require_new_hdfs_path(PREDICTIONS_PATH)
    require_new_hdfs_path(MODEL_PATH)

    raw = (
        spark.read
        .option("header", "false")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(INPUT_PATH)
    )

    # Exclude account IDs and the existing flagged-fraud indicator.
    # These are omitted to reduce high-cardinality features and avoid
    # depending on the dataset's pre-existing flag.
    data = (
        raw.select(
            "step", "type", "amount",
            "oldbalanceOrg", "newbalanceOrig",
            "originBalanceChange",
            "oldbalanceDest", "newbalanceDest",
            "destBalanceChange", "isFraud"
        )
        .dropna()
        .filter(F.col("isFraud").isin(0, 1))
        .withColumn("label", F.col("isFraud").cast("double"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total = data.count()
    print("Valid records used:", total)

    if total == 0:
        raise RuntimeError("No valid records were loaded. Check the HDFS input.")

    fraud_total = data.filter(F.col("label") == 1.0).count()
    legit_total = data.filter(F.col("label") == 0.0).count()
    fraud_rate = 100.0 * fraud_total / total

    print("Legitimate records:", legit_total)
    print("Fraud records:", fraud_total)
    print("Fraud percentage: %.5f%%" % fraud_rate)

    if fraud_total == 0 or legit_total == 0:
        raise RuntimeError("Both fraud and legitimate classes are required.")

    train, test = data.randomSplit([0.8, 0.2], seed=42)

    train_count = train.count()
    test_count = test.count()
    train_fraud = train.filter(F.col("label") == 1.0).count()
    train_legit = train.filter(F.col("label") == 0.0).count()

    if train_fraud == 0 or train_legit == 0 or test_count == 0:
        raise RuntimeError("Train/test split does not contain the required classes.")

    # Inverse-frequency weights help the model learn the rare fraud class.
    fraud_weight = float(train_count) / (2.0 * train_fraud)
    legit_weight = float(train_count) / (2.0 * train_legit)

    train = train.withColumn(
        "classWeight",
        F.when(F.col("label") == 1.0, F.lit(fraud_weight))
         .otherwise(F.lit(legit_weight))
    )

    print("\nTraining records:", train_count)
    print("Testing records:", test_count)
    print("Training class weights calculated.")

    type_indexer = StringIndexer(
        inputCol="type",
        outputCol="typeIndex",
        handleInvalid="keep"
    )

    encoder = OneHotEncoder(
        inputCols=["typeIndex"],
        outputCols=["typeVector"],
        handleInvalid="keep"
    )

    numeric_features = [
        "step", "amount",
        "oldbalanceOrg", "newbalanceOrig",
        "originBalanceChange",
        "oldbalanceDest", "newbalanceDest",
        "destBalanceChange"
    ]

    assembler = VectorAssembler(
        inputCols=numeric_features + ["typeVector"],
        outputCol="rawFeatures",
        handleInvalid="skip"
    )

    scaler = StandardScaler(
        inputCol="rawFeatures",
        outputCol="features",
        withMean=False,
        withStd=True
    )

    classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="classWeight",
        maxIter=30,
        regParam=0.01,
        elasticNetParam=0.0
    )

    pipeline = Pipeline(stages=[
        type_indexer, encoder, assembler, scaler, classifier
    ])

    print("\nTraining weighted Logistic Regression with Spark MLlib...")
    fitted_model = pipeline.fit(train)
    print("Model training completed.")

    print("Saving trained model to HDFS...")
    fitted_model.write().save(MODEL_PATH)

    print("Generating test-set predictions...")
    predictions = fitted_model.transform(test).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    predictions.count()

    evaluator_roc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )

    auc_roc = evaluator_roc.evaluate(predictions)
    auc_pr = evaluator_pr.evaluate(predictions)

    # Derive confusion-matrix values for the positive class (fraud).
    confusion_rows = predictions.groupBy("label", "prediction").count().collect()
    confusion = {
        (int(row["label"]), int(row["prediction"])): int(row["count"])
        for row in confusion_rows
    }

    tn = confusion.get((0, 0), 0)
    fp = confusion.get((0, 1), 0)
    fn = confusion.get((1, 0), 0)
    tp = confusion.get((1, 1), 0)

    accuracy = float(tp + tn) / max(1, tp + tn + fp + fn)
    precision = float(tp) / max(1, tp + fp)
    recall = float(tp) / max(1, tp + fn)
    f1 = (
        2.0 * precision * recall / (precision + recall)
        if precision + recall > 0 else 0.0
    )

    metrics = [
        "FraudLens - Spark MLlib evaluation",
        "Model: weighted Logistic Regression",
        "Input: " + INPUT_PATH,
        "Total valid records: %d" % total,
        "Legitimate records: %d" % legit_total,
        "Fraud records: %d" % fraud_total,
        "Fraud percentage: %.6f%%" % fraud_rate,
        "Training records: %d" % train_count,
        "Testing records: %d" % test_count,
        "",
        "Confusion matrix (test set):",
        "True negatives: %d" % tn,
        "False positives: %d" % fp,
        "False negatives: %d" % fn,
        "True positives: %d" % tp,
        "",
        "Accuracy: %.6f" % accuracy,
        "Fraud precision: %.6f" % precision,
        "Fraud recall: %.6f" % recall,
        "Fraud F1-score: %.6f" % f1,
        "ROC-AUC: %.6f" % auc_roc,
        "PR-AUC: %.6f" % auc_pr,
        "",
        "Saved model: " + MODEL_PATH,
        "Saved predictions: " + PREDICTIONS_PATH
    ]

    os.makedirs(os.path.dirname(LOCAL_RESULTS), exist_ok=True)
    with open(LOCAL_RESULTS, "w") as result_file:
        result_file.write("\n".join(metrics) + "\n")

    # Save compact, CSV-compatible predictions to HDFS.
    probability_of_fraud = F.udf(
        lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0,
        DoubleType()
    )

    (
        predictions.select(
            "step", "type", "amount", "isFraud",
            F.col("prediction").cast("integer").alias("predictedFraud"),
            probability_of_fraud(F.col("probability")).alias("fraudProbability")
        )
        .write
        .mode("errorifexists")
        .option("header", "true")
        .csv(PREDICTIONS_PATH)
    )

    print("\n=== MODEL EVALUATION ===")
    for line in metrics:
        print(line)

    print("\nSUCCESS: Part 3 completed.")
    print("Metrics saved locally:", LOCAL_RESULTS)

finally:
    try:
        data.unpersist()
    except Exception:
        pass
    spark.stop()
