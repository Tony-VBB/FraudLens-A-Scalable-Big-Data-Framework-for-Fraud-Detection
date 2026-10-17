# FraudLens — Big Data Fraud Detection on PaySim

End-to-end fraud-detection pipeline on the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) mobile-money dataset:
**Hadoop (HDFS/YARN) → Pig (cleaning) → Hive (analytics) → Spark MLlib (Random Forest classifier).**

> **Status of this revision.** The code and documentation issues from `docs/AUDIT_REPORT.md` are fixed and the shell control flow was
> tested against stub tools (see `CHANGELOG.md` for each item and what remains environment-specific). The pipeline has **not** been re-run on a real cluster since the fixes, so the
> Random Forest has no metrics yet and the Hive log still reflects the *old* cleaning. Run the pipeline once and refresh them
> ([Section 7](#7-results)).

---

## 1. Pipeline

```
PaySim CSV (local: data/raw/paysim/)
   │  scripts/prepare_data.sh
   ▼
HDFS  /fraudlens/raw/PS_20174392719_1491204439457_log.csv
   │  Part 1  scripts/run_pig.sh → pig/clean_paysim.pig   (validate, cast, balance deltas, DISTINCT)
   ▼
HDFS  /fraudlens/processed/paysim_cleaned     (13 comma-separated columns, no header, _SUCCESS marker)
   ├─ Part 2  scripts/run_hive.sh → hive/fraud_analysis.hql  → hive/fraud_analysis_output.log
   └─ Part 3  spark/run_fraud_model.sh → spark/fraud_model.py (Random Forest)
                 → HDFS /fraudlens/models/spark_fraud_rf_model
                 → HDFS /fraudlens/results/spark_fraud_rf_predictions
                 → spark/results/metrics.txt
```

Every HDFS path comes from one file, `config/fraudlens.env`, and is handed to Pig (`-param`), Hive (`--hivevar`) and
Spark (environment variables), so the three stages cannot drift apart.

## 2. Repository layout

| Path | Purpose |
|---|---|
| `setup/install_dependencies.sh` | apt packages: OpenJDK 8, Python helpers (numpy, packaging, setuptools), SSH. Ubuntu only |
| `setup/configure_environment.sh` | Detects installed tools, writes `config/fraudlens.env` |
| `setup/verify_installation.sh` | Checks executables, Java 8, Python deps, Spark version, HDFS reachability |
| `scripts/start_services.sh` | Starts HDFS, YARN, Hive metastore, HiveServer2; aborts if HDFS is in safe mode |
| `scripts/prepare_data.sh` | Uploads the raw CSV to HDFS (never overwrites) |
| `scripts/run_pig.sh` · `run_hive.sh` | Stage wrappers (parameters from the config) |
| `scripts/run_all.sh` | Master runner; skips stages whose output is complete (`_SUCCESS`) |
| `scripts/check_pipeline.sh` | Read-only configuration and pipeline checker |
| `scripts/lib.sh` | Shared helpers (config loading, `hdfs_uri`, `hdfs_job_done`) |
| `pig/clean_paysim.pig` | Part 1 cleaning (parameters `RAW_PATH`, `OUT_PATH`) |
| `hive/fraud_analysis.hql` | Part 2 analytics (variable `HDFS_CLEAN_DIR`); `hive/fraud_analysis_output.log` = captured output |
| `spark/fraud_model.py`, `spark/run_fraud_model.sh` | Part 3 Random Forest training/evaluation and runner |
| `test/` | Pig smoke test (`pig -x local test/pig_test.pig`) + 2-row sample |
| `docs/` | `AUDIT_REPORT.md` (original findings) and `baseline_logreg/` (previous logistic-regression run) |
| `config/fraudlens.env` | **Generated**, machine-specific, git-ignored |

The repository can live in any directory: scripts locate themselves instead of assuming `~/FraudLens`.

## 3. Environment

| Component | Version |
|---|---|
| OS | Ubuntu 24.04 (Python 3.12 is the system Python) |
| Java | OpenJDK 8 |
| Hadoop | 3.3.6, pseudo-distributed, `fs.defaultFS = hdfs://localhost:9000` |
| Pig | 0.17.0 (MapReduce mode) |
| Hive | 4.0.1 (HiveServer2 + Beeline) |
| Spark | 3.5.9, `local[2]`, 2 GB driver (configurable) |
| Python | 3.12 with `numpy`, `packaging`, `setuptools` |

`setuptools` matters: Python 3.12 removed `distutils`, which PySpark 3.5 still imports (`LooseVersion`). Installing
`python3-setuptools` (done by `install_dependencies.sh`) supplies the shim; no patching of Spark is required.

### Installing the toolchain
`install_dependencies.sh` installs Java 8 and the Python packages. Hadoop, Pig, Hive and Spark are unpacked by hand into the
default locations the configure script looks for (or set `HADOOP_HOME`, `PIG_HOME`, `HIVE_HOME`, `SPARK_HOME` first):

| Tool | Default directory | Download |
|---|---|---|
| Hadoop 3.3.6 | `~/hadoop` | `archive.apache.org/dist/hadoop/common/hadoop-3.3.6/` |
| Pig 0.17.0 | `~/pig-0.17.0` | `archive.apache.org/dist/pig/pig-0.17.0/` |
| Hive 4.0.1 | `~/hive` | `archive.apache.org/dist/hive/hive-4.0.1/` |
| Spark 3.5.9 | `~/spark-3.5.9-bin-hadoop3` | `archive.apache.org/dist/spark/spark-3.5.9/` |

Configure Hadoop for pseudo-distributed mode (`core-site.xml`: `fs.defaultFS=hdfs://localhost:9000`; `hdfs-site.xml`; passwordless
SSH to localhost; `hdfs namenode -format` once) using the official single-node guide. Keep free disk space: the NameNode enters
safe mode when it runs low, which broke several earlier runs.

## 4. Configuration

```bash
bash setup/install_dependencies.sh
# unpack Hadoop/Pig/Hive/Spark (see above)
bash setup/configure_environment.sh        # writes config/fraudlens.env
source config/fraudlens.env
bash setup/verify_installation.sh
```

Key settings written to `config/fraudlens.env` (override by exporting the variable before running the configure script):

```
HDFS_RAW_DIR=/fraudlens/raw                      HDFS_CLEAN_DIR=/fraudlens/processed/paysim_cleaned
HDFS_MODEL_DIR=/fraudlens/models/spark_fraud_rf_model
HDFS_PREDICTIONS_DIR=/fraudlens/results/spark_fraud_rf_predictions
DATASET_FILENAME=PS_20174392719_1491204439457_log.csv
SPARK_MASTER=local[2]  SPARK_DRIVER_MEMORY=2g  SPARK_SHUFFLE_PARTITIONS=8
RF_NUM_TREES=40  RF_MAX_DEPTH=8  RF_MAX_BINS=32  RF_SUBSAMPLING_RATE=0.5  RF_MIN_INSTANCES_PER_NODE=10
HIVE_JDBC_URL=jdbc:hive2://localhost:10000/default
```

If you already have a `config/fraudlens.env` from the previous version, **regenerate it**: it lacks the `RF_*` settings and still
names the old logistic-regression output paths.

## 5. Running

```bash
mkdir -p data/raw/paysim && cp PS_20174392719_1491204439457_log.csv data/raw/paysim/
bash scripts/start_services.sh
bash scripts/run_all.sh                    # upload → Pig → Hive → Spark
```

Single stages: `scripts/prepare_data.sh`, `scripts/run_pig.sh`, `scripts/run_hive.sh`, `spark/run_fraud_model.sh`.

Nothing in HDFS is ever deleted or overwritten by the scripts. If an output already exists they either skip (complete, `_SUCCESS`
present) or stop and print the exact `hdfs dfs -rm -r ...` command to run if you really want to redo it. Re-cleaning with Pig makes
the Hive result and the Spark model stale; remove those too (`rm logs/hive_analysis_completed`, delete model/predictions dirs).

**Moving from the old pipeline:** the cleaned data in `/fraudlens/processed/paysim_cleaned` was produced by the old numeric filter,
which dropped rows containing scientific-notation values. To benefit from the fix, remove that directory and rerun:
`hdfs dfs -rm -r /fraudlens/processed/paysim_cleaned && rm -f logs/hive_analysis_completed && bash scripts/run_all.sh`.
The Hive table is external, so it reads the new data automatically.

## 6. Checking configuration and pipeline

```bash
source config/fraudlens.env
bash scripts/check_pipeline.sh
```

The checker is read-only and reports PASS/WARN/FAIL for:

1. **Configuration** — all variables set, Java 8, Python dependencies (numpy, packaging, setuptools).
2. **Cross-file consistency** — Pig/Hive/Spark take their paths from the config (no literal HDFS paths), no hard-coded home
   directories, Hive script and test data not git-ignored.
3. **Services** — HDFS reachable, safe mode off, YARN, HiveServer2 on port 10000.
4. **Artefacts** — raw data in HDFS, Pig output has `_SUCCESS` and 13 columns, rows dropped by cleaning (warns above 1 %),
   Random Forest model and predictions present, `metrics.txt` produced by the Random Forest.

Manual spot checks:

```bash
hdfs dfs -ls "$HDFS_CLEAN_DIR"                                   # _SUCCESS + part-* files
beeline -u "$HIVE_JDBC_URL" -e "SELECT COUNT(*), SUM(isFraud) FROM fraudlens.paysim_cleaned"
cat spark/results/metrics.txt
```

Expected after the filter fix (stock PaySim): **6,362,620 rows, 8,213 fraud, 16 flagged** if nothing legitimate is dropped.
This is the expectation to confirm on the first run, not a measured result.

### Troubleshooting (from the recorded Pig logs)

| Symptom | Cause / action |
|---|---|
| `Failed to create DataStorage` | HDFS is down → `scripts/start_services.sh` |
| `Your endpoint configuration is wrong ... UnsetHostnameOrPort` | `fs.defaultFS` / YARN addresses missing in the Hadoop XML configs |
| `SafeModeException ... Resources are low on NN` | free disk space, then `hdfs dfsadmin -safemode leave` |
| `Output directory ... already exists` | intended guard; remove the directory deliberately or skip the stage |

## 7. Results

### Random Forest (current model)
Not yet measured with the fixed pipeline. After a run, `spark/results/metrics.txt` contains the confusion matrix, accuracy,
precision, recall, F1, ROC-AUC, PR-AUC, precision/recall at thresholds 0.3/0.5/0.7/0.9, and the feature importances.
Paste the final figures here.

Model: `RandomForestClassifier` in a pipeline `StringIndexer → OneHotEncoder → VectorAssembler → RandomForest`, 40 trees, depth 8,
32 bins, 50 % row subsampling per tree, inverse-frequency class weights, seed 42, 80/20 random split. No scaler is used
(trees are scale-invariant). Features: `step`, `amount`, origin/destination old and new balances, the two derived balance
changes, and the one-hot transaction type. Account IDs and `isFlaggedFraud` are excluded.

### Previous baseline: weighted Logistic Regression (old cleaning, 6,101,255 rows)
Kept in `docs/baseline_logreg/` for comparison only.

| Metric | Value |
|---|---:|
| Accuracy | 98.07 % |
| Fraud precision | 0.93 % |
| Fraud recall | 90.48 % |
| Fraud F1 | 1.65 % |
| ROC-AUC | 0.9622 |
| PR-AUC | 0.3000 |

Confusion matrix: TN 1,049,613 · FP 169,917 · FN 150 · TP 1,425. Accuracy is not meaningful at a 0.13 % fraud rate; compare
models on PR-AUC and on precision at a fixed recall.

## 8. Known limitations

- `oldbalance*/newbalance*` are tightly tied to how PaySim simulates fraud (7,808 of 7,820 fraud rows in the old data have
  `newbalanceOrig = 0`); results will not transfer to real transaction data.
- `step` is a time counter; consider dropping it or using a time-based split instead of a random split.
- A 40-tree forest on ~5 M rows with `local[2]` and 2 GB takes a while; lower `RF_NUM_TREES`/`RF_MAX_DEPTH` or raise
  `SPARK_DRIVER_MEMORY` in `config/fraudlens.env` to trade accuracy for speed.
- Pig 0.17.0 targets Hadoop 2.x. The first recorded Pig failure (`Job in state DEFINE instead of RUNNING`) may come from that combination with Hadoop 3.3.6; later runs succeeded, but keep it in mind if it reappears.

## 9. License

MIT — see `LICENSE`.
