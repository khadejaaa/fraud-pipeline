"""Spark ML : lit le Parquet (HDFS), entraine, evalue, log dans MLflow, enregistre le meilleur modele.

Utilisation (defauts = reseau docker-compose) :
  python train.py
  python train.py --data-path /tmp/sample --mlflow-uri file:/tmp/mlruns   # test local
"""
import argparse, csv, json, os
from datetime import datetime
import mlflow, mlflow.spark
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from mlflow import MlflowClient

ap = argparse.ArgumentParser()
ap.add_argument("--data-path", default=os.getenv("DATA_PATH", "hdfs://namenode:9000/lake/transactions/"))
ap.add_argument("--minio-endpoint", default=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
ap.add_argument("--mlflow-uri", default=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
ap.add_argument("--reports-dir", default=os.getenv("REPORTS_DIR", "/opt/project/reports"))
ap.add_argument("--model-name", default="fraud-detector")
args = ap.parse_args()

# Le client MLflow envoie les artefacts directement a MinIO (--default-artifact-root s3://mlflow/) :
# endpoint et identifiants sont donc necessaires cote client (surtout dans un pod Kubeflow, ou rien n'est injecte).
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", args.minio_endpoint)
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

mlflow.set_tracking_uri(args.mlflow_uri)
mlflow.set_experiment("fraud-detection")

builder = SparkSession.builder.appName("fraud-train").master("local[*]").config("spark.sql.shuffle.partitions", "8")
if args.data_path.startswith("s3a://"):
    builder = (builder
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(args.data_path).withColumn("label", F.col("is_fraud").cast("double"))
n_pos = df.filter("label = 1").count()
n_neg = df.filter("label = 0").count()
print(f"Lignes: {n_pos + n_neg} | fraudes: {n_pos} | normales: {n_neg}")
if n_pos < 20:
    raise SystemExit("Pas assez de fraudes, laisse tourner le generateur plus longtemps.")

# Gestion du desequilibre : ponderation des classes
ratio = n_neg / n_pos
df = df.withColumn("weight", F.when(F.col("label") == 1, ratio).otherwise(1.0))
train, test = df.randomSplit([0.8, 0.2], seed=42)

idx = StringIndexer(inputCol="merchant", outputCol="merchant_idx", handleInvalid="keep")
ohe = OneHotEncoder(inputCols=["merchant_idx"], outputCols=["merchant_vec"])
asm = VectorAssembler(
    inputCols=["log_amount", "amount", "hour", "is_night", "is_foreign", "device_new", "merchant_vec"],
    outputCol="features")

models = {
    "logreg": LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight", maxIter=50),
    "random_forest": RandomForestClassifier(featuresCol="features", labelCol="label",
                                            weightCol="weight", numTrees=60, maxDepth=7, seed=42),
}
auc_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
auc_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

best = (None, -1)
results, best_run_id, best_pred = {}, None, None
for name, clf in models.items():
    with mlflow.start_run(run_name=name) as run:
        model = Pipeline(stages=[idx, ohe, asm, clf]).fit(train)
        pred = model.transform(test)
        tp = pred.filter("label=1 AND prediction=1").count()
        fp = pred.filter("label=0 AND prediction=1").count()
        fn = pred.filter("label=1 AND prediction=0").count()
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        metrics = {"precision": precision, "recall": recall, "f1": f1,
                   "auc_pr": auc_pr.evaluate(pred), "auc_roc": auc_roc.evaluate(pred)}
        mlflow.log_param("model", name)
        mlflow.log_param("class_ratio_neg_pos", round(ratio, 2))
        mlflow.log_metrics(metrics)
        mlflow.spark.log_model(model, "model")
        print(name, {k: round(v, 3) for k, v in metrics.items()})
        results[name] = metrics
        if metrics["auc_pr"] > best[1]:
            best = (name, metrics["auc_pr"])
            best_run_id, best_pred = run.info.run_id, pred

print(f"Meilleur modele: {best[0]} (AUC-PR={best[1]:.3f})")

# Deploiement : le meilleur modele est enregistre dans le registre MLflow (alias "champion")
try:
    mv = mlflow.register_model(f"runs:/{best_run_id}/model", args.model_name)
    MlflowClient().set_registered_model_alias(args.model_name, "champion", mv.version)
    print(f"Modele '{args.model_name}' version {mv.version} enregistre avec l'alias 'champion'")
except Exception as e:
    print("Enregistrement dans le registre impossible :", e)

# Export des resultats (utilises par le rapport)
os.makedirs(args.reports_dir, exist_ok=True)
with open(os.path.join(args.reports_dir, "model_metrics.json"), "w", encoding="utf-8") as f:
    json.dump({"trained_at": datetime.now().isoformat(timespec="seconds"),
               "rows": n_pos + n_neg, "frauds": n_pos, "best_model": best[0],
               "best_auc_pr": best[1], "models": results}, f, ensure_ascii=False, indent=2)

# Exemple de predictions du meilleur modele
cols = ["transaction_id", "user_id", "amount", "merchant", "label", "prediction", "proba_fraude"]
sample = (best_pred.withColumn("proba_fraude", F.round(vector_to_array("probability")[1], 4))
          .orderBy(F.desc("proba_fraude")).select(*cols).limit(20).collect())
with open(os.path.join(args.reports_dir, "predictions_sample.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(cols)
    for r in sample:
        w.writerow([r[c] for c in cols])
print("Exports ecrits dans", args.reports_dir)
spark.stop()
