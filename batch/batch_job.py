"""Traitement BATCH : lit les transactions accumulees dans HDFS, nettoie, agrege, calcule des stats.

Sorties :
  - HDFS  : tables d'agregation (Parquet) dans /lake/gold/
  - Local : reports/batch_summary.json (utilise pour le rapport)

Utilisation (defauts = reseau docker-compose) :
  python batch/batch_job.py
  python batch/batch_job.py --data-path /tmp/sample --gold-path /tmp/gold --reports-dir /tmp/reports   # test local
"""
import argparse, json, os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--data-path", default=os.getenv("DATA_PATH", "hdfs://namenode:9000/lake/transactions/"))
ap.add_argument("--gold-path", default=os.getenv("GOLD_PATH", "hdfs://namenode:9000/lake/gold/"))
ap.add_argument("--reports-dir", default=os.getenv("REPORTS_DIR", "/opt/project/reports"))
args = ap.parse_args()

spark = SparkSession.builder.appName("fraud-batch").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Lecture
raw = spark.read.parquet(args.data_path)
n_raw = raw.count()

# 2. Nettoyage : doublons, valeurs invalides, valeurs manquantes
clean = (raw.dropDuplicates(["transaction_id"])
         .filter(F.col("amount") > 0)
         .dropna(subset=["user_id", "merchant", "country", "event_ts"]))
clean.cache()
n_clean = clean.count()
print(f"Lignes brutes: {n_raw} | apres nettoyage: {n_clean} | supprimees: {n_raw - n_clean}")

# 3. Statistiques globales
g = clean.agg(
    F.count("*").alias("nb_tx"),
    F.sum("is_fraud").alias("nb_fraudes"),
    F.round(F.avg("amount"), 2).alias("montant_moyen"),
    F.round(F.max("amount"), 2).alias("montant_max"),
    F.countDistinct("user_id").alias("nb_clients"),
    F.min("event_ts").alias("debut"),
    F.max("event_ts").alias("fin"),
).collect()[0]
nb_tx, nb_fraudes = int(g["nb_tx"]), int(g["nb_fraudes"] or 0)

# 4. Agregations
by_user = (clean.groupBy("user_id")
           .agg(F.count("*").alias("nb_tx"),
                F.round(F.sum("amount"), 2).alias("montant_total"),
                F.round(F.avg("amount"), 2).alias("montant_moyen"),
                F.sum("is_fraud").alias("nb_fraudes"))
           .orderBy(F.desc("montant_total")))
by_merchant = (clean.groupBy("merchant")
               .agg(F.count("*").alias("nb_tx"),
                    F.round(F.avg("amount"), 2).alias("montant_moyen"),
                    F.sum("is_fraud").alias("nb_fraudes"),
                    F.round(F.avg("is_fraud") * 100, 2).alias("taux_fraude_pct"))
               .orderBy(F.desc("nb_tx")))
by_country = (clean.groupBy("country")
              .agg(F.count("*").alias("nb_tx"),
                   F.sum("is_fraud").alias("nb_fraudes"),
                   F.round(F.avg("is_fraud") * 100, 2).alias("taux_fraude_pct"))
              .orderBy(F.desc("nb_tx")))
by_hour = (clean.groupBy("hour")
           .agg(F.count("*").alias("nb_tx"),
                F.sum("is_fraud").alias("nb_fraudes"),
                F.round(F.avg("is_fraud") * 100, 2).alias("taux_fraude_pct"))
           .orderBy("hour"))

# 5. Stockage des resultats dans HDFS (couche "gold")
gold = args.gold_path.rstrip("/")
for name, d in [("by_user", by_user), ("by_merchant", by_merchant),
                ("by_country", by_country), ("by_hour", by_hour)]:
    d.write.mode("overwrite").parquet(f"{gold}/{name}")
print(f"Tables d'agregation ecrites dans {gold}/")

# 6. Resume JSON pour le rapport
def rows(df, n=None):
    data = df.limit(n).collect() if n else df.collect()
    return [{k: (float(v) if isinstance(v, float) else v) for k, v in r.asDict().items()} for r in data]

summary = {
    "generated_at": datetime.now().isoformat(timespec="seconds"),
    "rows_raw": n_raw,
    "rows_clean": n_clean,
    "global": {
        "nb_tx": nb_tx,
        "nb_fraudes": nb_fraudes,
        "taux_fraude_pct": round(100 * nb_fraudes / nb_tx, 2) if nb_tx else 0,
        "montant_moyen": float(g["montant_moyen"]),
        "montant_max": float(g["montant_max"]),
        "nb_clients": int(g["nb_clients"]),
        "debut": str(g["debut"]),
        "fin": str(g["fin"]),
    },
    "top_clients": rows(by_user, 10),
    "by_merchant": rows(by_merchant),
    "by_country": rows(by_country, 10),
    "by_hour": rows(by_hour),
}
os.makedirs(args.reports_dir, exist_ok=True)
out = os.path.join(args.reports_dir, "batch_summary.json")
with open(out, "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
print(f"Resume ecrit : {out}")
spark.stop()
