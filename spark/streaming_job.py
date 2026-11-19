"""Spark Structured Streaming : Kafka -> preparation -> HDFS (Parquet) + agregations en console.

Sorties (2 destinations, comme demande) :
  1. HDFS      : transactions preparees (Parquet partitionne par date)
  2. Console   : agregations en temps reel par client et par marchand (docker logs spark-streaming)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, BooleanType)

HDFS = os.getenv("HDFS_URL", "hdfs://namenode:9000")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

spark = SparkSession.builder.appName("fraud-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("country", StringType()),
    StructField("home_country", StringType()),
    StructField("hour", IntegerType()),
    StructField("device_new", BooleanType()),
    StructField("timestamp", StringType()),
    StructField("is_fraud", IntegerType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", "transactions")
       .option("startingOffsets", "earliest")
       .load())

# Preparation : parsing JSON, nettoyage, creation des variables
df = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("t")).select("t.*")
      .withColumn("event_ts", F.to_timestamp("timestamp"))
      .withColumn("is_foreign", (F.col("country") != F.col("home_country")).cast("int"))
      .withColumn("is_night", F.when((F.col("hour") >= 23) | (F.col("hour") <= 5), 1).otherwise(0))
      .withColumn("log_amount", F.log1p("amount"))
      .withColumn("device_new", F.col("device_new").cast("int"))
      .dropna(subset=["transaction_id", "amount"])
      .withColumn("date", F.to_date("event_ts")))

# Sortie 1 : stockage HDFS (Parquet)
q_hdfs = (df.writeStream.format("parquet")
          .option("path", f"{HDFS}/lake/transactions/")
          .option("checkpointLocation", f"{HDFS}/lake/checkpoints/transactions/")
          .partitionBy("date")
          .trigger(processingTime="30 seconds")
          .outputMode("append")
          .start())

# Sortie 2a : agregation par CLIENT, triee (top 10 par montant total) -> console
by_user = (df.groupBy("user_id")
           .agg(F.count("*").alias("nb_tx"),
                F.round(F.sum("amount"), 2).alias("montant_total"),
                F.sum("is_fraud").alias("nb_fraudes"))
           .orderBy(F.desc("montant_total")).limit(10))
q_user = (by_user.writeStream.format("console")
          .outputMode("complete").option("truncate", "false")
          .queryName("top_clients")
          .trigger(processingTime="30 seconds").start())

# Sortie 2b : agregation par MARCHAND -> console
by_merchant = (df.groupBy("merchant")
               .agg(F.count("*").alias("nb_tx"),
                    F.round(F.avg("amount"), 2).alias("montant_moyen"),
                    F.sum("is_fraud").alias("nb_fraudes"))
               .orderBy(F.desc("nb_tx")))
q_merchant = (by_merchant.writeStream.format("console")
              .outputMode("complete").option("truncate", "false")
              .queryName("stats_marchands")
              .trigger(processingTime="30 seconds").start())

spark.streams.awaitAnyTermination()
