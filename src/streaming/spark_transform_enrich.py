"""
spark_transform_enrich.py
1er job Spark Structured Streaming : lit le topic Redpanda des activités
sportives, filtre le bruit du snapshot, et écrit les données brutes
en Delta Lake (couche bronze) — sans enrichissement.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType
)

REDPANDA_BROKERS = "redpanda:9092"
TOPIC = "activity_cdc.public.activites_sportives"
CHECKPOINT_PATH = "/data/checkpoints/bronze_activites"
BRONZE_PATH = "/data/bronze/activites_enrichies"

schema_activite = StructType([
    StructField("id", IntegerType()),
    StructField("id_salarie", IntegerType()),
    StructField("date_debut", LongType()),
    StructField("type_activite", StringType()),
    StructField("distance", DoubleType()),
    StructField("date_fin", LongType()),
    StructField("commentaire", StringType()),
])

schema_source = StructType([
    StructField("snapshot", StringType()),
])

schema_debezium = StructType([
    StructField("payload", StructType([
        StructField("after", schema_activite),
        StructField("source", schema_source),
        StructField("op", StringType()),
    ])),
])


def main():
    spark = (
        SparkSession.builder
        .appName("SparkTransformEnrich")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", REDPANDA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_brut = (
        df_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema_debezium).alias("data"))
        .select("data.payload.*")
        .filter(col("source.snapshot") != "true")
        .select("after.*")
    )

    requete = (
        df_brut.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(BRONZE_PATH)
    )

    print("Job Spark (bronze) démarré, en écoute sur le topic...")
    requete.awaitTermination()


if __name__ == "__main__":
    main()