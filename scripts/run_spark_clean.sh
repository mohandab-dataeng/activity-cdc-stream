#!/bin/bash
# run_spark_clean.sh
# Lance le job Spark de nettoyage/enrichissement final vers la couche silver.

docker exec spark /opt/spark/bin/spark-submit \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/spark-apps/src/streaming/spark_clean_finalize.py