#!/bin/bash
# run_spark_transform.sh
# Lance le job Spark d'enrichissement des activités sportives.

docker exec spark /opt/spark/bin/spark-submit \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.4 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/spark-apps/src/streaming/spark_transform_enrich.py