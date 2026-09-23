"""
spark_clean_finalize.py
2e job Spark Structured Streaming : lit la couche bronze, enrichit avec
le référentiel entreprise (via salaries) et le référentiel sportif,
puis écrit le résultat en Delta Lake (couche silver).
"""

import os

from pyspark.sql import SparkSession

BRONZE_PATH = "/data/bronze/activites_enrichies"
SILVER_PATH = "/data/silver/activites_finales"
CHECKPOINT_PATH = "/data/checkpoints/silver_activites"

POSTGRES_DB = os.getenv("POSTGRES_DB", "sportdata")
POSTGRES_JDBC_URL = f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}"
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def lire_referentiel(spark, table, colonnes=None):
    """Lecture JDBC statique d'une table de référence PostgreSQL."""
    df = (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return df.select(*colonnes) if colonnes else df


def main():
    spark = (
        SparkSession.builder
        .appName("SparkCleanFinalize")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # Référentiel entreprise : salariés (BU, nom, prénom) + noms des BU
    df_salaries = lire_referentiel(
        spark, "salaries", ["id_salarie", "id_bu", "prenom", "nom"]
    )
    df_referentiel_entreprise = lire_referentiel(
        spark, "referentiel_entreprise", ["id_bu", "nom_bu"]
    )

    # Référentiel sportif : catégorie par type d'activité
    df_referentiel_sportif = lire_referentiel(
        spark, "referentiel_sportif", ["type_activite", "categorie", "eligible_jours_bien_etre"]
    )

    # Lecture de la couche bronze, en streaming
    df_bronze = (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )

    # Enrichissement : référentiel entreprise (via salariés) + référentiel sportif
    df_enrichi = (
        df_bronze
        .join(df_salaries, on="id_salarie", how="left")
        .join(df_referentiel_entreprise, on="id_bu", how="left")
        .join(df_referentiel_sportif, on="type_activite", how="left")
    )

    requete = (
        df_enrichi.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(SILVER_PATH)
    )

    print("Job Spark (silver) démarré, en écoute sur la couche bronze...")
    requete.awaitTermination()


if __name__ == "__main__":
    main()