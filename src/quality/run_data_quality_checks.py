"""
run_data_quality_checks.py
Applique des règles de qualité (Great Expectations) sur les données
de la couche bronze avant leur passage en couche silver.
"""

import glob
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
import sys

BRONZE_PATH = "data/bronze/activites_enrichies"


def charger_donnees_bronze():
    """Lit tous les fichiers Parquet de la couche bronze avec pandas."""
    fichiers_parquet = glob.glob(f"{BRONZE_PATH}/*.parquet")
    df = pd.concat([pd.read_parquet(f) for f in fichiers_parquet], ignore_index=True)
    return df


def construire_expectations():
    """Définit la liste des règles de qualité à appliquer."""
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="id_salarie"),
        gxe.ExpectColumnValuesToNotBeNull(column="date_debut"),
        gxe.ExpectColumnValuesToNotBeNull(column="date_fin"),
        gxe.ExpectColumnValuesToNotBeNull(column="type_activite"),
        gxe.ExpectColumnValuesToBeBetween(column="distance", min_value=0, max_value=None),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="date_fin", column_B="date_debut"
        ),
    ]


def main():
    df = charger_donnees_bronze()
    print(f"{len(df)} lignes chargées depuis la couche bronze")

    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="bronze_source")
    data_asset = data_source.add_dataframe_asset(name="activites_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("activites_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    print("\nRésultats de la validation :")
    toutes_reussies = True
    for expectation in construire_expectations():
        resultat = batch.validate(expectation)
        statut = "✓" if resultat.success else "✗ ÉCHEC"
        print(f"  {statut} {expectation.__class__.__name__}")
        if not resultat.success:
            toutes_reussies = False

    print(f"\nValidation globale : {'RÉUSSIE' if toutes_reussies else 'ÉCHEC'}")

    if not toutes_reussies:
        sys.exit(1)


if __name__ == "__main__":
    main()