"""
run_silver_quality_checks.py
Applique des règles de qualité (Great Expectations) sur la couche silver,
après enrichissement — détecte les jointures ratées (référentiel entreprise
ou sportif introuvable pour une ligne).
"""

import glob
import sys

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

SILVER_PATH = "data/silver/activites_finales"


def charger_donnees_silver():
    """Lit tous les fichiers Parquet de la couche silver avec pandas."""
    fichiers_parquet = glob.glob(f"{SILVER_PATH}/*.parquet")
    df = pd.concat([pd.read_parquet(f) for f in fichiers_parquet], ignore_index=True)
    return df


def construire_expectations():
    """Définit les règles de cohérence attendues après enrichissement."""
    return [
        # Jointure avec salaries : un id_salarie doit toujours avoir un nom/prénom
        gxe.ExpectColumnValuesToNotBeNull(column="nom"),
        gxe.ExpectColumnValuesToNotBeNull(column="prenom"),
        # Jointure avec referentiel_entreprise : id_bu doit toujours résoudre un nom_bu
        gxe.ExpectColumnValuesToNotBeNull(column="nom_bu"),
        # Jointure avec referentiel_sportif : type_activite doit toujours résoudre une catégorie
        gxe.ExpectColumnValuesToNotBeNull(column="categorie"),
        gxe.ExpectColumnValuesToNotBeNull(column="eligible_jours_bien_etre"),
    ]


def main():
    df = charger_donnees_silver()
    print(f"{len(df)} lignes chargées depuis la couche silver")

    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="silver_source")
    data_asset = data_source.add_dataframe_asset(name="activites_enrichies_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("activites_enrichies_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    print("\nRésultats de la validation (cohérence des jointures) :")
    toutes_reussies = True
    for expectation in construire_expectations():
        resultat = batch.validate(expectation)
        statut = "✓" if resultat.success else "✗ ÉCHEC"
        print(f"  {statut} {expectation.__class__.__name__} (colonne: {expectation.column})")
        if not resultat.success:
            toutes_reussies = False

    print(f"\nValidation globale : {'RÉUSSIE' if toutes_reussies else 'ÉCHEC'}")

    if not toutes_reussies:
        sys.exit(1)


if __name__ == "__main__":
    main()
