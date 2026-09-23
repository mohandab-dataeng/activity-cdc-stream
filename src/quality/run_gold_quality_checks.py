"""
run_gold_quality_checks.py
Applique des règles de qualité (Great Expectations) sur les KPI finaux
(table kpis_salaries, PostgreSQL) — dernière frontière avant Metabase.
"""

import sys

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from sqlalchemy import create_engine

from src.config import DATABASE_URL


def charger_kpis():
    """Lit la table kpis_salaries directement depuis PostgreSQL."""
    engine = create_engine(DATABASE_URL)
    return pd.read_sql_table("kpis_salaries", engine)


def construire_expectations():
    """Définit les règles de cohérence attendues sur les KPI finaux."""
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="id_salarie"),
        gxe.ExpectColumnValuesToBeBetween(column="montant_prime", min_value=0, max_value=None),
        gxe.ExpectColumnValuesToBeInSet(column="jours_bien_etre_accordes", value_set=[0, 5]),
        gxe.ExpectColumnValuesToBeBetween(column="nb_activites_eligibles", min_value=0, max_value=None),
    ]


def main():
    df = charger_kpis()
    print(f"{len(df)} salarié(s) chargé(s) depuis kpis_salaries")

    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="gold_source")
    data_asset = data_source.add_dataframe_asset(name="kpis_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("kpis_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    print("\nRésultats de la validation (KPI finaux) :")
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
