"""
main.py
Orchestration du pipeline batch : initialisation, chargement RH, validation,
génération des données, configuration du CDC.
Les jobs Spark (streaming) et le consumer Slack sont des processus longs,
lancés séparément (voir scripts/ et README.md).
"""

from src.ingestion.init_database import init_database
from src.ingestion.load_rh_data import main as charger_rh
from src.validation.validate_declarations import main as valider_declarations
from src.ingestion.generate_strava_data import main as generer_activites
from src.ingestion.populate_referentiel_sportif import main as peupler_referentiel_sportif
from src.streaming.setup_cdc_connector import main as configurer_cdc
from src.analytics.compute_kpis import main as calculer_kpis


def main():
    print("=" * 60)
    print("PIPELINE ACTIVITY-CDC-STREAM — Initialisation batch")
    print("=" * 60)

    print("\n[1/7] Initialisation de la base de données...")
    init_database()

    print("\n[2/7] Chargement des données RH...")
    charger_rh()

    print("\n[3/7] Peuplement du référentiel sportif...")
    peupler_referentiel_sportif()

    print("\n[4/7] Validation des déclarations de déplacement...")
    valider_declarations()

    print("\n[5/7] Configuration du connecteur CDC (Debezium)...")
    configurer_cdc()

    print("\n[6/7] Génération de l'historique d'activités sportives...")
    generer_activites()

    print("\n[7/7] Calcul des KPI...")
    calculer_kpis()

    print("\n" + "=" * 60)
    print("Pipeline batch terminé.")
    print("Prochaines étapes (processus longs, à lancer séparément) :")
    print("  - ./scripts/run_spark_transform.sh")
    print("  - ./scripts/run_spark_clean.sh")
    print("  - uv run python src/notifications/generate_slack_messages.py")
    print("  - uv run python src/ingestion/simulate_live_activities.py")
    print("=" * 60)


if __name__ == "__main__":
    main()