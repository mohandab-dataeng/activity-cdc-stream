"""
main.py
Orchestration complète du pipeline : initialisation, chargement RH, validation,
génération des données, configuration du CDC, calcul des KPI, puis démarrage
des processus longs (jobs Spark streaming + consumer Slack) en arrière-plan
pour que le pipeline réagisse en direct à toute nouvelle donnée.
"""

import signal
import subprocess
import sys
import time
from pathlib import Path

from src.ingestion.init_database import init_database
from src.ingestion.load_rh_data import main as charger_rh
from src.validation.validate_declarations import main as valider_declarations
from src.ingestion.generate_strava_data import main as generer_activites
from src.ingestion.populate_referentiel_sportif import main as peupler_referentiel_sportif
from src.streaming.setup_cdc_connector import main as configurer_cdc
from src.analytics.compute_kpis import main as calculer_kpis

RACINE = Path(__file__).parent
LOGS_DIR = RACINE / "logs"

# (nom, commande, fichier de log)
PROCESSUS_LONGS = [
    ("spark-bronze (transform_enrich)", [str(RACINE / "scripts" / "run_spark_transform.sh")], "spark_bronze.log"),
    ("spark-silver (clean_finalize)", [str(RACINE / "scripts" / "run_spark_clean.sh")], "spark_silver.log"),
    ("slack-notifier", [sys.executable, "-m", "src.notifications.generate_slack_messages"], "slack_notifier.log"),
]


def demarrer_processus_longs():
    """Démarre les jobs Spark streaming et le consumer Slack en arrière-plan.

    Ce sont des tâches infinies : on ne peut pas les `subprocess.run()`
    sans bloquer le reste du pipeline, donc on les détache via Popen,
    avec chaque sortie redirigée vers son propre fichier de log.
    """
    LOGS_DIR.mkdir(exist_ok=True)
    processus = []

    for nom, commande, nom_log in PROCESSUS_LONGS:
        fichier_log = open(LOGS_DIR / nom_log, "w")
        proc = subprocess.Popen(
            commande,
            stdout=fichier_log,
            stderr=subprocess.STDOUT,
            cwd=RACINE,
        )
        processus.append((nom, proc, fichier_log))
        print(f"  - {nom} démarré (PID {proc.pid}, log : logs/{nom_log})")

    return processus


def arreter_processus_longs(processus):
    print("\nArrêt des processus longs...")
    for nom, proc, fichier_log in processus:
        proc.terminate()
    for nom, proc, fichier_log in processus:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        fichier_log.close()
        print(f"  - {nom} arrêté")


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
    print("Pipeline batch terminé. Démarrage des processus longs (streaming)...")
    print("=" * 60)

    processus = demarrer_processus_longs()

    print("\nTous les processus sont actifs. Le pipeline réagit désormais en direct.")
    print("Astuce démo : uv run python src/ingestion/simulate_live_activities.py")
    print("Ctrl+C pour arrêter les processus longs et quitter.\n")

    def gerer_arret(signum, frame):
        arreter_processus_longs(processus)
        sys.exit(0)

    signal.signal(signal.SIGINT, gerer_arret)
    signal.signal(signal.SIGTERM, gerer_arret)

    while True:
        for nom, proc, _ in list(processus):
            code = proc.poll()
            if code is not None:
                print(f"\nATTENTION : {nom} s'est arrêté (code {code}). Voir logs/ pour le détail.")
                processus.remove((nom, proc, _))
        time.sleep(5)


if __name__ == "__main__":
    main()