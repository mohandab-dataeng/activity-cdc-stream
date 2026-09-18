"""
init_database.py
Crée les tables PostgreSQL à partir des modèles SQLAlchemy définis dans models.py.
"""

from sqlalchemy import create_engine
from src.ingestion.models import Base
from src.config import DATABASE_URL


def init_database():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    print("Tables créées avec succès : salaries, referentiel_entreprise")


if __name__ == "__main__":
    init_database()