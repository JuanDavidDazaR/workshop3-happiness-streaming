import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Ajustar la ruta para apuntar al directorio raíz del proyecto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)


def connect_db(db_name=None):
    """Connection to the database."""
    database = db_name if db_name else os.getenv("DB_NAME")

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")

    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    if not all([user, password, host, port, database]):
        raise ValueError(
            "Missing database configuration values. Check your .env file."
        )

    return create_engine(connection_url)
