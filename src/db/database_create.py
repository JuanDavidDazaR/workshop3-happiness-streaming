"""This script creates a PostgreSQL database and table if they do not exist."""

import os
from sqlalchemy import text, create_engine, MetaData, Table, Column, Integer, String, Float
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from .db_conection import connect_db

# Cargar variables de entorno
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

db_name_default = os.getenv("DB_NAME", "happiness_db")  # Valor predeterminado desde .env
db_name_load = os.getenv("DB_NAME_LOAD", "postgres")  # Base para verificar/crear

def create_database(db_name=None):
    """Create a PostgreSQL database and table if they do not exist.
    
    Args:
        db_name (str, optional): Nombre de la base de datos a crear. Si no se proporciona,
                                se usa el valor de DB_NAME desde .env.
    """
    engine = None
    try:
        # Usar el nombre proporcionado o el predeterminado
        db_name = db_name or db_name_default
        
        # Conectar a la base de datos postgres para crear la nueva base
        engine = connect_db(db_name_load)
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")

            # Verificar si la base de datos existe
            result = connection.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name;"),
                {"db_name": db_name},
            ).fetchone()
            if not result:
                connection.execute(text(f'CREATE DATABASE "{db_name}";'))
                print(f"Database '{db_name}' successfully created.")
            else:
                print(f"Database '{db_name}' already exists.")

        # Conectar a la nueva base de datos para crear la tabla
        engine = connect_db(db_name)
        metadata = MetaData()

        # Definir la tabla con las columnas específicas
        happiness_predictions = Table(
            'happiness_predictions',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('index', Integer),
            Column('features', String),
            Column('y_test', Float),
            Column('y_pred', Float)
        )

        # Crear la tabla si no existe
        metadata.create_all(engine)
        print("Table 'happiness_predictions' created or verified.")

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except KeyError as e:
        print(f"Missing environment variable: {e}")
    except TypeError as e:
        print(f"Type error: {e}")
    except OSError as e:
        print(f"OS error: {e}")
    except RuntimeError as e:
        print(f"Unexpected runtime error: {e}")

    finally:
        if engine is not None:
            engine.dispose()

if __name__ == "__main__":
    # Opcional: Pedir al usuario el nombre de la base de datos
    user_db_name = input("Enter the database name (press Enter for default): ")
    create_database(user_db_name if user_db_name else None)