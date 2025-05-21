import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaConsumer
from sqlalchemy import Table, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
import json
import logging
import time
import pandas as pd
import joblib
from src.db.db_conection import connect_db
from src.db.database_create import create_database

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def kafka_consumer():
    """Configura el consumidor de Kafka para leer mensajes del topic happiness-topic."""
    consumer_config = {
        'bootstrap_servers': 'localhost:9092',
        'group_id': 'happiness_group',
        'auto_offset_reset': 'earliest'
    }

    consumer = KafkaConsumer('happiness-topic', **consumer_config)
    return consumer

def save_to_postgresql(message_value):
    """Guarda las predicciones y features en PostgreSQL."""
    try:
        engine = connect_db()
        # Crear MetaData sin el argumento bind
        metadata = MetaData()
        # Cargar la tabla con autoload_with, pasando el engine
        table = Table('happiness_predictions', metadata, autoload_with=engine)

        # Crear una sesión
        Session = sessionmaker(bind=engine)
        session = Session()

        # Insertar los datos
        session.execute(table.insert(), [message_value])
        session.commit()
        logging.info(f"✅ Predicción guardada en PostgreSQL: {message_value}")

        session.close()

    except Exception as e:
        logging.error(f"Error al guardar en PostgreSQL: {e}")
        raise  # Levanta la excepción para depurar si es necesario

def calculate_performance_metric(y_true, y_pred):
    """Calcula el MSE entre los valores reales y predichos."""
    from sklearn.metrics import mean_squared_error
    mse = mean_squared_error(y_true, y_pred)
    logging.info(f"Mean Squared Error (MSE): {mse}")
    return mse

def consume_kafka_messages():
    """Consume mensajes de Kafka, predice y guarda en PostgreSQL."""
    # Cargar el modelo preentrenado
    model = joblib.load("../models/happiness_model_XGB.pkl")

    # Definir el orden esperado de las features
    expected_feature_order = [
        'Health (Life Expectancy)',
        'Freedom',
        'Trust (Government Corruption)',
        'Family',
        'Economy (GDP per Capita)',
        'Generosity',
        'Year_2015',
        'Year_2016',
        'Year_2017',
        'Year_2018',
        'Year_2019'
    ]

    create_database()
    logging.info(f"Base de datos creada o verificada.")

    consumer = kafka_consumer()
    predictions = []
    y_true_list = []

    try:
        for msg in consumer:
            try:
                message_value = json.loads(msg.value.decode('utf-8'))
                logging.info(f"Mensaje recibido: {message_value}")

                # Extraer features y valor real del mensaje
                features = message_value['features']
                y_test = message_value.get('y_test')

                if y_test is None:
                    logging.warning("No se encontró y_test en el mensaje")
                    continue

                # Crear un DataFrame con las features y reordenarlas
                features_df = pd.DataFrame([features])
                features_df = features_df[expected_feature_order]  # Reordenar columnas

                # Predecir Happiness Score
                y_pred_value = model.predict(features_df)[0]

                # Preparar datos para guardar
                data_to_save = {
                    'index': message_value['index'],
                    'features': json.dumps(features),
                    'y_test': float(y_test),
                    'y_pred': float(y_pred_value)
                }

                save_to_postgresql(data_to_save)
                predictions.append(data_to_save)
                y_true_list.append(float(y_test))

            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logging.error(f"Error decodificando mensaje: {e}")
            except Exception as e:
                logging.error(f"Error procesando mensaje: {e}")

            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Consumiendo mensajes interrumpido por el usuario.")
        if y_true_list and predictions:  # Solo calcular si hay datos
            calculate_performance_metric(y_true_list, [pred['y_pred'] for pred in predictions])
    except Exception as e:
        logging.error(f"Error en el consumidor: {e}")
    finally:
        consumer.close()
        logging.info("Consumidor de Kafka cerrado.")

if __name__ == "__main__":
    consume_kafka_messages()