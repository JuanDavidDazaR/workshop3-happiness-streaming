import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaConsumer
from sqlalchemy import Table, MetaData
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
        'auto_offset_reset': 'earliest',
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 10000,
        'max_poll_interval_ms': 600000
    }

    consumer = KafkaConsumer('happiness-topic', **consumer_config)
    return consumer

def save_to_postgresql(message_value):
    """Guarda las predicciones y features en PostgreSQL."""
    try:
        engine = connect_db()
        metadata = MetaData()
        table = Table('happiness_predictions', metadata, autoload_with=engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        session.execute(table.insert(), [message_value])
        session.commit()
        logging.info(f"✅ Predicción guardada en PostgreSQL: {message_value}")

        session.close()

    except Exception as e:
        logging.error(f"Error al guardar en PostgreSQL: {e}")
        # No levantes la excepción para que el consumidor continúe

def calculate_performance_metric(y_true, y_pred):
    """Calcula el MSE entre los valores reales y predichos."""
    from sklearn.metrics import mean_squared_error
    mse = mean_squared_error(y_true, y_pred)
    logging.info(f"Mean Squared Error (MSE): {mse}")
    return mse

def consume_kafka_messages():
    """Consume mensajes de Kafka, predice y guarda en PostgreSQL."""
    # Cargar el modelo preentrenado
    try:
        model = joblib.load("../models/happiness_model_XGB.pkl")
        # Verificar las features esperadas por el modelo
        expected_feature_order = model.get_booster().feature_names
        logging.info(f"Features esperadas por el modelo: {expected_feature_order}")
    except Exception as e:
        logging.error(f"Error al cargar el modelo: {e}")
        return

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

                features = message_value['features']
                y_test = message_value.get('y_test')

                if y_test is None:
                    logging.warning("No se encontró y_test en el mensaje")
                    continue

                # Crear un DataFrame con las features y reordenarlas según el modelo
                features_df = pd.DataFrame([features])
                features_df = features_df[expected_feature_order]

                # Predecir Happiness Score
                y_pred_value = model.predict(features_df)[0]

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

            # Reducir el tiempo de espera para evitar problemas de heartbeat
            time.sleep(0.1)

    except KeyboardInterrupt:
        logging.info("Consumiendo mensajes interrumpido por el usuario.")
        if y_true_list and predictions:
            calculate_performance_metric(y_true_list, [pred['y_pred'] for pred in predictions])
    except Exception as e:
        logging.error(f"Error en el consumidor: {e}")
    finally:
        consumer.close()
        logging.info("Consumidor de Kafka cerrado.")

if __name__ == "__main__":
    consume_kafka_messages()