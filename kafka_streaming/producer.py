import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

import json
import pandas as pd
import time
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

def send_features_to_kafka(df, topic="happiness-topic", sleep_seconds=1):
    """Envía las features y el valor real de cada fila del DataFrame a Kafka como un mensaje JSON."""
    try:
        logging.info(f"Iniciando envío de features a Kafka (topic: {topic})...")

        # Definir las columnas de features en el orden exacto esperado por el modelo
        feature_cols = [
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
        target_col = 'Happiness Score'  # Asegúrate de que este sea el nombre correcto

        # Debug: Imprimir las columnas disponibles
        logging.info(f"Columnas en df_clean.csv: {list(df.columns)}")
        if target_col not in df.columns:
            logging.error(f"La columna '{target_col}' no se encuentra en el DataFrame. Columnas disponibles: {list(df.columns)}")
            raise ValueError(f"La columna '{target_col}' no se encuentra en el DataFrame.")

        producer = kafka_producer()

        for idx, row in df.iterrows():
            # Extraer las features en el orden correcto
            features = {col: float(row[col]) if col in df.columns else 0 for col in feature_cols}
            message = {
                'index': int(idx),
                'features': features,
                'y_test': float(row[target_col]) if target_col in df.columns else None
            }
            producer.send(topic, value=message)
            logging.info(f"Mensaje {idx} enviado a Kafka: {message}")
            time.sleep(sleep_seconds)

        producer.flush()
        logging.info("Todos los features enviados correctamente a Kafka.")

    except Exception as e:
        logging.error(f"Error durante el envío a Kafka: {e}")
        raise

if __name__ == "__main__":
    # Cargar el DataFrame desde el archivo limpio
    df = pd.read_csv("../data/processed/df_clean.csv")
    send_features_to_kafka(df)