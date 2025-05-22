import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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
            'Freedom',
            'Trust (Government Corruption)',
            'Family',
            'Economy (GDP per Capita)',
            'Health (Life Expectancy)',
            'Year_2015',
            'Year_2016',
            'Year_2017',
            'Year_2018',
            'Year_2019',
            'Region_Western Europe',
            'Region_North America',
            'Region_Australia and New Zealand',
            'Region_Middle East and Northern Africa',
            'Region_Latin America and Caribbean',
            'Region_Southeastern Asia',
            'Region_Central and Eastern Europe',
            'Region_Eastern Asia',
            'Region_Southern Asia',
            'Region_Sub-Saharan Africa'
        ]
        target_col = 'Happiness Score'  # Ajusta según el nombre exacto en df_clean.csv

        # Debug: Imprimir las columnas disponibles
        logging.info(f"Columnas en df_clean.csv: {list(df.columns)}")
        if target_col not in df.columns:
            logging.error(f"La columna '{target_col}' no se encuentra en el DataFrame. Columnas disponibles: {list(df.columns)}")
            raise ValueError(f"La columna '{target_col}' no se encuentra en el DataFrame.")

        # Filtrar feature_cols a solo las que existen en el DataFrame
        available_cols = [col for col in feature_cols if col in df.columns]
        logging.info(f"Columnas utilizadas: {available_cols}")

        producer = kafka_producer()

        for idx, row in df.iterrows():
            # Extraer las features disponibles en el orden correcto
            features = {col: float(row[col]) if pd.notna(row[col]) else 0 for col in available_cols}
            message = {
                'index': int(idx),
                'features': features,
                'y_test': float(row[target_col]) if pd.notna(row[target_col]) else None
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
    # Cargar el DataFrame desde el archivo limpio con manejo de errores
    try:
        df = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/processed/df_clean.csv"))
        send_features_to_kafka(df)
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
    except Exception as e:
        logging.error(f"Error al cargar el DataFrame: {e}")