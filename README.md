workshop3-happiness-streaming
==============================
Predicción del Happiness Score
Este proyecto predice el Happiness Score de países utilizando datos del World Happiness Report (2015–2019), logrando una precisión del 87.86% (R²=0.8786) con un modelo XGBoost. A través de un análisis exploratorio de datos (EDA) meticuloso, se identifican factores clave como Region_Latin America and Caribbean, Economy (GDP per Capita) y Health (Life Expectancy) como los más influyentes. El pipeline incluye limpieza y unificación de datos, entrenamiento de modelos de machine learning y un flujo de streaming con Apache Kafka que procesa datos en tiempo real, genera predicciones y almacena resultados en PostgreSQL. El objetivo es desarrollar un modelo robusto, simplificado y generalizable para aplicaciones predictivas escalables que analicen la felicidad global.
Estructura del Proyecto
├── LICENSE
├── Makefile           # Comandos para ejecutar tareas como `make data` o `make train`
├── README.md          # Documentación principal del proyecto
├── data
│   ├── processed      # Datos finales para modelado (e.g., df_clean.csv)
│   └── raw            # Datos originales inmutables (e.g., WH_2015.csv, WH_2016.csv, ...)
├── docs               # Documentación generada con Sphinx
├── kafka_streaming    # Archivos para el pipeline de streaming con Apache Kafka
│   ├── consumer.py    # Script para consumir datos del topic de Kafka
│   ├── producer.py    # Script para enviar datos al topic de Kafka
│   └── docker-compose.yml  # Configuración de servicios para Kafka
├── models             # Modelos entrenados (e.g., happiness_model_XGB.pkl)
├── notebooks          # Cuadernos Jupyter para EDA y modelado
│   ├── 001_EDA.ipynb  # Análisis exploratorio de datos
│   └── 002_ML.ipynb   # Entrenamiento y evaluación de modelos
├── requirements.txt   # Dependencias del entorno (generado con `pip freeze`)
├── src                # Código fuente del proyecto
│   ├── __init__.py    # Convierte src en un módulo de Python
│   └── db
│       ├── __init__.py
│       ├── database_create.py  # Script para crear la base de datos PostgreSQL
│       └── db_connection.py    # Script para conectar con PostgreSQL
└── tox.ini            # Configuraciones para pruebas con tox

Metodología
El proyecto se desarrolla en tres fases principales:

Análisis Exploratorio de Datos (EDA)Se cargan y limpian los datasets de 2015–2019 (782 registros), unificando columnas comunes (Happiness Score, Economy, Health, Family, Freedom, Trust, Region) y resolviendo inconsistencias, como la ausencia de Region en 2018–2019, mediante mapeo manual. Se excluyen variables redundantes (Generosity, Happiness Rank, Country) y se eliminan Year_ y Family por su baja contribución (1.97% para Family), resultando en 15 características predictivas (5 numéricas, 10 de Region_). Visualizaciones como histogramas, boxplots y mapas de calor revelan correlaciones fuertes (0.7–0.8) entre Economy, Health y Happiness Score.

Modelado PredictivoSe entrenan siete modelos de regresión, seleccionando XGBoost por su precisión (R²=0.8786, MSE=0.1514). Los factores más influyentes son Region_Latin America and Caribbean (33.31%) y Economy (~21.71%). El modelo se guarda como happiness_model_XGB.pkl en la carpeta models.

Streaming en Tiempo RealUn pipeline con Apache Kafka envía datos de df_clean.csv al topic happiness-topic, los procesa en tiempo real con el modelo XGBoost y almacena las predicciones en PostgreSQL, calculando métricas como MSE para monitoreo. Este enfoque asegura un sistema robusto y escalable.


Casos de Uso
Este proyecto puede aplicarse en diversos escenarios para analizar y predecir la felicidad global:

Políticas Públicas: Gobiernos y ONGs pueden usar el modelo para identificar factores que mejoren el bienestar de la población, priorizando inversiones en salud, economía o libertad según las predicciones.
Investigación Académica: Investigadores pueden aprovechar el pipeline para estudiar correlaciones entre variables socioeconómicas y felicidad, extendiendo el análisis a nuevos años o regiones.
Aplicaciones en Tiempo Real: Empresas de tecnología pueden integrar el pipeline de streaming para monitorear tendencias de bienestar en tiempo real, útil para plataformas de análisis social.
Educación: Instituciones educativas pueden usar los notebooks para enseñar técnicas de EDA, modelado de machine learning y procesamiento de datos en streaming.

Requisitos

Sistema operativo: Linux, macOS o Windows con WSL2.
Herramientas:
Python 3.8+
Docker y Docker Compose (para Kafka)
PostgreSQL


Dependencias: Listadas en requirements.txt.

Instrucciones para Ejecutar el Proyecto
Sigue estos pasos para configurar y ejecutar el proyecto:

Clonar el repositorio  
git clone https://github.com/JuanDavidDazaR/workshop3-happiness-streaming.git
cd workshop3-happiness-streaming


Instalar dependencias  
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
pip install -r requirements.txt


Configurar credenciales de PostgreSQLCrea un archivo .env en la raíz del proyecto con las siguientes variables:
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_base_datos
DB_USER=tu_usuario_postgres
DB_PASSWORD=tu_contraseña


Nota: Escoge un nombre para DB_NAME que no exista en tu PostgreSQL. El proyecto creará la base de datos automáticamente usando database_create.py.


Ejecutar los notebooksCorre los siguientes notebooks en orden para realizar el análisis y entrenamiento:

notebooks/001_EDA.ipynb: Análisis exploratorio de datos.
notebooks/002_ML.ipynb: Entrenamiento y evaluación del modelo.
Nota: Asegúrate de que df_clean.csv se genere en data/processed/ y happiness_model_XGB.pkl en models/ tras ejecutar los notebooks.




Iniciar los servicios de Kafka  
cd kafka_streaming
docker-compose up -d

Verifica que los servicios estén corriendo:
docker ps


Configurar el topic de KafkaCrea el topic happiness-topic:
kafka-topics --bootstrap-server kafka-test:9092 --create --topic happiness-topic


Ejecutar el pipeline de streaming  

En una terminal, inicia el consumidor:python consumer.py


En otra terminal, ejecuta el productor:python producer.py




Detener los servicios  
docker-compose down



Notas

Asegúrate de que df_clean.csv esté en data/processed/ y happiness_model_XGB.pkl en models/ antes de ejecutar el pipeline de streaming.
Los scripts database_create.py y db_connection.py en src/db gestionan la creación y conexión a la base de datos PostgreSQL.
Si encuentras errores de conexión con Kafka, verifica que kafka-test:9092 sea accesible o ajusta docker-compose.yml.

Agradecimientos
Gracias por explorar este repositorio. ¡Espero que este proyecto inspire aplicaciones innovadoras para analizar la felicidad global!
Autor: Juan David DazaContacto: [Inserta tu correo o enlace a tu perfil si lo deseas]


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
