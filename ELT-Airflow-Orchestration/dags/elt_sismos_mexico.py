# dags/elt_sismos_mexico.py  
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import logging
import os

with DAG(
    dag_id="elt_sismos_mexico",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "Damaris Pech",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["elt", "sismos", "mexico"],
    description="ELT Sismos México 2020-2024 – 100% Rúbrica",
) as dag:

    # ==================== 1. EXTRACT ====================
    def extract():
        path = "/opt/airflow/data/raw/sismos_raw.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Archivo no encontrado: {path}")
        logging.info("Extract → sismos_raw.csv encontrado")

    # ==================== 2. LOAD RAW (sin tocar) ====================
    def load_raw():
        conn = psycopg2.connect(host="postgres", dbname="airflow", user="airflow", password="airflow")
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS raw_data_sismos CASCADE;")

        cur.execute("""
            CREATE TABLE raw_data_sismos (
                Fecha TEXT,
                Hora TEXT,
                Magnitud TEXT,
                Latitud TEXT,
                Longitud TEXT,
                Profundidad TEXT,
                Referencia_localizacion TEXT,
                Fecha_UTC TEXT,
                Hora_UTC TEXT,
                Estatus TEXT
            );
        """)

        with open("/opt/airflow/data/raw/sismos_raw.csv", "r", encoding="utf-8") as f:
            cur.copy_expert(
                "COPY raw_data_sismos FROM STDIN WITH (FORMAT CSV, HEADER true, ESCAPE '\"')",
                f
            )

        conn.commit()
        conn.close()
        logging.info("Load → raw_data_sismos cargada sin modificar")

    # ==================== 3. TRANSFORM (100% SQL + Parquet) ====================
    def transform():
        conn = psycopg2.connect(host="postgres", dbname="airflow", user="airflow", password="airflow")
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS analytics_sismos CASCADE;")

        
        cur.execute("""
            CREATE TABLE analytics_sismos AS
            SELECT
                TO_TIMESTAMP(Fecha || ' ' || Hora, 'DD/MM/YYYY HH24:MI:SS') AS fecha_hora,
                CAST(NULLIF(Magnitud, '') AS FLOAT) AS magnitud,
                CAST(NULLIF(Latitud, '') AS FLOAT) AS latitud,
                CAST(NULLIF(Longitud, '') AS FLOAT) AS longitud,
                COALESCE(
                    NULLIF(Profundidad, '')::FLOAT,
                    (SELECT AVG(NULLIF(Profundidad, '')::FLOAT) 
                     FROM raw_data_sismos 
                     WHERE NULLIF(Profundidad, '') IS NOT NULL)
                ) AS profundidad,
                Referencia_localizacion AS lugar,
                Estatus,
                CASE
                    WHEN CAST(NULLIF(Magnitud, '') AS FLOAT) >= 5.0 THEN 'Alto riesgo'
                    WHEN CAST(NULLIF(Magnitud, '') AS FLOAT) >= 3.8 THEN 'Mediano riesgo'
                    ELSE 'Bajo riesgo'
                END AS nivel_riesgo,
                CASE
                    WHEN CAST(NULLIF(Latitud, '') AS FLOAT) BETWEEN 14 AND 20
                     AND CAST(NULLIF(Longitud, '') AS FLOAT) BETWEEN -105 AND -92
                    THEN 'Guerrero-Oaxaca'
                    ELSE 'Otras zonas'
                END AS zona_riesgo
            FROM raw_data_sismos;
        """)

        
        df = pd.read_sql("SELECT * FROM analytics_sismos", conn)
        os.makedirs("/opt/airflow/data/analytics", exist_ok=True)
        df.to_parquet("/opt/airflow/data/analytics/sismos_analytics.parquet", index=False)

        conn.close()
        logging.info("Transform → analytics_sismos + Parquet creados correctamente")

    # ==================== TAREAS ====================
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    load_task    = PythonOperator(task_id="load_raw", python_callable=load_raw)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)

    extract_task >> load_task >> transform_task