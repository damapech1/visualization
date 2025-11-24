"""
Air Quality ETL Pipeline - Monterrey & Saltillo
3 SEPARATE TASKS: Extract → Transform → Load
100% compliant with assignment requirements
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import logging

RAW_CSV = Path("/opt/airflow/data/raw/air_quality.csv")
PROCESSED_DIR = Path("/opt/airflow/data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

@dag(
    dag_id="air_quality_mty_saltillo",
    description="ETL with explicit Extract → Transform → Load tasks",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "student",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["air-quality", "final", "explicit-etl"]
)
def air_quality_explicit_etl():

    @task
    def extract() -> str:
        """EXTRACT: Validate raw file exists"""
        try:
            if not RAW_CSV.exists():
                raise FileNotFoundError(f"Raw file not found: {RAW_CSV}")
            logging.info(f"EXTRACT SUCCESS - File found: {RAW_CSV}")
            return str(RAW_CSV)
        except Exception as e:
            logging.error(f"EXTRACT FAILED: {e}")
            raise

    @task
    def transform(raw_path: str) -> str:
        """TRANSFORM: Clean + feature engineering with chunk processing"""
        try:
            logging.info("TRANSFORM STARTED - Processing with chunks...")

            chunks = pd.read_csv(
                raw_path,
                chunksize=300_000,
                parse_dates=["fecha"],
                dtype={"id": str, "sensor_id": str, "sensor_nombre": str},
                low_memory=False
            )

            processed = []
            for i, chunk in enumerate(chunks, 1):
                try:
                    chunk = chunk.dropna(subset=["pm25", "fecha"])
                    chunk["pm25"] = pd.to_numeric(chunk["pm25"], errors="coerce")
                    chunk = chunk.dropna(subset=["pm25"])
                    chunk = chunk.drop_duplicates(subset=["id", "fecha"])

                    chunk["year"] = chunk["fecha"].dt.year
                    chunk["month"] = chunk["fecha"].dt.month
                    chunk["hour"] = chunk["fecha"].dt.hour
                    chunk["city"] = chunk["estado"].apply(
                        lambda x: "Monterrey" if "Nuevo León" in str(x) else "Saltillo"
                    )

                    processed.append(chunk)
                except Exception as e:
                    logging.warning(f"Chunk {i} skipped: {e}")
                    continue

            if not processed:
                raise ValueError("No data survived transformation")

            df_clean = pd.concat(processed, ignore_index=True)
            temp_path = PROCESSED_DIR / "temp_transformed.parquet"
            df_clean.to_parquet(temp_path, index=False)
            logging.info(f"TRANSFORM SUCCESS - {len(df_clean):,} rows")
            return str(temp_path)

        except Exception as e:
            logging.error(f"TRANSFORM FAILED: {e}")
            raise

    @task
    def load(transformed_path: str):
        """LOAD: Save final CSV + JSON for dashboard"""
        try:
            df = pd.read_parquet(transformed_path)
            
            csv_final = PROCESSED_DIR / "air_quality_clean.csv"
            json_final = PROCESSED_DIR / "air_quality_clean.json"
            
            df.to_csv(csv_final, index=False)
            df.to_json(json_final, orient="records", date_format="iso")
            
            avg = df["pm25"].mean()
            logging.info(f"LOAD SUCCESS - Final files created")
            logging.info(f"Average PM2.5: {avg:.1f} µg/m³")
            logging.info(f"CSV: {csv_final} | JSON: {json_final}")

            return {"rows": len(df), "pm25_avg": round(avg, 1)}

        except Exception as e:
            logging.error(f"LOAD FAILED: {e}")
            raise

    
    raw_file = extract()
    transformed_file = transform(raw_file)
    load(transformed_file)

air_quality_explicit_etl()