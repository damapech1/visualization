# Laboratory 4: Apache Airflow - ETL Pipeline Orchestration

## Learning Objectives

1. **Understand Apache Airflow** - The #1 workflow orchestration tool used by companies like Airbnb, Twitter, and Adobe
2. **Master TaskFlow API** - Modern `@task` decorator syntax (Airflow 2.x standard)
3. **Build Real ETL Pipelines** - Extract from APIs → Transform with pandas → Load to files
4. **Manage Dependencies** - Control task execution order and parallel processing
5. **Debug Like a Pro** - Use Airflow UI to monitor, troubleshoot, and fix issues


##  What is Apache Airflow?

**Apache Airflow** is an open-source platform for orchestrating complex workflows and data pipelines. It allows you to:
- Define workflows as Python code (DAGs - Directed Acyclic Graphs)
- Schedule and monitor pipelines
- Retry failed tasks automatically
- Track execution history
- Visualize pipeline dependencies

### Real-World Use Cases
- **ETL/ELT Pipelines**: Daily data warehouse updates
- **Machine Learning**: Model training and deployment
- **Data Quality**: Automated data validation
- **Report Generation**: Scheduled business reports
- **API Integrations**: Sync data between systems

##  Quick Start - ONE COMMAND!

### Start Airflow (Automatic Setup)

```bash
# Navigate to lab directory
cd visualization/4-ETL-Airflow-Orchestration

# Start everything! (First time: builds, initializes DB, creates user, starts services)
docker-compose up -d
```

**That's it!** ✨ The first time you run this:
1. Docker builds the custom image with pandas & requests
2. PostgreSQL starts and becomes healthy
3. Airflow database is automatically initialized
4. Admin user is automatically created
5. Webserver and Scheduler start

**Wait 1-2 minutes** for initialization to complete.

### Check Status

```bash
# View real-time logs
docker-compose logs -f

# Or check service status
docker-compose ps
# You should see: postgres (up), airflow-init (exited 0), webserver (up), scheduler (up)
```

### Access Airflow Web Interface

Open your browser: **http://localhost:8080**

**Login Credentials:**
- **Username**: `airflow`
- **Password**: `airflow`

> 💡 **Tip**: Bookmark this page! You'll use it throughout the lab.

## 📁 Project Structure

```
4-ETL-Airflow-Orchestration/
├── docker-compose.yml                  # Orchestrates all services (auto-init included!)
├── Dockerfile                          # Custom Airflow image with pandas 2.0.3 & requests
├── requirements.txt                    # Python dependencies
├── init-airflow.sh                     # Manual init script (optional, for troubleshooting)
├── dags/                               # Your DAG files go here
│   ├── air_quality_mty_saltillo.py     # Dag was created using a database from Monterrey data and Saltillo data.
├── logs/                               # Airflow execution logs (auto-created)
├── data/                               # Your processed data files (auto-created)
│   ├── raw/                            #Raw air_quality.csv
│   ├── processed/                      # Transformed data (CSV + JSON)
├── README.md                           # This file - Main guide
├── QUICKSTART.md                       # Fast setup reference
├── CREDENTIALS.md                      # Password and user from Airflow.
└── JUSTIFICATION.md                    # Why I choose the dataset 
```

**What each directory does:**
- `dags/` - Python files defining your workflows (DAGs)
- `logs/` - Task execution logs for debugging
- `data/` - CSV/JSON files created by your ETL pipelines

## 🎓 Understanding the DAGs

### 1. ETL Pipeline (`air_quality_mty_saltillo.py`)

A complete ETL pipeline demonstrating:

#### Extract Phase
```python
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
```

#### Transform Phase
```python
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
```

#### Load Phase
```python
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
```

### View Logs

In Airflow UI:
1. Click on a task (colored box in Graph view)
2. Click **Log** button
3. Scroll through execution logs

### Check Task Status

- **Green**: Success ✅
- **Red**: Failed ❌
- **Yellow**: Running 🔄
- **Light Green**: Queued ⏳
- **Pink**: Upstream failed
- **Gray**: Not yet run

### Common Issues

**DAG not appearing?**
```bash
# Check for import errors
docker-compose exec webserver airflow dags list-import-errors
```

**Task failed?**
- Check logs in Airflow UI
- Verify API connectivity
- Check data directory permissions

**Need to reset?**
```bash
docker-compose down
docker volume rm 4-etl-airflow-orchestration_postgres_data
# Then run setup steps again
```

##  TaskFlow API vs Traditional Operators

### Traditional Way (Old)
```python
def my_function():
    return "data"

task = PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    dag=dag
)
```

### TaskFlow API (Modern) ✨
```python
@task
def my_function():
    return "data"

result = my_function()  
```

**Benefits of TaskFlow API:**
- ✅ Cleaner syntax
- ✅ Automatic XCom handling
- ✅ Type hints support
- ✅ Better code reusability
- ✅ Less boilerplate

##  Useful Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Access container shell
docker-compose exec webserver bash

# List DAGs
docker-compose exec webserver airflow dags list

# Test a specific task
docker-compose exec webserver airflow tasks test <dag_id> <task_id> <date>

# Rebuild after Dockerfile changes
docker-compose build
docker-compose up -d
```

##  Best Practices

1. **Use TaskFlow API** - Modern and cleaner syntax
2. **Add docstrings** - Document what each task does
3. **Use meaningful names** - `extract_quality` not `task1`
4. **Add logging** - `print()` statements help debugging
5. **Handle errors** - Use try/except blocks
6. **Test incrementally** - Test each task before connecting
7. **Keep tasks focused** - One task, one purpose
8. **Use type hints** - `def transform(data: dict) -> dict:`

##  Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [TaskFlow API Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Common Patterns](https://airflow.apache.org/docs/apache-airflow/stable/howto/index.html)

##  Getting Help

1. Check task logs in Airflow UI
2. Review error messages carefully
3. Test tasks independently
4. Consult documentation
5. Ask instructor during office hours

---
