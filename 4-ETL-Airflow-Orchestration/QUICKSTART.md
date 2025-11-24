# Quick Start - Airflow ETL Lab

Get your Airflow environment running in 5 minutes!

##  One-Time Setup

```bash
# 1. Navigate to lab directory
cd visualization/4-ETL-Airflow-Orchestration

# 2. Create a virtual enviroment and select your python version 
py -3.11 -m venv venv

# 3. Activate the enviroment
source venv/Scripts/activate

# 4 Install the dependences
pip install -r requirements.txt

# 5. Build Docker images (includes pandas and requests)
docker-compose build

# 6. Initialize Airflow database
docker-compose run --rm webserver airflow db init

# 7. Create admin user (username: airflow, password: airflow)
docker-compose run --rm webserver airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow

# 8. Start Airflow
docker-compose up -d
```

##  Access Airflow Web UI

Open your browser: **http://localhost:8080**

### Login Credentials
```
Username: airflow
Password: airflow
```

**First Time Login:**
1. You'll see the Airflow dashboard
2. Two DAGs will be visible: `hello_world` and `api_etl_pipeline`
3. Both will be paused (toggle switch is OFF)
4. You need to enable them to run

##  Run Your First DAG

1. In Airflow UI, find `hello_world` DAG
2. Toggle it **ON** (click the switch)
3. Click **Play button** → **Trigger DAG**
4. Watch it run in **Graph** view!

##  Run the ETL Pipeline

1. Find `api_etl_pipeline` DAG
2. Toggle **ON**
3. Click **Trigger DAG**
4. Wait for completion (~30 seconds)
5. Check data files:
   ```bash
   docker-compose exec webserver ls -la /opt/airflow/data
   ```

##  Daily Commands

```bash
# Start Airflow
docker-compose up -d

# Stop Airflow
docker-compose down

# View logs
docker-compose logs -f

# Check if running
docker-compose ps
```

##  Reset Everything

If you need a fresh start:

```bash
docker-compose down
docker volume rm 4-etl-airflow-orchestration_postgres_data
# Then run setup steps again
```


## ❓ Troubleshooting

**DAG not showing?**
```bash
docker-compose exec webserver airflow dags list-import-errors
```

**Port 8080 in use?**
Edit `docker-compose.yml`, change `"8080:8080"` to `"8081:8080"`

**Permission errors?**
```bash
chmod -R 755 dags logs
```


