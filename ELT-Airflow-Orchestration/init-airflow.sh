#!/bin/bash
# Airflow Initialization Script
# This script initializes the Airflow database and creates the admin user

set -e

echo "🚀 Initializing Apache Airflow..."
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Build the images first
echo "📦 Building Docker images..."
docker-compose build

# Start only postgres
echo "🐘 Starting PostgreSQL..."
docker-compose up -d postgres

# Wait for postgres to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 5

# Initialize the database
echo "🗄️  Initializing Airflow database..."
docker-compose run --rm webserver airflow db init

# Create admin user
echo "👤 Creating admin user..."
docker-compose run --rm webserver airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow 2>/dev/null || echo "   User might already exist, continuing..."

echo ""
echo "✅ Initialization complete!"
echo ""
echo "📝 Next steps:"
echo "   1. Start Airflow: docker-compose up -d"
echo "   2. Access UI: http://localhost:8080"
echo "   3. Login with: airflow / airflow"
echo ""

