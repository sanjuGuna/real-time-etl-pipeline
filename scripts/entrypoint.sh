#!/bin/bash
set -e

echo "Starting Airflow entrypoint..."

if [ -f "/opt/airflow/requirements.txt" ]; then
  echo "Installing requirements..."
  pip install -r /opt/airflow/requirements.txt
fi

echo "Initializing Airflow DB..."
airflow db init

echo "Creating admin user..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

echo "Airflow initialization complete."

# Always execute airflow subcommand safely
exec airflow "$@"
