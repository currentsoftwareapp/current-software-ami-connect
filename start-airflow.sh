# Utility script to start Airflow on the production server
export PYTHONPATH=$(pwd)
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
nohup airflow api-server &
nohup airflow scheduler &
nohup airflow dag-processor &

