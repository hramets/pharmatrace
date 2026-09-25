from os import listdir
from airflow.sdk import DAG, task
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'project'))
from paths import BRONZE_ACTIVE_BATCH_DATA
import subprocess

@task.bash
def ingest_openfda_data():
    return "python /opt/airflow/project/ingestion/openfda.py"

@task
def process_all_batches():
    """Process all batches in the bronze active directory.
    Implemented to integrate the batching loop into the DAG."""
    while not (listdir(BRONZE_ACTIVE_BATCH_DATA) == ['last_processed.json']):
        # move batch
        subprocess.run(['python', '/opt/airflow/project/scripts/move_bronze_active_batch.py'], check=True)
        # check if active batch has files
        if not listdir(BRONZE_ACTIVE_BATCH_DATA):
            break
        # run dbt
        subprocess.run(
            ['dbt', 'run', '--profiles-dir', '/opt/airflow/project/dbt/pharmatrace'], 
            cwd='/opt/airflow/project/dbt/pharmatrace',
            check=True
        )
        # cleanup
        subprocess.run(['python', '/opt/airflow/project/scripts/cleanup_bronze_active.py'], check=True)


with DAG(
    dag_id="pharmatrace_dag",
    schedule=None,
    start_date=datetime(2026, 8, 1),
    end_date=None,
    catchup=False
) as dag:
    ingest_openfda_data() >> process_all_batches()


