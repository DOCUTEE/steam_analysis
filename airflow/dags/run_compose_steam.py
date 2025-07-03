from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='run_compose_steam',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run docker compose for Steam Spark pipeline',
    tags=['docker', 'steam', 'spark'],
) as dag:

    run_compose = BashOperator(
        task_id='run_compose_steam',
        bash_command='docker exec spark_steam-spark-master-1 ls /',
        do_xcom_push=False,  # prevent storing large output
    )
