from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, date

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='run_pipeline_steam',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run pipeline for processing reviews and games',
    tags=['docker', 'steam', 'spark'],
) as dag:


    # today = date.today().strftime("%Y-%m-%d")
    start_day = '2010-10-15'
    end_day = '2010-10-15'
    today = '2010-10-15'
    start_extract = EmptyOperator(task_id='start_extract')
    done_extract = EmptyOperator(task_id='done_extract')
    start_transform = EmptyOperator(task_id='start_transform')
    done_transform = EmptyOperator(task_id='done_transform')
    
    download_dependencies = SSHOperator(
        task_id='download_dependencies',
        ssh_conn_id='ssh_spark_master',
        command='/opt/spark-app/dependences/download_packages.sh ',
        do_xcom_push=True
    )
    run_extract_review = SSHOperator(
        task_id='run_extract_review',
        ssh_conn_id='ssh_spark_master',
        command=f'/opt/spark-app/bronze_script/run_extract_review.sh {today} ',
        do_xcom_push=False
    )
    run_extract_game = SSHOperator(
        task_id='run_extract_game',
        ssh_conn_id='ssh_spark_master',
        command='/opt/spark-app/bronze_script/run_extract_game.sh ',
        do_xcom_push=False
    )
    run_dbt_modelling = SSHOperator(
        task_id='run_dbt_modelling',
        ssh_conn_id='ssh_dbt',
        command=f'/dbt/transform/run_dbt.sh {today} ',
        do_xcom_push=False
    )

    run_thrift = SSHOperator(
        task_id='run_thrift',
        ssh_conn_id='ssh_spark_master',
        command='/opt/spark-app/gold_script/run_thrift.sh ',
        do_xcom_push=False
    )

    wait_thrift = SSHOperator(
        task_id='wait_thrift',
        ssh_conn_id='ssh_spark_master',
        command='bash -c "until nc -z localhost 10000; do sleep 2; done" ',
        do_xcom_push=False
    )
    download_dependencies >> start_extract
    start_extract >> [run_extract_game, run_extract_review] >> done_extract >> start_transform
    start_transform >> [run_thrift, wait_thrift]
    wait_thrift >> run_dbt_modelling >> done_transform
