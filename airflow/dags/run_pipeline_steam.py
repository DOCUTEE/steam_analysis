from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
    
    download_dependencies = BashOperator(
        task_id='download_dependencies',
        bash_command='docker exec steam_analysis-spark-master-1 bash /opt/spark-app/dependences/download_packages.sh '
    ) 
    run_extract_review = BashOperator(
        task_id='run_extract_review',
        bash_command=f'docker exec steam_analysis-spark-master-1 bash /opt/spark-app/bronze_script/run_extract_review.sh {today} '
    )
    run_extract_game = BashOperator(
        task_id='run_extract_game',
        bash_command=f'docker exec steam_analysis-spark-master-1 bash /opt/spark-app/bronze_script/run_extract_game.sh {today}'
    )
    run_clean_review = BashOperator(
        task_id='run_clean_review',
        bash_command=f'docker exec steam_analysis-spark-master-1 bash /opt/spark-app/silver_script/run_clean_reviews.sh {today} '
    )
    run_clean_game = BashOperator(
        task_id='run_clean_game',
        bash_command='docker exec steam_analysis-spark-master-1 bash /opt/spark-app/silver_script/run_clean_games.sh '
    )
    run_dbt_modelling = BashOperator(
        task_id='run_dbt_modelling',
        bash_command=f'docker exec steam_analysis-dbt-1 bash /dbt/transform/run_dbt.sh {today} '
    )

    run_thrift = BashOperator(
        task_id='run_thrift',
        bash_command='docker exec steam_analysis-spark-master-1 bash /opt/spark-app/gold_script/run_thrift.sh '
    )

    wait_thrift = BashOperator(
        task_id='wait_thrift',
        bash_command='docker exec steam_analysis-spark-master-1 bash -c "until nc -z localhost 10000; do sleep 2; done"'
    )
    
    stop_thrift = BashOperator(
        task_id='stop_thrift',
        bash_command='docker exec steam_analysis-spark-master-1 bash stop_thrift.sh '
    )

    start_modelling = EmptyOperator(
        task_id='start_modelling'
    )
    download_dependencies >> start_extract
    start_extract >> [run_extract_game, run_extract_review] >> done_extract >> start_transform
    start_transform >> [run_clean_review, run_clean_game] >> start_modelling >> [run_thrift, wait_thrift]
    wait_thrift >> run_dbt_modelling >> stop_thrift >> done_transform
