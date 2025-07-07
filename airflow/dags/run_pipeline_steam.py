from airflow import DAG
from airflow.operators.bash import BashOperator
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

    today = date.today().strftime("%Y-%m-%d")
    
    run_extract_review = BashOperator(
        task_id='run_extract_review',
        bash_command=f'docker exec spark_steam-spark-master-1 bash /opt/spark-app/bronze_script/run_extract_review.sh {today} '
    )
    run_extract_game = BashOperator(
        task_id='run_extract_game',
        bash_command='docker exec spark_steam-spark-master-1 bash /opt/spark-app/bronze_script/run_extract_game.sh '
    )
    run_clean_review = BashOperator(
        task_id='run_clean_review',
        bash_command=f'docker exec spark_steam-spark-master-1 bash /opt/spark-app/silver_script/run_clean_reviews.sh {today} '
    )
    run_clean_game = BashOperator(
        task_id='run_clean_game',
        bash_command='docker exec spark_steam-spark-master-1 bash /opt/spark-app/silver_script/run_clean_games.sh '
    )

    run_extract_review >> run_clean_review
    run_extract_game >> run_clean_game


    
    
