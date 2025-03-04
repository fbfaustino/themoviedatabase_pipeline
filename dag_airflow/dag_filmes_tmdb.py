from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='pipeline_tmdb',
    default_args=default_args,
    description='Executa o script de filmes IMDb e o código de transformação dbt',
    schedule_interval=None,
    catchup=False,
) as dag:

    executar_filmes = BashOperator(
        task_id='executar_filmes_py',
        bash_command='python3 /opt/airflow/pipelines/tmdb/main_filme.py',
        dag=dag
    )

    executar_detalhes_filmes = BashOperator(
        task_id='executar_detalhes_filmes_py',
        bash_command='python3 /opt/airflow/pipelines/tmdb/main_detalhes_filme.py',
        dag=dag
    )

    executar_dbt_exclude_silver = BashOperator(
        task_id='executar_dbt_exclude_silver',
        cwd='/opt/airflow/dbt_projetos/job_themoviedatabase_dbt',
        bash_command='dbt clean && dbt compile && dbt run --exclude silver',
        dag=dag
    )

    executar_dbt_silver = BashOperator(
        task_id='executar_dbt_silver',
        cwd='/opt/airflow/dbt_projetos/job_themoviedatabase_dbt',
        bash_command='dbt clean && dbt compile && dbt run --select silver',
        dag=dag
    )

    executar_filmes >> executar_detalhes_filmes >> executar_dbt_silver >> executar_dbt_exclude_silver
