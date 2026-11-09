# Проверка airflow и подключение к БД

import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def check_postgres():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    logging.info("Тест подключения: к citybikes")

    # Параметры подключения через webhook
    hook=PostgresHook(postgres_conn_id="citybikes_pg")

    # простая проверка
    db, u=hook.get_first(
        "SELECT current_database(), current_user;"
    )

    logging.info("Подключение успешно: база=%s, пользователь=%s", db, u)


with DAG(
    dag_id="check_citybikes",
    description="Проверка airflow и подключения к Postgresql",
    schedule=None,  # вручную
    tags=["citybikes", "check"],
) as dag:
    dag_start=EmptyOperator(task_id='dag_start')
    dag_end=EmptyOperator(task_id='dag_end')
    test_dag=PythonOperator(
        task_id="check_postgres",
        python_callable=check_postgres,
    )
    dag_start >> test_dag >> dag_end
