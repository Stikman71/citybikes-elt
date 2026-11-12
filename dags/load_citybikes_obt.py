"""Задание 2: запуск spark и запись One Big Table в hdfs"""

from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Внутренний адрес spark
SPARK_URL = "http://spark:8765/run-obt"


def run_spark_obt():
    """Запуск основного скрипта по переносу данных в obt"""
    req=Request(SPARK_URL, data=b"", method="POST")

    try:
        with urlopen(req, timeout=180) as res:
            print(res.read().decode("utf-8", errors="replace"))
    except HTTPError as error:
        # логирование ошибки
        print(error.read().decode("utf-8", errors="replace"))
        raise


with DAG(
    dag_id="citybikes_obt",
    description="Задание 2: RAW -> OBT -> HDFS",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,  # ручной запуск из Airflow
    catchup=False,
    max_active_runs=1,
    tags=["citybikes", "obt", "task-2"],
) as dag:
    dag_start=EmptyOperator(task_id="dag_start")

    create_obt=PythonOperator(
        task_id="create_obt",
        python_callable=run_spark_obt,
    )

    dag_end=EmptyOperator(task_id="dag_end")

    dag_start >> create_obt >> dag_end
