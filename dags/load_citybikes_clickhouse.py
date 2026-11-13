"""Задание 3 Перенос hdfs в clickhouse"""

from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


SPARK_URL="http://spark:8765/run-clickhouse"


def run_spark_clickhouse():
    req=Request(SPARK_URL, data=b"", method="POST")
    try:
        with urlopen(req, timeout=210) as res:
            print(res.read().decode("utf-8", errors="replace"))
    except HTTPError as error:
        print(error.read().decode("utf-8", errors="replace"))
        raise


with DAG(
    dag_id="citybikes_clickhouse",
    description="Задание 3. HDFS -> ClickHouse",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["citybikes", "clickhouse", "task-3"],
) as dag:
    dag_start=EmptyOperator(task_id="dag_start")

    load_clickhouse=PythonOperator(
        task_id="load_clickhouse",
        python_callable=run_spark_clickhouse,
    )

    dag_end=EmptyOperator(task_id="dag_end")

    dag_start >> load_clickhouse >> dag_end
