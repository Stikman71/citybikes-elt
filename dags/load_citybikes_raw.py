"""
    Задание 1: загрузка citybikes через Airflow
"""

from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from citybikes_raw import (
    create_tables as raw_create_tables,
    load_networks as raw_load_networks,
    load_stations as raw_load_stations,
    validate_loaded as raw_validate_loaded,
    check_prefix,
)


# Префикс для названий таблиц: заменить на имя и фамилию, например "i_ivanov"
TABLE_PREFIX = "n_muravev"

# подключения через хук
PG_CONN_ID = "citybikes_pg"

# Пути внутри контейнера Airflow
DATA_DIR = Path("/opt/airflow/data")
SCHEMA_PATH = Path("/opt/airflow/sql/citybikes_raw.sql")


def _postgres_connection():
    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()


def create_tables():
    check_prefix(TABLE_PREFIX)
    with closing(_postgres_connection()) as connection:
        raw_create_tables(connection, TABLE_PREFIX, SCHEMA_PATH)


def load_networks():
    with closing(_postgres_connection()) as connection:
        return raw_load_networks(connection, TABLE_PREFIX, DATA_DIR / "networks.json")


def load_stations():
    with closing(_postgres_connection()) as connection:
        return raw_load_stations(connection, TABLE_PREFIX, DATA_DIR / "stations_output")


def validate_loaded():
    with closing(_postgres_connection()) as connection:
        return raw_validate_loaded(connection, TABLE_PREFIX)


with DAG(
    dag_id="citybikes_raw",
    description="Задание 1: подготовленные JSON -> PostgreSQL",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,  # ручной запуск из Airflow
    catchup=False,
    max_active_runs=1,
    tags=["citybikes", "raw", "task-1"],
) as dag:
    dag_start=EmptyOperator(task_id='dag_start')
    create_tables=PythonOperator(
        task_id="create_tables", python_callable=create_tables
    )
    load_networks=PythonOperator(
        task_id="load_networks", python_callable=load_networks
    )
    load_stations=PythonOperator(
        task_id="load_stations", python_callable=load_stations
    )
    validate=PythonOperator(
        task_id="validate_loaded", python_callable=validate_loaded
    )
    dag_end=EmptyOperator(task_id='dag_end')

    dag_start >> create_tables >> load_networks >> load_stations >> validate >> dag_end
