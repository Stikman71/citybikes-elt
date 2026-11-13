"""Загрузка витрины CityBikes из HDFS в ClickHouse."""

import argparse
from getpass import getpass
from itertools import islice
import os
from uuid import uuid4

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


HDFS_PATH = "hdfs://namenode:9000/my_user/dm"
DATABASE = "citybikes"
TABLE = "citybikes_obt"
BATCH_SIZE = 5000

# Порядок полей совпадает со схемой Parquet. Его же используем при вставке.
SCHEMA = {
    "network_id": "Nullable(String)",
    "network_name": "Nullable(String)",
    "network_latitude": "Nullable(Float64)",
    "network_longitude": "Nullable(Float64)",
    "network_city": "Nullable(String)",
    "network_country": "Nullable(String)",
    "network_company": "Array(Nullable(String))",
    "network_system": "Nullable(String)",
    "station_name": "Nullable(String)",
    "station_latitude": "Nullable(Float64)",
    "station_longitude": "Nullable(Float64)",
    "station_timestamp": "Nullable(DateTime64(6, 'UTC'))",
    "station_free_bikes": "Nullable(Int32)",
    "station_empty_slots": "Nullable(Int32)",
    "station_renting": "Nullable(Bool)",
    "station_returning": "Nullable(Bool)",
    "station_last_updated": "Nullable(DateTime64(6, 'UTC'))",
    "station_slots": "Nullable(Int32)",
    "station_ebikes": "Nullable(Int32)",
    "station_has_ebikes": "Nullable(Bool)",
    "station_payment": "Array(Nullable(String))",
    "station_payment_terminal": "Nullable(Bool)",
    "station_address": "Nullable(String)",
    "station_post_code": "Nullable(String)",
    "companies_count": "Nullable(Int32)",
    "network_station": "Nullable(String)",
}
COLUMNS = list(SCHEMA)


def prepare_data(obt):
    # DateTime64(6) принимает микросекунды от начала эпохи Unix.
    # Так часовой пояс Python не влияет на передаваемое время.
    timestamps = {"station_timestamp", "station_last_updated"}
    arrays = {"network_company", "station_payment"}
    columns = []
    for name in COLUMNS:
        column = F.col(name)
        if name in timestamps:
            column = F.unix_micros(column)
        elif name in arrays:
            # ClickHouse не принимает NULL вместо массива, но принимает NULL внутри.
            column = F.coalesce(column, F.array().cast("array<string>"))
        columns.append(column.alias(name))
    return obt.select(*columns)


def publish_obt(client, data, expected_rows):
    if not expected_rows:
        raise ValueError("В HDFS нет строк. Таблица ClickHouse не изменена.")

    # У каждого запуска своя промежуточная таблица. Рабочая витрина
    # остаётся доступной, пока новая версия не загружена целиком.
    staging = f"{TABLE}_load_{uuid4().hex}"
    ddl_columns = ",\n            ".join(
        f"{name} {type_name}" for name, type_name in SCHEMA.items()
    )
    client.command(f"""
        CREATE TABLE {staging} (
            {ddl_columns}
        )
        ENGINE = MergeTree
        ORDER BY (ifNull(network_id, ''), ifNull(station_name, ''))
    """)

    try:
        rows = iter(data.toLocalIterator())
        inserted = 0
        while True:
            batch = [tuple(row) for row in islice(rows, BATCH_SIZE)]
            if not batch:
                break
            client.insert(staging, batch, column_names=COLUMNS)
            inserted += len(batch)
            print(f"Загружено {inserted} из {expected_rows} строк", flush=True)

        stored_rows = int(client.command(f"SELECT count() FROM {staging}"))
        if inserted != expected_rows or stored_rows != expected_rows:
            raise RuntimeError(
                f"Число строк не совпало: HDFS={expected_rows}, "
                f"отправлено={inserted}, ClickHouse={stored_rows}"
            )

        if int(client.command(f"EXISTS TABLE {TABLE}")):
            # После обмена под промежуточным именем остаётся предыдущая витрина.
            client.command(f"EXCHANGE TABLES {TABLE} AND {staging}")
        else:
            client.command(f"RENAME TABLE {staging} TO {TABLE}")
    finally:
        # Если загрузка сорвалась, удаляем только таблицу этого запуска.
        try:
            client.command(f"DROP TABLE IF EXISTS {staging}")
        except Exception as exc:
            print(f"Не удалось удалить промежуточную таблицу {staging}: {exc}", flush=True)

    print(f"Готово: {DATABASE}.{TABLE}, {stored_rows} строк", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ask-password", action="store_true", help="Запросить пароль вручную")
    args = parser.parse_args()
    password = (
        getpass("Пароль ClickHouse: ")
        if args.ask_password
        else os.environ.get("CLICKHOUSE_PASSWORD", "")
    )

    spark = (
        SparkSession.builder.appName("CityBikes ClickHouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        obt = spark.read.parquet(HDFS_PATH)
        data = prepare_data(obt)
        expected_rows = data.count()
        print(f"HDFS: {HDFS_PATH}, {expected_rows} строк, {len(COLUMNS)} полей", flush=True)

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="citybikes",
            password=password,
            database=DATABASE,
        )
        try:
            publish_obt(client, data, expected_rows)
        finally:
            client.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
