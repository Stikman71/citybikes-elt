"""Задание 2.2: запуск Spark"""

import os
from pyspark.sql import SparkSession

from prepare_spark_obt import make_obt


TABLE_PREFIX="n_muravev"

# настройка подключения к бд
JDBC_URL="jdbc:postgresql://postgres:5432/citybikes"
PG_USER="citybikes"

# настройка hdfs
HDFS_PATH="hdfs://namenode:9000/citybikes"


def read_raw(spark: SparkSession, query: str):
    """Чтение таблиц postgre"""
    r=(
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", query)
        .option("user", PG_USER)
        .option("driver", "org.postgresql.Driver")
    )

    # пароль из окружения если есть
    if os.getenv("PG_PASSWORD"):
        r=r.option("password", os.environ["PG_PASSWORD"])

    return r.load()


def main():
    # Spark-сессия время в utc
    spark=(
        SparkSession.builder
        .appName("citybikes_obt")
        .config("spark.sql.session.timeZone", "UTC")
#	.config("spark.sql.session.timeZone")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(
    "Spark timezone:",
    repr(spark.conf.get("spark.sql.session.timeZone")),
    flush=True
)
    try:
        # чтение networks 
        networks=read_raw(spark, f"""
            SELECT
                id,
                name,
                location::text AS location,
                company::text AS company,
                system
            FROM public.{TABLE_PREFIX}_networks
        """)

        # чтение stations
        stations=read_raw(spark, f"""
            SELECT
                network_id,
                name,
                latitude,
                longitude,
                "timestamp",
                free_bikes,
                empty_slots,
                extra::text AS extra
            FROM public.{TABLE_PREFIX}_stations
        """)

        # создание большой таблицы
        obt=make_obt(networks, stations)

        # ЗаписьHDFS
        # При повторном запуске предыдущий результат заменяетс
        obt.write.mode("overwrite").parquet(HDFS_PATH)

        print(f"Сохранение в HDFS: {HDFS_PATH}")
        print(f"Число полей: {len(obt.columns)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
