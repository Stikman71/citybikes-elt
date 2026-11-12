"""Задание 2: One Big Table"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Обьявление схем

location_schema=StructType([
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
])

company_schema=ArrayType(StringType())

extra_schema=StructType([
    StructField("renting", StringType()),   # renting/returning - true/false и 1/0
    StructField("returning", StringType()), # renting/returning - true/false и 1/0
    StructField("last_updated", StringType()),
    StructField("slots", IntegerType()),
    StructField("ebikes", IntegerType()),
    StructField("has_ebikes", BooleanType()),
    StructField("payment", ArrayType(StringType())),
    StructField("payment-terminal", BooleanType()),
    StructField("address", StringType()),
    StructField("post_code", StringType()),
])



def _to_boolean(value):
    """Приводит 1/0 и true/false к bool - остальные значения - NULL"""
    val=F.lower(F.trim(value))
    return (
        F.when(val.isin("1", "true"), F.lit(True))
        .when(val.isin("0", "false"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def _to_timestamp(value):
    """Перевод к timestamp"""
    val=F.trim(value)
    return F.when(
        val.rlike(r"^[0-9]+$"),
        F.from_unixtime(val.cast("long")).cast("timestamp"),
    ).otherwise(F.to_timestamp(val))


def make_obt(networks: DataFrame, stations: DataFrame)->DataFrame:
    """Создание большой таблицы из jsonb - location, company, extra
    """

    # Приведение к подготовленным схемам
    network=(
        networks
        .withColumn("location_data", F.from_json(F.col("location"), location_schema))
        .withColumn("company_data", F.from_json(F.col("company"), company_schema))
        .alias("ntw")
    )

    station=(
        stations
        .withColumn("extra_data", F.from_json(F.col("extra"), extra_schema))
        .alias("s")
    )

    res=network.join(
        station,
        F.col("ntw.id") == F.col("s.network_id"),
        "left", # сохранить сети где нет станций
    )

    return res.select(
        # Данные сети
        F.trim(F.col("ntw.id")).alias("network_id"),
        F.trim(F.col("ntw.name")).alias("network_name"),
        F.col("ntw.location_data.latitude").alias("network_latitude"),
        F.col("ntw.location_data.longitude").alias("network_longitude"),
        F.trim(F.col("ntw.location_data.city")).alias("network_city"),
        F.trim(F.col("ntw.location_data.country")).alias("network_country"),
        F.transform(F.col("ntw.company_data"), lambda x: F.trim(x)).alias("network_company"),
        F.trim(F.col("ntw.system")).alias("network_system"),

        # Данные станции
        F.trim(F.col("s.name")).alias("station_name"),
        F.col("s.latitude").cast("double").alias("station_latitude"),
        F.col("s.longitude").cast("double").alias("station_longitude"),
        _to_timestamp(F.col("s.timestamp")).alias("station_timestamp"),
        F.col("s.free_bikes").cast("int").alias("station_free_bikes"),
        F.col("s.empty_slots").cast("int").alias("station_empty_slots"),
        _to_boolean(F.col("s.extra_data.renting")).alias("station_renting"),
        _to_boolean(F.col("s.extra_data.returning")).alias("station_returning"),
        _to_timestamp(F.col("s.extra_data.last_updated")).alias("station_last_updated"),
        F.col("s.extra_data.slots").cast("int").alias("station_slots"),
        F.col("s.extra_data.ebikes").cast("int").alias("station_ebikes"),
        F.col("s.extra_data.has_ebikes").alias("station_has_ebikes"),
        F.transform(F.col("s.extra_data.payment"), lambda x: F.trim(x)).alias("station_payment"),
        F.col("s.extra_data").getField("payment-terminal").alias("station_payment_terminal"),
        F.trim(F.col("s.extra_data.address")).alias("station_address"),
        F.trim(F.col("s.extra_data.post_code")).alias("station_post_code"),

        # пункт companies_count/network_station 
        F.coalesce(F.size(F.col("ntw.company_data")), F.lit(0)).cast("int").alias("companies_count"),
        F.when(
            F.col("ntw.name").isNotNull() & F.col("s.name").isNotNull(),
            F.concat(F.trim(F.col("ntw.name")), F.lit("/"), F.trim(F.col("s.name"))),
        ).alias("network_station"),
    )
