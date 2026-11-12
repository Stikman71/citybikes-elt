# CityBikes

Финальный проект по загрузке, преобразованию и визуализации данных из набора CityBikes.

## Стек технологий

Python, Airflow, Docker, SQL, Spark, Hadoop, Metabase, Git

## Текущее состояние

Реализована загрузка сырых данных CityBikes в PostgreSQL через Airflow. Spark создает One Big Table из RAW и сохраняет в HDFS в формате Parquet. Запуск обработки выполняется отдельным DAG Airflow

## Добавленные файлы проекта

- `compose.yaml` - запуск PostgreSQL, Airflow, Spark и Hadoop HDFS; подключение каталогов проекта
- `spark_jobs/prepare_spark_obt.py` - преобразование RAW-таблиц в One Big Table
- `spark_jobs/create_spark_session_obt.py` - чтение PostgreSQL через Spark и запись Parquet в HDFS
- `spark_jobs/spark_server.py` - запуск Spark-задачи по запросу от Airflow
- `dags/load_citybikes_obt.py` - DAG формирования витрины в HDFS

## Описание данных

Исходные JSON-файлы в Git не добавляются. Для запуска их необходимо разместить в `data/`:

- `data/networks.json` - список велосипедных сетей
- `data/stations_output/` - JSON-файлы со станциями сетей

## Запуск

Из корня проекта:

```bash
sudo docker compose up -d --wait
```

Для Spark необходим PostgreSQL JDBC-драйвер:

```bash
mkdir -p spark_jobs/jars
curl -fL --retry 3 -o spark_jobs/jars/postgresql-42.7.14.jar \
  https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar
```

Перед запуском DAG `citybikes_obt` создать каталог в HDFS (опционально):

```bash
sudo docker compose exec namenode hdfs dfs -mkdir -p /citybikes
```

DAG `citybikes_raw` и `citybikes_obt` запускаются вручную через Airflow

## Проверка
Проверка сохранёных данных HDFS:

```bash
sudo docker compose exec namenode hdfs dfs -ls /citybikes
```
