# CityBikes

Финальный проект по загрузке, преобразованию и визуализации данных из набора CityBikes.

## Стек технологий

Python, Airflow, Docker, SQL, PostgreSQL, Spark, Hadoop HDFS, ClickHouse, Metabase, Git

## Текущее состояние

DAG `citybikes_clickhouse` читает из `/my_user/dm` и загружает в таблицу `citybikes.citybikes_obt`. После сверки числа строк загрузчик заменяет рабочую таблицу. Повторный запуск не дописывает строки к предыдущей загрузке

## Новые файлы задания 3

- `docker/spark/Dockerfile` -сборка образа Spark с драйвером `clickhouse-connect`
- `spark_jobs/load_citybikes_clickhouse.py` - чтение Parquet и загрузка в ClickHouse
- `dags/load_citybikes_clickhouse.py` - запуск загрузки из Airflow

## Описание данных

Исходные JSON-файлы в Git не добавляются. Для загрузки в PostgreSQL размести их в `data/`:

- `data/networks.json` — список велосипедных сетей.
- `data/stations_output/` — JSON-файлы со станциями сетей.

## Запуск

Команды выполняются из корня проекта

Для сборки Spark с драйвером ClickHouse нужны пакеты в `docker/spark/wheels/`

```bash
mkdir -p docker/spark/wheels
sudo docker run --rm --user "$(id -u):$(id -g)" --entrypoint python3 \
  -v "$PWD/docker/spark/wheels:/wheels" \
  apache/spark:3.5.5-scala2.12-java11-python3-ubuntu \
  -m pip download --dest /wheels --find-links /wheels \
  --no-cache-dir --only-binary=:all: --timeout 60 --retries 5 \
  clickhouse-connect==0.8.18
```

Сборка образа Spark

```bash
sudo docker compose build spark
sudo docker compose up -d --wait
```

## Проверка

Проверка числоа строк в ClickHouse

```bash
sudo docker compose exec -T clickhouse clickhouse-client \
  --user citybikes \
  --query 'SELECT count() FROM citybikes.citybikes_obt'
```
