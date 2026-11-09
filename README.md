# CityBikes

Финальный проект по загрузке, преобразованию и визуализации данных из набора CityBikes.

## Стек технологий

Python, Airflow, Docker, SQL, Spark, Hadoop, Metabase, Git

## Текущее состояние

Созданы таблицы для хранения сырых данных CityBikes и реализовано их заполнение через Airflow.

## Файлы проекта

- `sql/init/create-databases.sql` - иницилизация базы данных `citybikes` и `airflow` и пользователей для них
- `sql/citybikes_raw.sql` - описание структуры таблиц для сырых данных
- `compose.yaml` - настройки запуска контейнеров; добавлено подключение каталогов `src` и `sql` для Airflow
- `src/citybikes_raw.py` - чтение JSON и загрузка данных в PostgreSQL
- `dags/load_citybikes_raw.py` - DAG загрузки и проверки данных

## Описание данных

Исходные JSON-файлы в Git не добавлены. Для запуска их необходимо разместить в `data/`:

- `data/networks.json` - список велосипедных сетей
- `data/stations_output/` - JSON-файлы со станциями сетей

## Запуск

Из корня проекта:

```bash
sudo docker compose up -d --wait
```

## Проверка

```bash
sudo docker compose exec postgres psql -U postgres -d postgres -c "SELECT datname FROM pg_database WHERE datname IN ('airflow', 'citybikes') ORDER BY datname;"
```
