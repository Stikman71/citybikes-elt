# CityBikes

Финальный проект по загрузке, преобразованию и визуализации данных из набора CityBikes.

## Стек технологий 

Python, Airflow, Docker, SQL, Spark, Hadoop, Metabase, Git

## Текущее состояние

Запущен PostgreSQL. Созданы базы данных `citybikes` и `airflow` и их пользователи.

База `citybikes` предназначена для данных проекта, `airflow` - для данных Airflow.

## Файлы проекта

- `sql/init/create-databases.sql` - иницилизация базы данных `citybikes` и `airflow` и пользователей для них
- `compose.yaml` - настройки запуска контейнера PostgreSQL 16 подключения SQL-файла и хранения данных

## Запуск

Из корня проекта:

```bash
sudo docker compose up -d --wait
```

## Проверка

```bash
sudo docker compose exec postgres psql -U postgres -d postgres -c "SELECT datname FROM pg_database WHERE datname IN ('airflow', 'citybikes') ORDER BY datname;"
```
