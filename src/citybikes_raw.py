"""
Загрузка citybikes в Postgre task-1
"""

import hashlib
import json
import logging
from pathlib import Path


LOG=logging.getLogger(__name__)

PREFIX_MARKER="NAME_PREFIX"


def check_prefix(prefix: str) -> str:
    if prefix==PREFIX_MARKER:
        raise ValueError(
            "dags/load_citybikes_raw.py проверить префикс"
        )
    return prefix


def _load_json(path: Path) -> dict:
    """
    Чтение JSON файла
    """
    with path.open("r", encoding="utf-8") as src:
        data=json.load(src)
    if not isinstance(data, dict):
        raise ValueError(f"Не JSON: {path}")
    return data


def network_to_task(record: dict) -> dict:
    """Сохраняет известные поля сети/ отсутствующие - null"""
    if not isinstance(record, dict) or not isinstance(record.get("id"), str):
        raise ValueError("У сети нет id")
    if not record["id"]:
        raise ValueError("Пустой id сети")
    
    # все найденные в networks.json
    fields=(
        "id", "name", "location", "href", "company", "gbfs_href",
        "system", "source", "ebikes", "license", "instances", "scooters",
    )
    return {field: record.get(field) for field in fields}


def execute_network_id(path: Path) -> str:
    """Извлечение id сети: stations_andria.json -> andria"""
    name=path.name
    if not name.startswith("stations_") or not name.endswith(".json"):
        raise ValueError(f"Что то не так с назваие файла: {name}")
    _id=name[len("stations_") : -len(".json")]
    if not _id:
        raise ValueError(f"Отсутствует id сети в названии файла: {name}")
    return _id


def _op_int(value: object, field: str) -> int | None:
    """
    Это поле должно быть строго числом или null
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} должен быть целым числом или null: {value!r}")
    return value


def _geo_temp(value: object, field: str) -> float:
    """
    Для полей позициирования
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} должен быть числом: {value!r}")
    return float(value)


def station_to_task(network_id: str, record: dict, source_file: str) -> dict:
    """
    Приводит поля файла к схеме задания

    citybikes поле !bikes означает доступные велосипеды/free — пустые

    """

    if not isinstance(record, dict):
        raise ValueError("Станция должна быть JSON-объектом")
    
    extra=record.get("extra")
    if extra is not None and not isinstance(extra, dict):
        raise ValueError("station.extra должен быть объектом или null")
    
    name=record.get("name")
    if name is not None and not isinstance(name, str):
        raise ValueError("station.name должен быть строкой или null")
    
    timestamp=record.get("timestamp")
    if timestamp is not None and not isinstance(timestamp, str):
        raise ValueError("station.timestamp должен быть строкой или null")

    latitude=_geo_temp(record.get("latitude"), "latitude")
    longitude=_geo_temp(record.get("longitude"), "longitude")

    # extra может отсутствовать-вместо None- пустой словарь
    # uid тоже может отсутствовать или повторяттся у разных станций

    uid=(extra or {}).get("uid")

    # собираем признаки станции в один список для первичного ключа
    # timestamp не берем так как может ментся у одной станции
    # Велосепедная сеть+uid станции если есть+название станции+широта+долгота
    identity=[network_id, str(uid) if uid is not None else None, name, latitude, longitude]

    # в идентификатор в строку фиксированной длины
    station_id=hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return {
        "station_id": station_id,
        "network_id": network_id,
        "name": name,
        "latitude": latitude,
        "longitude": longitude,
        "timestamp": timestamp,
        "free_bikes": _op_int(record.get("bikes"), "bikes"),
        "empty_slots": _op_int(record.get("free"), "free"),
        "extra": extra,
        "source_file": source_file,
    }



### БД >>
def prepare_station_batch(network_id: str, stations: list, source_file: str):
    """Убирает повторные станции внутри одного файла перед пакетным обновлением"""
    if not isinstance(stations, list):
        raise ValueError(f"Ожидался массив stations: {source_file}")
    unique: dict[str, dict]={}
    dups=0
    for station in stations:
        row=station_to_task(network_id, station, source_file)
        if row["station_id"] in unique:
            dups+=1
        unique[row["station_id"]]=row
    return list(unique.values()), dups


def create_tables(connection, prefix: str, schema_path: Path) -> None:
    """Создание таблиц"""

    prefix=check_prefix(prefix)
    schema=schema_path.read_text(encoding="utf-8")

    if PREFIX_MARKER not in schema:
        raise ValueError(f"Не найден шаблон {PREFIX_MARKER} в {schema_path}")
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(schema.replace(PREFIX_MARKER, prefix))

    LOG.info("Таблицы %s_networks и %s_stations готовы", prefix, prefix)


def load_networks(connection, prefix: str, json_path: Path) -> int:
    """Загружает networks - за раз"""
    from psycopg2.extras import Json, execute_values

    prefix=check_prefix(prefix)
    networks=_load_json(json_path).get("networks")

    if not isinstance(networks, list) or not networks:
        raise ValueError(f"Нет networks в {json_path}")
    
    rows=[network_to_task(row) for row in networks]
    if len({row["id"] for row in rows}) != len(rows):
        raise ValueError("В networks.json повторяются id")

    values = [(
        row["id"],
        row["name"],
        Json(row["location"]) if row["location"] is not None else None,
        row["href"],
        Json(row["company"]) if row["company"] is not None else None,
        row["gbfs_href"],
        row["system"],
        row["source"],
        row["ebikes"],
        Json(row["license"]) if row["license"] is not None else None,
        Json(row["instances"]) if row["instances"] is not None else None,
        row["scooters"],
    ) for row in rows]
    query = f"""
        INSERT INTO {prefix}_networks
            (id, name, location, href, company, gbfs_href, system,
             source, ebikes, license, instances, scooters)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            location = EXCLUDED.location,
            href = EXCLUDED.href,
            company = EXCLUDED.company,
            gbfs_href = EXCLUDED.gbfs_href,
            system = EXCLUDED.system,
            source = EXCLUDED.source,
            ebikes = EXCLUDED.ebikes,
            license = EXCLUDED.license,
            instances = EXCLUDED.instances,
            scooters = EXCLUDED.scooters
    """
    with connection:
        with connection.cursor() as cursor:
            execute_values(cursor, query, values, page_size=500)
    LOG.info("Сети: прочитано и загружено %d", len(rows))
    return len(rows)


def load_stations(connection, prefix: str, station_dir: Path) -> dict:
    """Загружает stations по однму"""
    from psycopg2.extras import Json, execute_values

    prefix = check_prefix(prefix)
    files = sorted(station_dir.glob("stations_*.json"))
    if not files:
        raise FileNotFoundError(f"Нет station-файлов в {station_dir}")

    query = f"""
        INSERT INTO {prefix}_stations
            (station_id, network_id, name, latitude, longitude,
             "timestamp", free_bikes, empty_slots, extra, source_file)
        VALUES %s
        ON CONFLICT (station_id) DO UPDATE SET
            network_id = EXCLUDED.network_id,
            name = EXCLUDED.name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            "timestamp" = EXCLUDED."timestamp",
            free_bikes = EXCLUDED.free_bikes,
            empty_slots = EXCLUDED.empty_slots,
            extra = EXCLUDED.extra,
            source_file = EXCLUDED.source_file
    """
    total_raw=total_unique=total_dups=0
    for index, path in enumerate(files, start=1):
        network_id=execute_network_id(path)
        stations=_load_json(path).get("stations")
        normalized, dups=prepare_station_batch(network_id, stations, path.name)
        values = [(
            row["station_id"],
            row["network_id"],
            row["name"],
            row["latitude"],
            row["longitude"],
            row["timestamp"],
            row["free_bikes"],
            row["empty_slots"],
            Json(row["extra"]) if row["extra"] is not None else None,
            row["source_file"],
        ) for row in normalized]
        if values:
            with connection:
                with connection.cursor() as cursor:
                    execute_values(cursor, query, values, page_size=500)
        total_raw+=len(stations)
        total_unique+=len(normalized)
        total_dups+=dups
        if index % 25 == 0 or index == len(files):
            LOG.info("Станции: обработано файлов %d/%d, строк %d", index, len(files), total_unique)

    result = {"files": len(files), "raw_rows": total_raw,
              "unique_rows": total_unique, "duplicates_in_files": total_dups}
    LOG.info("Загрузка станций завершена: %s", result)
    return result


def validate_loaded(connection, prefix: str) -> dict:
    """Проверяет итоговые количества и наличие связей stations -> networks."""
    prefix = check_prefix(prefix)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {prefix}_networks")
        networks = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {prefix}_stations")
        stations = cursor.fetchone()[0]
        cursor.execute(f"""
            SELECT COUNT(*) FROM {prefix}_stations s
            LEFT JOIN {prefix}_networks n ON n.id = s.network_id
            WHERE n.id IS NULL
        """)
        orphan_stations = cursor.fetchone()[0]

    result = {"networks": networks, "stations": stations, "orphan_stations": orphan_stations}
    LOG.info("Проверка RAW: %s", result)
    if not networks or not stations or orphan_stations:
        raise RuntimeError(f"Проверка загруженных данных не пройдена: {result}")
    return result
