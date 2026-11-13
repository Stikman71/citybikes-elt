"""Локальный запуск Spark из Airflow"""

import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer


SPARK_SUBMIT=[
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "--conf", "spark.hadoop.dfs.replication=1",
    "--conf", "spark.sql.session.timeZone=UTC",
    "--jars", "/opt/spark/work-dir/jars/postgresql-42.7.14.jar",
    "/opt/spark/work-dir/create_spark_session_obt.py",
]

SPARK_SUBMIT_CLICKHOUSE=[
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "/opt/spark/work-dir/load_citybikes_clickhouse.py",
]

JOBS={
    "/run-obt": SPARK_SUBMIT,
    "/run-clickhouse": SPARK_SUBMIT_CLICKHOUSE,
}


class SparkRunner(BaseHTTPRequestHandler):

    def _reply(self, status, message):
        body=message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        """Обработка GET-запросов"""
        if self.path=="/health":
            self._reply(200, "ok")
        else:
            self._reply(404, "not found")


    def do_POST(self):
        """Обработка POST-запросов"""
        # Выбирает Spark-задачу по адресу запроса
        task=JOBS.get(self.path)
        if task is None:
            self._reply(404, "not found")
            return
        self.run_job(task)



    def run_job(self,task):
        """Запуск spark для формирования obt"""
        try:
            res=subprocess.run(
                task,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=180,
                check=False,
            )
            self._reply(
                200 if res.returncode==0 else 500,
                res.stdout
            )
        except subprocess.TimeoutExpired:
            self._reply(500, "Spark выполняется более 3 мин")



if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8765), SparkRunner).serve_forever()
