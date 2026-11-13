FROM apache/spark:3.5.5-scala2.12-java11-python3-ubuntu

USER root
COPY wheels/ /opt/wheels/
RUN python3 -m pip install --no-index --find-links=/opt/wheels clickhouse-connect==0.8.18

USER spark
