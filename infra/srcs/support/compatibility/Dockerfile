# syntax=docker/dockerfile:1
FROM ubuntu:24.04 AS base
ENV VIRTUAL_ENV=/opt/venv PATH=/opt/venv/bin:$PATH
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates curl python3-venv python3-dev build-essential libpq-dev \
        openjdk-21-jre-headless libgomp1 procps tini \
    && rm -rf /var/lib/apt/lists/* && python3 -m venv /opt/venv
WORKDIR /opt/stack
ENTRYPOINT ["/usr/bin/tini", "--"]

FROM base AS airflow
COPY airflow/requirements.txt ./requirements.txt
RUN curl -fsSL --retry 3 https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt -o constraints.txt \
    && grep -q '^pyspark-client==' constraints.txt \
    && sed -i 's/^pyspark-client==.*/pyspark-client==4.2.0/' constraints.txt \
    && pip install --no-cache-dir -c constraints.txt -r requirements.txt \
    && pip check && airflow version
ENV AIRFLOW_HOME=/home/ubuntu/airflow
RUN install -d -o ubuntu -g ubuntu "$AIRFLOW_HOME"
USER ubuntu
WORKDIR /home/ubuntu
CMD ["airflow", "standalone"]

FROM base AS spark
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip check
ENV SPARK_CONF_DIR=/opt/stack/conf PYSPARK_PYTHON=/opt/venv/bin/python
COPY spark-defaults.conf conf/spark-defaults.conf
COPY smoke.py .
ARG SPARK_PACKAGES=io.delta:delta-spark_2.13:4.4.1,org.apache.hadoop:hadoop-aws:3.5.0,software.amazon.awssdk:bundle:2.35.4,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
RUN mkdir -p /opt/connectors && SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' \
        --conf spark.jars= --conf spark.jars.ivy=/tmp/ivy --repositories https://repo.maven.apache.org/maven2 \
        --packages "$SPARK_PACKAGES" smoke.py \
    && mv /tmp/ivy/jars/*.jar /opt/connectors/ && rm -rf /tmp/ivy
USER ubuntu
WORKDIR /home/ubuntu
RUN --network=none SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' /opt/stack/smoke.py
CMD ["python"]
