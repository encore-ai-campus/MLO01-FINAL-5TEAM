FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir 'pyspark==4.2.0' 'delta-spark==4.4.1' \
    'feast[spark,redis,postgres]==0.68.0' 'redis==7.4.1' 'prometheus-client==0.24.1' \
    'soda-sparkdf==4.31.0' psutil
ENV SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON=python
WORKDIR /test
