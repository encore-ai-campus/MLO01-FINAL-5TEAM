FROM spark:4.2.0-scala2.13-java21-python3-ubuntu AS spark_distribution
FROM python:3.12-slim-bookworm
COPY --from=spark_distribution /opt/spark /opt/spark
COPY --from=spark_distribution /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark PYSPARK_PYTHON=/usr/local/bin/python3
ENV PATH=/opt/java/openjdk/bin:/opt/spark/bin:$PATH
ENV PYTHONPATH=/opt/data06-python:/opt/data06
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir --target /opt/data06-python redis==7.4.1 confluent-kafka==2.16.1 \
    numpy==2.2.6 pandas==2.2.3 pyarrow==26.0.1 protobuf==6.33.5
COPY ivy.xml event_schema.py stream.py refined.py spark_decision.py feedback.py recover_redis.py bootstrap_redis.py redis_contract.py redis_kafka.py /opt/data06/
