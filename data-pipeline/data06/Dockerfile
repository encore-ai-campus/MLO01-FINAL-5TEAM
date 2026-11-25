FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu
USER root
RUN python3 -m pip install --no-cache-dir --target /opt/data06-python redis==5.2.1 kafka-python==2.2.15
COPY ivy.xml event_schema.py stream.py refined.py feedback.py recover_redis.py bootstrap_redis.py redis_contract.py redis_kafka.py /opt/data06/
