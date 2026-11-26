import json
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from feast import Entity, FeatureStore, FeatureView, Field, FileSource, ValueType
from feast.types import Int64
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def verify(spark, root):
    assert spark.version == "4.2.0"
    assert version("delta-spark") == "4.4.1"
    assert version("feast") == "0.68.0"

    topic = "feature-check-" + uuid4().hex
    config = {"bootstrap.servers": "localhost:9092"}
    admin = AdminClient(config)
    admin.create_topics([NewTopic(topic, 1, 1)], request_timeout=20)[topic].result()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    event = {"account_id": 17, "amount": 21, "event_timestamp": now.isoformat()}
    errors = []
    producer = Producer({**config, "message.timeout.ms": 20000})
    producer.produce(topic, value=json.dumps(event),
                     on_delivery=lambda error, _: errors.append(error) if error else None)
    if producer.flush(30) or errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}")

    messages = (spark.read.format("kafka")
                .option("kafka.bootstrap.servers", config["bootstrap.servers"])
                .option("subscribe", topic).option("startingOffsets", "earliest")
                .option("endingOffsets", "latest").load())
    schema = "account_id long, amount long, event_timestamp timestamp"
    features = (messages.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
                .select("e.account_id", "e.event_timestamp", (F.col("e.amount") * 2).alias("score")))
    delta_path = str(root / "delta")
    features.write.format("delta").save(delta_path)
    table = spark.read.format("delta").load(delta_path)
    assert table.count() == 1
    parquet = str(root / "features.parquet")
    # Feast's local file store reads Parquet exported from the committed Delta snapshot.
    table.toPandas().to_parquet(parquet, index=False)
    (root / "feature_store.yaml").write_text(
        "project: stack_check\nprovider: local\nregistry: registry.db\n"
        "offline_store:\n  type: file\nonline_store:\n  type: sqlite\n  path: online.db\n",
        encoding="utf-8",
    )
    store = FeatureStore(repo_path=str(root))
    entity = Entity(name="account", join_keys=["account_id"], value_type=ValueType.INT64)
    source = FileSource(name="account_scores_source", path=parquet, timestamp_field="event_timestamp")
    view = FeatureView(name="account_scores", entities=[entity], ttl=timedelta(days=1),
                       schema=[Field(name="score", dtype=Int64)], source=source)
    store.apply([entity, view])
    query = pd.DataFrame({"account_id": [17], "event_timestamp": [now + timedelta(seconds=1)]})
    result = store.get_historical_features(entity_df=query, features=["account_scores:score"]).to_df()
    assert result["score"].tolist() == [42], result
    print(f"PASS Kafka -> Spark -> Delta -> Parquet -> Feast: topic={topic} account_id=17 amount=21 score=42", flush=True)


if __name__ == "__main__":
    session = SparkSession.builder.appName("data-stack-integration").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        with TemporaryDirectory() as directory:
            verify(session, Path(directory))
    finally:
        session.stop()
