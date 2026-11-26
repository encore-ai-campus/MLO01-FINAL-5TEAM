from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("image-check").getOrCreate()
try:
    assert spark.version == "4.2.0", spark.version
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    for name in (
        "org.apache.spark.sql.kafka010.KafkaSourceProvider",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "software.amazon.awssdk.services.s3.S3Client",
    ):
        loader.loadClass(name)
    with TemporaryDirectory() as path:
        spark.range(3).write.format("delta").mode("overwrite").save(path)
        assert spark.read.format("delta").load(path).count() == 3
finally:
    spark.stop()
