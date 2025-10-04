from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

schema = StructType() \
    .add("id", StringType()) \
    .add("ts", StringType()) \
    .add("value", DoubleType()) \
    .add("type", StringType())

spark = SparkSession.builder.master("local[*]").appName("socket-json-consumer").getOrCreate()

raw = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9999).load()
parsed = raw.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = parsed.writeStream.format("console").option("truncate", False).start()

print("Consumer started — streaming to console. Ctrl+C to stop.")
query.awaitTermination()