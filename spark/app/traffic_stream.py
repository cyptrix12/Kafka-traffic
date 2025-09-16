import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_csv, to_timestamp, window, sum as _sum, expr, lit, when, to_json, struct
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

KAFKA = os.getenv("KAFKA_BROKERS", "kafka:9092")
SRC_TOPIC = os.getenv("SOURCE_TOPIC", "traffic_events")
AGG_TOPIC = os.getenv("AGG_TOPIC", "traffic_agg")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "congestion_alerts")
THRESH = int(os.getenv("CONGESTION_THRESHOLD", "25"))

spark = (
    SparkSession.builder.appName("TrafficRT")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
])

# Read from Kafka (value is JSON from producer)
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", SRC_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("d"))
       .select(
           to_timestamp(col("d.timestamp")).alias("timestamp"),
           col("d.road_id").alias("road_id"),
           col("d.vehicle_count").cast("int").alias("vehicle_count")
       )
)

# Windowed aggregation: 1-min window, slide 30s
agg = (
    events.withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("road_id")
    )
    .agg(_sum("vehicle_count").alias("veh_sum"))
    .select(
        col("road_id"),
        col("window").start.alias("window_start"),
        col("window").end.alias("window_end"),
        col("veh_sum")
    )
)

# Congestion detection in Spark (threshold on windowed sum)
alerts = (
    agg.where(col("veh_sum") > lit(THRESH))
       .withColumn("severity", when(col("veh_sum") > THRESH * 1.5, lit("high")).otherwise(lit("moderate")))
)

# === Write aggregated stream to Kafka (JSON) ===
agg_out = (
    agg.selectExpr(
        "CAST(NULL AS STRING) as key",
        "to_json(named_struct('road_id', road_id, 'window_start', window_start, 'window_end', window_end, 'veh_sum', veh_sum)) as value"
    )
)

agg_q = (
    agg_out.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("topic", AGG_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints/agg")
    .outputMode("update")
    .start()
)

# === Write alerts to Kafka (JSON) ===
alerts_out = (
    alerts.selectExpr(
        "CAST(NULL AS STRING) as key",
        "to_json(named_struct('road_id', road_id, 'window_start', window_start, 'window_end', window_end, 'veh_sum', veh_sum, 'severity', severity)) as value"
    )
)

alerts_q = (
    alerts_out.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("topic", ALERT_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints/alerts")
    .outputMode("update")
    .start()
)

spark.streams.awaitAnyTermination()
