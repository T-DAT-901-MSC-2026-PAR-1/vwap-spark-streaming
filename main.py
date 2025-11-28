import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as spark_sum, to_json, struct, concat, lit, lower
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SPARK_APP_NAME: str = os.environ.get('SPARK_APP_NAME')
SPARK_MASTER_URL: str = os.environ.get('SPARK_MASTER_URL')
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SUBSCRIBE_TOPICS: str = os.environ.get('KAFKA_SUBSCRIBE_TOPICS')
KAFKA_OUTPUT_TOPIC: str = os.environ.get('KAFKA_OUTPUT_TOPIC', 'vwap-results')

trade_schema = StructType([
    StructField("type", StringType(), True),
    StructField("market", StringType(), True),
    StructField("from_symbol", StringType(), True),
    StructField("to_symbol", StringType(), True),
    StructField("flags", StringType(), True),
    StructField("trade_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("price", StringType(), True),
    StructField("total_value", StringType(), True),
    StructField("received_ts", StringType(), True),
    StructField("ccseq", StringType(), True),
    StructField("timestamp_ns", StringType(), True),
    StructField("received_ts_ns", StringType(), True)
])

spark: SparkSession = SparkSession \
    .builder \
    .master(SPARK_MASTER_URL) \
    .appName(SPARK_APP_NAME) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.streaming.kafka.maxRatePerPartition", "50") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_trades = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribePattern", KAFKA_SUBSCRIBE_TOPICS) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_trades = raw_trades \
    .select(from_json(col("value").cast("string"), trade_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp_dt", (col("timestamp").cast("long")).cast("timestamp")) \
    .withColumn("price_num", col("price").cast(DoubleType())) \
    .withColumn("quantity_num", col("quantity").cast(DoubleType())) \
    .withColumn("price_x_quantity", col("price_num") * col("quantity_num")) \
    .filter(col("price_num").isNotNull() & col("quantity_num").isNotNull()) \
    .withWatermark("timestamp_dt", "30 seconds")

vwap_1min = parsed_trades \
    .groupBy(
    window(col("timestamp_dt"), "1 minute", "10 seconds"),
    col("market"),
    col("from_symbol"),
    col("to_symbol")
) \
    .agg(
    spark_sum("price_x_quantity").alias("total_price_x_quantity"),
    spark_sum("quantity_num").alias("total_quantity")
) \
    .withColumn("vwap", col("total_price_x_quantity") / col("total_quantity")) \
    .select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("market"),
    col("from_symbol"),
    col("to_symbol"),
    col("vwap"),
    col("total_quantity").alias("volume")
)


def main():
    output_dataframe = vwap_1min \
        .withColumn("key", concat(col("from_symbol"), lit("_"), col("to_symbol"))) \
        .withColumn("topic", lower(concat(
            lit("vwap-1min-"),
            col("from_symbol"),
            lit("-"),
            col("to_symbol")
        ))) \
        .withColumn("value", to_json(struct(
            col("window_start"),
            col("window_end"),
            col("market"),
            col("from_symbol"),
            col("to_symbol"),
            col("vwap"),
            col("volume")
        ))) \
        .selectExpr("CAST(topic AS STRING) as topic", "CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

    # Keep console output to check before flooding Kafka with no wanted data
    # query = output_dataframe \
    #     .writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()
    # query.awaitTermination()

    queryKafka =  output_dataframe \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("checkpointLocation", "/tmp/vwap-checkpoint") \
        .start()

    queryKafka.awaitTermination()

if __name__ == "__main__":
    main()