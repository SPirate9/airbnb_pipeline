from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_date, year, month, dayofmonth

spark = SparkSession.builder.appName("StreamReviews").getOrCreate()

schema = StructType() \
    .add("listing_id", "integer") \
    .add("id", "integer") \
    .add("date", "string") \
    .add("reviewer_id", "integer") \
    .add("reviewer_name", "string") \
    .add("comments", "string")

df = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).csv("data/reviews_stream/")

df_partitioned = df.withColumn("processing_date", current_date()) \
    .withColumn("year", year("processing_date")) \
    .withColumn("month", month("processing_date")) \
    .withColumn("day", dayofmonth("processing_date"))

df_partitioned.repartition(3) \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "bronze/reviews_ckpt") \
    .partitionBy("year", "month", "day") \
    .option("path", "bronze/reviews/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()