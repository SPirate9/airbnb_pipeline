from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StreamReviews").getOrCreate()

schema = StructType() \
    .add("listing_id", "integer") \
    .add("id", "integer") \
    .add("date", "string") \
    .add("reviewer_id", "integer") \
    .add("reviewer_name", "string") \
    .add("comments", "string")

df = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).csv("data/reviews_stream/")
df.writeStream.format("parquet") \
    .option("checkpointLocation", "bronze/reviews_ckpt") \
    .start("bronze/reviews/") \
    .awaitTermination()