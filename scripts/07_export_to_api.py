from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExportAPI") \
    .config("spark.jars", "postgresql-42.7.5.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/airbnb"
properties = {
    "user": "saad",
    "password": "saad",
    "driver": "org.postgresql.Driver"
}
df_predictions_price = spark.read.parquet("gold/predictions_price/")
df_predictions_price.write.jdbc(
    url=jdbc_url,
    table="listing_predictions_price",
    mode="overwrite",
    properties=properties
)

df_predictions_reserved = spark.read.parquet("gold/predictions_reserved/")
df_predictions_reserved.write.jdbc(
    url=jdbc_url,
    table="listing_predictions_reserved",
    mode="overwrite",
    properties=properties
)

df_predictions_rating = spark.read.parquet("gold/predictions_rating/")
df_predictions_rating.write.jdbc(
    url=jdbc_url,
    table="listing_predictions_rating",
    mode="overwrite",
    properties=properties
)

df_predictions_sentiment = spark.read.parquet("gold/predictions_sentiment/")
df_predictions_sentiment.write.jdbc(
    url=jdbc_url,
    table="listing_predictions_sentiment",
    mode="overwrite",
    properties=properties
)

df_summary = spark.read.parquet("gold/listing_summary/")
df_summary.write.jdbc(
    url=jdbc_url,
    table="listing_summary",
    mode="overwrite",
    properties=properties
)