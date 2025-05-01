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

df_predictions = spark.read.parquet("gold/predictions/")
df_predictions.write.jdbc(
    url=jdbc_url,
    table="listing_predictions",
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
