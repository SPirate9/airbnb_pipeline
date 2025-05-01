from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IngestListingsFromDB") \
    .config("spark.jars", "postgresql-42.7.5.jar") \
    .getOrCreate()

url = "jdbc:postgresql://localhost:5432/airbnb"
properties = {
    "user": "saad",
    "password": "saad",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=url, table="listings", properties=properties)
df.write.mode("overwrite").parquet("bronze/listings/")
