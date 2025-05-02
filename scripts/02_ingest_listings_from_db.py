from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, year, month, dayofmonth

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

df_partitioned = df.withColumn("processing_date", current_date()) \
    .withColumn("year", year("processing_date")) \
    .withColumn("month", month("processing_date")) \
    .withColumn("day", dayofmonth("processing_date"))

df_partitioned.repartition(3) \
    .write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("bronze/listings/")
