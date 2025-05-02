from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder \
    .appName("Transform") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "silver") \
    .getOrCreate()

df_reviews = spark.read.parquet("bronze/reviews/")
df_listings = spark.read.parquet("bronze/listings/")

df_reviews = df_reviews.drop("year", "month", "day", "processing_date")
df_listings = df_listings.drop("year", "month", "day", "processing_date")

df_reviews_clean = df_reviews.withColumnRenamed("id", "review_id")
df_listings_clean = df_listings.withColumn("price", regexp_replace(col("price"), "[$,]", "").cast("double"))

df_joined = df_reviews_clean.join(df_listings_clean, df_reviews_clean["listing_id"] == df_listings_clean["id"])
df_joined.write.mode("overwrite").saveAsTable("silver_joined")


