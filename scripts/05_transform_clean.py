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

df_reviews_clean = df_reviews \
    .withColumnRenamed("id", "review_id") \
    .fillna({"comments": ""})
df_listings_clean = df_listings \
    .withColumn("price", regexp_replace("price", "[$,]", "").cast("double")) \
    .withColumn("cleaning_fee", regexp_replace("cleaning_fee", "[$,]", "").cast("double")) \
    .withColumn("security_deposit", regexp_replace("security_deposit", "[$,]", "").cast("double")) \
    .withColumn("extra_people", regexp_replace("extra_people", "[$,]", "").cast("double")) \
    .withColumn("host_response_rate", regexp_replace("host_response_rate", "[% ]", "").cast("double")) \
    .withColumn("review_scores_rating", col("review_scores_rating").cast("double")) \
    .withColumn("reviews_per_month", col("reviews_per_month").cast("double")) \
    .withColumn("accommodates", col("accommodates").cast("int")) \
    .withColumn("bedrooms", col("bedrooms").cast("double")) \
    .withColumn("beds", col("beds").cast("double")) \
    .withColumn("availability_365", col("availability_365").cast("int")) \
    .withColumn("latitude", col("latitude").cast("double")) \
    .withColumn("longitude", col("longitude").cast("double")) \
    .select("id", "host_id", "neighbourhood_cleansed", "room_type", "price", "cleaning_fee", 
            "security_deposit", "extra_people", "host_response_rate", "review_scores_rating", 
            "reviews_per_month", "accommodates", "bedrooms", "beds", "availability_365", 
            "latitude", "longitude")

df_listings_clean = df_listings_clean.fillna({
    "price": 0.0,
    "cleaning_fee": 0.0,
    "security_deposit": 0.0,
    "extra_people": 0.0,
    "host_response_rate": 0.0,
    "review_scores_rating": 0.0,
    "reviews_per_month": 0.0,
    "accommodates": 0,
    "bedrooms": 0.0,
    "beds": 0.0,
    "availability_365": 0,
    "latitude": 0.0,
    "longitude": 0.0
})

df_listings_clean = df_listings_clean \
    .filter(col("price").isNotNull()) \
    .filter(col("room_type").isNotNull()) \
    .filter(col("host_id").isNotNull()) \
    .filter(col("neighbourhood_cleansed").isNotNull())

df_joined = df_reviews_clean.join(df_listings_clean, df_reviews_clean["listing_id"] == df_listings_clean["id"])
df_joined.write.mode("overwrite").saveAsTable("silver_joined")


