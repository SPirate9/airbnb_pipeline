from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("ML_Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("SELECT * FROM silver_joined")

indexer = StringIndexer(inputCol="room_type", outputCol="room_type_index")
assembler = VectorAssembler(
    inputCols=["room_type_index", "price", "number_of_reviews"],
    outputCol="features"
)
pipeline = Pipeline(stages=[indexer, assembler])
df_prepared = pipeline.fit(df).transform(df)

train, test = df_prepared.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(featuresCol="features", labelCol="price")
model = lr.fit(train)

predictions = model.transform(test)

evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE):", rmse)

predictions.select("room_type", "price", "prediction") \
    .write.mode("overwrite") \
    .parquet("gold/predictions")

df_gold = spark.sql("SELECT * FROM silver_joined")

df_gold.write.mode("overwrite").parquet("gold/listing_summary")