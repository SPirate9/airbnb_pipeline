from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("ML_Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("SELECT * FROM silver_joined")

# **1. Prédiction du prix**
print("Training models to predict price...")
indexer_price = StringIndexer(inputCol="room_type", outputCol="room_type_index")
assembler_price = VectorAssembler(
    inputCols=["room_type_index", "accommodates", "bedrooms", "beds", "cleaning_fee", 
               "security_deposit", "extra_people", "host_response_rate", "review_scores_rating"],
    outputCol="features"
)
pipeline_price = Pipeline(stages=[indexer_price, assembler_price])
df_prepared_price = pipeline_price.fit(df).transform(df)

train_price, test_price = df_prepared_price.randomSplit([0.8, 0.2], seed=42)

# Tester plusieurs modèles de régression
models_price = {
    "LinearRegression": LinearRegression(featuresCol="features", labelCol="price"),
    "DecisionTree": DecisionTreeRegressor(featuresCol="features", labelCol="price"),
    "RandomForest": RandomForestRegressor(featuresCol="features", labelCol="price")
}

evaluator_price = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
best_model_price = None
best_rmse_price = float("inf")

for name, model in models_price.items():
    print(f"Training {name}...")
    trained_model = model.fit(train_price)
    predictions = trained_model.transform(test_price)
    rmse = evaluator_price.evaluate(predictions)
    print(f"{name} RMSE: {rmse}")
    
    if rmse < best_rmse_price:
        best_rmse_price = rmse
        best_model_price = trained_model

# Sauvegarder les prédictions du meilleur modèle
predictions_price = best_model_price.transform(test_price)
predictions_price.select("listing_id","room_type", "price", "prediction") \
    .write.mode("overwrite") \
    .parquet("gold/predictions_price")

print(f"Best model for price prediction: {type(best_model_price).__name__} with RMSE: {best_rmse_price}")

# **2. Prédiction de la probabilité de réservation**
print("Training model to predict reservation probability...")
df = df.withColumn("reserved", (df["reviews_per_month"] > 0).cast("integer"))
assembler_reserved = VectorAssembler(
    inputCols=["room_type_index", "price", "review_scores_rating", "availability_365"],
    outputCol="features"
)
pipeline_reserved = Pipeline(stages=[indexer_price, assembler_reserved])
df_prepared_reserved = pipeline_reserved.fit(df).transform(df)

train_reserved, test_reserved = df_prepared_reserved.randomSplit([0.8, 0.2], seed=42)
lr_reserved = LogisticRegression(featuresCol="features", labelCol="reserved")
model_reserved = lr_reserved.fit(train_reserved)
predictions_reserved = model_reserved.transform(test_reserved)

evaluator_reserved = MulticlassClassificationEvaluator(labelCol="reserved", predictionCol="prediction", metricName="accuracy")
accuracy_reserved = evaluator_reserved.evaluate(predictions_reserved)
print(f"Reservation Prediction Accuracy: {accuracy_reserved}")

predictions_reserved.select("listing_id","room_type", "price", "reserved", "prediction") \
    .write.mode("overwrite") \
    .parquet("gold/predictions_reserved")

# **3. Prédiction des notes des avis**
print("Training model to predict review scores...")
assembler_rating = VectorAssembler(
    inputCols=["room_type_index", "price", "cleaning_fee", "security_deposit", 
               "host_response_rate", "reviews_per_month"],
    outputCol="features"
)
pipeline_rating = Pipeline(stages=[indexer_price, assembler_rating])
df_prepared_rating = pipeline_rating.fit(df).transform(df)

train_rating, test_rating = df_prepared_rating.randomSplit([0.8, 0.2], seed=42)
dt_rating = DecisionTreeRegressor(featuresCol="features", labelCol="review_scores_rating")
model_rating = dt_rating.fit(train_rating)
predictions_rating = model_rating.transform(test_rating)

evaluator_rating = RegressionEvaluator(labelCol="review_scores_rating", predictionCol="prediction", metricName="rmse")
rmse_rating = evaluator_rating.evaluate(predictions_rating)
print(f"Review Scores Prediction RMSE: {rmse_rating}")

predictions_rating.select("listing_id","room_type", "review_scores_rating", "prediction") \
    .write.mode("overwrite") \
    .parquet("gold/predictions_rating")

# **4. Analyse des sentiments des commentaires**
print("Training model for sentiment analysis...")
tokenizer = Tokenizer(inputCol="comments", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
vectorizer = CountVectorizer(inputCol="filtered", outputCol="features")
nb = NaiveBayes(featuresCol="features", labelCol="sentiment")

# Ajouter une colonne fictive pour les sentiments (positif = 1, négatif = 0)
df_sentiment = df.withColumn("sentiment", (df["review_scores_rating"] > 8).cast("integer"))
pipeline_sentiment = Pipeline(stages=[tokenizer, remover, vectorizer, nb])
model_sentiment = pipeline_sentiment.fit(df_sentiment)
predictions_sentiment = model_sentiment.transform(df_sentiment)

evaluator_sentiment = MulticlassClassificationEvaluator(labelCol="sentiment", predictionCol="prediction", metricName="accuracy")
accuracy_sentiment = evaluator_sentiment.evaluate(predictions_sentiment)
print(f"Sentiment Analysis Accuracy: {accuracy_sentiment}")

predictions_sentiment.select("listing_id","comments", "sentiment", "prediction") \
    .write.mode("overwrite") \
    .parquet("gold/predictions_sentiment")

df_gold = spark.sql("SELECT * FROM silver_joined")

df_gold.write.mode("overwrite").parquet("gold/listing_summary")