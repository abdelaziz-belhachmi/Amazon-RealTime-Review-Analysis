from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType
import os
import logging
from threading import Thread
import time
from pyspark.sql.functions import when
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    # 1. Create Spark Session with necessary configurations
    logger.info("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Real-time Review Analysis") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,"
                "org.mongodb:mongodb-driver-sync:4.11.1,"
                "org.mongodb:mongodb-driver-core:4.11.1,"
                "org.mongodb:bson:4.11.1") \
        .config("spark.mongodb.output.uri", "mongodb://mongo:27017/sentiment.predictions") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017") \
        .config("spark.mongodb.write.database", "sentiment") \
        .config("spark.mongodb.write.collection", "predictions") \
        .config("spark.files.overwrite", "true") \
        .getOrCreate()
    logger.info("Spark Session created successfully")

    # 2. Define the schema for incoming JSON data
    schema = StructType([
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("reviewerName", StringType(), True),
        StructField("helpful", ArrayType(FloatType()), True),
        StructField("reviewText", StringType(), True),
        StructField("overall", FloatType(), True),
        StructField("summary", StringType(), True),
        StructField("unixReviewTime", FloatType(), True),
        StructField("reviewTime", StringType(), True)
    ])
    # 3. Model Pipeline
    logger.info("Setting up ML Pipeline...")
    
    # Create pipeline stages
    tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(
        maxIter=20,
        regParam=1.0,
        elasticNetParam=0.0,
        featuresCol="features",
        labelCol="label",
        predictionCol="predicted_sentiment"
    )

    # Create pipeline
    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
    logger.info("ML Pipeline created successfully")

    # ... rest of the streaming setup ...

    # 4. Create streaming DataFrame from Kafka
    logger.info("Setting up Kafka streaming...")
    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "reviews") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    logger.info("Kafka streaming setup complete")

    # 5. Parse JSON data and apply sentiment analysis
    parsed_df = streaming_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    # 6. Apply sentiment analysis
    # Create a small sample dataset with the same schema
    sample_data = spark.createDataFrame([
        ("This is a good product", 5.0),
        ("This is a bad product", 1.0),
        ("Average product", 3.0)
    ], ["reviewText", "overall"])
    
    # Add label column to sample data
    sample_data = sample_data.withColumn(
        "label",
        when(col("overall") > 3.0, 1.0)
        .when(col("overall") < 3.0, 0.0)
        .otherwise(2.0)
    )
    
    # Fit the pipeline once
    pipeline_model = pipeline.fit(sample_data)
    
    # Use the fitted model for streaming predictions
    predictions = pipeline_model.transform(parsed_df.withColumn(
        "label",
        when(col("overall") > 3.0, 1.0)
        .when(col("overall") < 3.0, 0.0)
        .otherwise(2.0)
    ))
    logger.info("Sentiment analysis transformation setup complete")

    # 7. Select relevant columns and prepare for MongoDB
    # Update the output selection to include more relevant fields
    output_df = predictions.select(
        col("reviewerID").alias("reviewer_id"),
        col("asin").alias("product_id"),
        col("reviewerName").alias("reviewer_name"),
        col("helpful").alias("helpfulness_votes"),
        col("reviewText").alias("review_text"),
        col("overall").alias("rating"),
        col("summary").alias("review_summary"),
        col("predicted_sentiment"),
        current_timestamp().alias("prediction_timestamp")
    )

    # 8. Define batch processing function
    def process_batch(df, epoch_id):
        try:
            count = df.count()
            logger.info("🔄 Processing New Batch:")
            logger.info(f"    Batch ID: {epoch_id}")
            logger.info(f"    Records: {count}")
            
            if count > 0:
                # Write to MongoDB using newer configuration
                logger.info("Writing to MongoDB...")
                df.write \
                    .format("com.mongodb.spark.sql.DefaultSource") \
                    .mode("append") \
                    .option("database", "sentiment") \
                    .option("collection", "predictions") \
                    .option("uri", "mongodb://mongo:27017") \
                    .save()
                logger.info("✅ Data Saved")
                sample = df.limit(1).collect()[0]
                logger.info("✅ Sample Prediction:")
                logger.info(f"    Review ID: {sample['reviewer_id']}")
                logger.info(f"    Sentiment: {sample['predicted_sentiment']}")
            
            logger.info("------------------------")
            
        except Exception as e:
            logger.error(f"❌ Batch Processing Error:")
            logger.error(f"    Error: {str(e)}")
            logger.error("------------------------")
            raise

    # 9. Write predictions to MongoDB
    logger.info("Starting MongoDB stream...")
    query = output_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    # 10. Write to console for monitoring
    logger.info("Starting console stream...")
    console_query = output_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # 11. Monitor active streams
    def monitor_streams():
        while True:
            try:
                logger.info("\n=== Streaming Status ===")
                logger.info(f"Active streams: {len(spark.streams.active)}")
                logger.info(f"MongoDB Stream - Active: {query.isActive}")
                logger.info(f"Console Stream - Active: {console_query.isActive}")
                
                if query.lastProgress:
                    logger.info(f"MongoDB Progress: {query.lastProgress}")
                if console_query.lastProgress:
                    logger.info(f"Console Progress: {console_query.lastProgress}")
                    
                time.sleep(10)
            except Exception as e:
                logger.error(f"Monitoring error: {str(e)}")

    # Start monitoring in background
    monitor_thread = Thread(target=monitor_streams, daemon=True)
    monitor_thread.start()

    # 12. Keep the application running
    logger.info("Waiting for stream termination...")
    spark.streams.awaitAnyTermination()

except Exception as e:
    logger.error(f"Application error: {str(e)}")
    raise

finally:
    logger.info("Shutting down application...")
    if 'spark' in locals():
        spark.stop()