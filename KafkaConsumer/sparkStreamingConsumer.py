from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType
import os
import logging
from threading import Thread
import time

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
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
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

    def analyze_sentiment(text, rating):
        # Classify based on the rating (overall) according to Amazon's rules
        if rating > 3.0:
            return 1  # Positive
        elif rating < 3.0:
            return 0  # Negative
        else:
            # For neutral ratings (overall = 3), use text analysis as fallback
            positive_words = {'good', 'great', 'nice', 'excellent', 'love', 'perfect', 'best', 
                            'recommend', 'worth', 'quality', 'satisfied', 'happy', 'helpful'}
            negative_words = {'bad', 'poor', 'terrible', 'worst', 'hate', 'awful', 'disappointing',
                            'waste', 'defective', 'broken', 'cheap', 'return', 'refund'}
            
            text = text.lower()
            pos_count = sum(1 for word in positive_words if word in text)
            neg_count = sum(1 for word in negative_words if word in text)
            
            # If equal counts or no sentiment words found, return neutral (2)
            if pos_count == neg_count:
                return 2  # Neutral
            return 1 if pos_count > neg_count else 0

    # Register UDF with three possible outcomes (0: Negative, 1: Positive, 2: Neutral)
    sentiment_udf = udf(analyze_sentiment, IntegerType())
    spark.udf.register("analyze_sentiment", sentiment_udf)

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
    predictions = parsed_df.withColumn(
        "predicted_sentiment",
        sentiment_udf(col("reviewText"), col("overall"))
    )
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
                # Write to MongoDB
                df.write \
                    .format("mongodb") \
                    .mode("append") \
                    .option("database", "sentiment") \
                    .option("collection", "predictions") \
                    .save()
                
                sample = df.limit(1).collect()[0]
                logger.info("✅ Sample Prediction:")
                logger.info(f"    Review ID: {sample['reviewerID']}")
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
        .outputMode("update") \
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