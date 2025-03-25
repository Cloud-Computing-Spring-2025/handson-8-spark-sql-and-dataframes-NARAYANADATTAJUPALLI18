from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col,round

spark = SparkSession.builder.appName("Sentiment vs Engagement").getOrCreate()

# Load data
df = spark.read.csv("input/posts.csv", header=True, inferSchema=True)

# Categorize sentiment
df = df.withColumn("Sentiment",
    when(col("SentimentScore") > 0.2, "Positive")
    .when(col("SentimentScore") < -0.2, "Negative")
    .otherwise("Neutral")
)

# Aggregate engagement
sentiment_stats = df.groupBy("Sentiment").agg(
    round(avg("Likes"),1).alias("Avg_Likes"),
    round(avg("Retweets"),1).alias("Avg_Retweets")
).orderBy("Sentiment")

# Save results
sentiment_stats.coalesce(1).write.csv("outputs/task3_sentiment_vs_engagement.csv", header=True, mode="overwrite")
spark.stop()
