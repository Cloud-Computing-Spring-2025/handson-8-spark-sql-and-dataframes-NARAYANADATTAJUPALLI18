from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim, lower, col, count

spark = SparkSession.builder.appName("Hashtag Trends").getOrCreate()

# Load data
df = spark.read.csv("input/posts.csv", header=True, inferSchema=True)

# Explode hashtags into individual rows
hashtags = df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))
hashtags = hashtags.withColumn("Hashtag", trim(lower(col("Hashtag"))))

# Count and sort hashtags
top_hashtags = hashtags.groupBy("Hashtag").agg(count("*").alias("Count")) \
                       .orderBy(col("Count").desc()) \
                       .limit(10)

# Save results
top_hashtags.coalesce(1).write.csv("outputs/task1_top_hashtags.csv", header=True, mode="overwrite")
spark.stop()
