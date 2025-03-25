from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, expr

spark = SparkSession.builder.appName("Top Verified Users by Reach").getOrCreate()

# Load data
posts = spark.read.csv("input/posts.csv", header=True, inferSchema=True)
users = spark.read.csv("input/users.csv", header=True, inferSchema=True)

# Join and filter for verified users
joined = posts.join(users, on="UserID", how="inner").filter(col("Verified") == True)

# Calculate reach
reach_df = joined.withColumn("Reach", expr("Likes + Retweets"))

# Group by Username and get top 5
top_verified = reach_df.groupBy("Username").agg(sum_("Reach").alias("Total_Reach")) \
                       .orderBy(col("Total_Reach").desc()) \
                       .limit(5)

# Save results
top_verified.coalesce(1).write.csv("outputs/task4_top_verified_users.csv", header=True, mode="overwrite")
spark.stop()
