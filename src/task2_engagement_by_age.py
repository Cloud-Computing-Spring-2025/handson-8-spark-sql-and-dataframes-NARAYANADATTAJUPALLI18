from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round

spark = SparkSession.builder.appName("Engagement by Age").getOrCreate()

# Load data
posts = spark.read.csv("input/posts.csv", header=True, inferSchema=True)
users = spark.read.csv("input/users.csv", header=True, inferSchema=True)

# Join on UserID
joined = posts.join(users, on="UserID", how="inner")

# Group by AgeGroup and calculate average engagement
engagement = joined.groupBy("AgeGroup").agg(
    round(avg("Likes"),1).alias("Avg_Likes"),
    round(avg("Retweets"),1).alias("Avg_Retweets")
).orderBy(col("Avg_Likes").desc())

# Save results
engagement.coalesce(1).write.csv("outputs/task2_engagement_by_age.csv", header=True, mode="overwrite")
spark.stop()
