# Spark SQL & DataFrames: Social Media Sentiment & Engagement Analysis

In this project, we analyze sentiment trends and audience engagement on a fictional social media platform using **Apache Spark SQL** and **DataFrame APIs**. The goal is to understand hashtag popularity, user behavior by age, the impact of sentiment on engagement, and the reach of verified users.

---

## 🎯 Objectives

- **Hashtag Trends:** Analyze frequency of hashtags across all posts.
- **Engagement by Age Group:** Study average likes and retweets segmented by user age group.
- **Sentiment vs Engagement:** Observe how post sentiment affects user interaction.
- **Top Verified Users:** Identify the most influential verified users by total engagement.

---

## 🛠️ Setup and Execution

### 1. Install Requirements

Ensure you have Python and Spark set up. Install PySpark if needed:

```bash
pip install pyspark
```

### 2. Generate Input Data

Use the provided script to generate input files:

```bash
python input_generater.py
```

This will create:
- `input/posts.csv`
- `input/users.csv`

---

## 📁 Input Dataset Description

### posts.csv

Contains social media post activity.

| Column Name     | Data Type | Description                                 |
|------------------|-----------|---------------------------------------------|
| PostID           | Integer   | Unique post ID                              |
| UserID           | Integer   | ID of the user who posted                   |
| Content          | String    | Text content of the post                    |
| Timestamp        | String    | Timestamp of the post                       |
| Likes            | Integer   | Number of likes                             |
| Retweets         | Integer   | Number of retweets                          |
| Hashtags         | String    | Comma-separated hashtags                    |
| SentimentScore   | Float     | Sentiment score from -1 (neg) to 1 (pos)    |

### users.csv

Contains user profile information.

| Column Name | Data Type | Description                    |
|-------------|-----------|--------------------------------|
| UserID      | Integer   | Unique user ID                 |
| Username    | String    | User handle                    |
| AgeGroup    | String    | Teen / Adult / Senior          |
| Country     | String    | Country of residence           |
| Verified    | Boolean   | Whether the account is verified|

---

## 🚀 How to Execute Spark Jobs

All scripts are located in the `src/` folder.

Run each job using the following commands:

```bash
spark-submit src/task1_hashtag_trends.py
spark-submit src/task2_engagement_by_age.py
spark-submit src/task3_sentiment_vs_engagement.py
spark-submit src/task4_top_verified_users.py
```

---

## 📊 Task Breakdown

### ✅ Task 1: Hashtag Trends

**Goal:** Find the top 10 most-used hashtags.

#### Logic:
- Split hashtags per post using `explode()`.
- Normalize using `trim()` and `lower()`.
- Group and count hashtags.

#### Output:
```bash
outputs/task1_top_hashtags.csv
```

---

### ✅ Task 2: Engagement by Age Group

**Goal:** Analyze average likes and retweets by user age category.

#### Logic:
- Join `posts.csv` with `users.csv` on `UserID`.
- Group by `AgeGroup`.
- Compute average likes and retweets.

#### Output:
```bash
outputs/task2_engagement_by_age.csv
```

---

### ✅ Task 3: Sentiment vs Engagement

**Goal:** Categorize sentiment and correlate with likes and retweets.

#### Logic:
- Use `SentimentScore` to assign label: Positive, Neutral, or Negative.
- Group by sentiment.
- Aggregate average likes and retweets.

#### Output:
```bash
outputs/task3_sentiment_vs_engagement.csv
```

---

### ✅ Task 4: Top Verified Users by Reach

**Goal:** Rank verified users by total reach (likes + retweets).

#### Logic:
- Filter `Verified == True`.
- Calculate `Reach = Likes + Retweets`.
- Group by `Username` and get top 5.

#### Output:
```bash
outputs/task4_top_verified_users.csv
```

---

## 📦 Project Structure

```
project_root/
├── input/
│   ├── posts.csv
│   └── users.csv
├── outputs/
│   ├── task1_top_hashtags.csv
│   ├── task2_engagement_by_age.csv
│   ├── task3_sentiment_vs_engagement.csv
│   └── task4_top_verified_users.csv
├── src/
│   ├── task1_hashtag_trends.py
│   ├── task2_engagement_by_age.py
│   ├── task3_sentiment_vs_engagement.py
│   └── task4_top_verified_users.py
└── README.md
```

---

## ✅ Observations

- Positive sentiment posts tend to get more engagement.
- Teens are the most active users in terms of likes and retweets.
- Certain hashtags dominate conversations, especially those related to tech and design.
- Verified users significantly boost visibility via reach.

---

## 👨‍💻 Author

Narayanadatta Jupalli  
