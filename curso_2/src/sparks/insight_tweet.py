from pyspark.sql import functions as f
from os.path import join
import argparse
from pyspark.sql import SparkSession

def get_tweet_conversas(df_tweet):
    return df_tweet.alias("tweet")\
                    .groupBy(f.to_date("created_at").alias("created_at"))\
                    .agg(
                            f.countDistinct("author_id").alias("n_tweets"),
                            f.sum("like_count").alias("n_likes"),
                            f.sum("quote_count").alias("n_quote_count"),
                            f.sum("reply_count").alias("n_reply_count"),
                            f.sum("retweet_count").alias("n_retweet_count"),
                    ).withColumn("weekday", f.date_format("created_at","E"))
     

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)


def twitter_insight(spark, src, dest, process_date):

    df_tweet = spark.read.json(join(src, 'tweets'))

    tweet_conversas = get_tweet_conversas(df_tweet)

    export_json(tweet_conversas, join(dest, f"process_date={process_date}"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation Silver and Gold"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_insight(spark, args.src, args.dest, args.process_date)
