from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from os.path import join
import argparse

def get_tweet_data(df):
    """get_tweet_data, metodo responsavel por  criar o dataset tweet , fazendo limpeza das estruturas
        e organizando a coluna

    Args:
        df (Dataframe): dados brutos retirados da requisão do twitter 

    Returns:
        Dataframe: dados limpos e estruturados
    """
    tweet_df = df.select(f.explode("data").alias("tweets"))\
                .select("tweets.author_id", "tweets.conversation_id",
                        "tweets.created_at", "tweets.id",
                        "tweets.public_metrics.*", "tweets.text"
                        )
    return tweet_df

def get_users_data(df):
    """get_users_data 
     metodo responsavel por  criar o dataset USERS , fazendo limpeza das estruturas e organizando a coluna

    Args:
        df (dataframe): dados brutos retirados da requisão do twitter

    Returns:
        _type_: dados limpos e estruturados
    """
    user_df = df.select(f.explode("includes.users").alias("users")).select("users.*")

    return user_df

def export_json(df, dest_path):
    """export_json metodo responsavel por realizar o armazenamento dos dados tratados, após o tramento salvamos o dados no formato json.

    Args:
        df (dataframe): dados limpos e estruturados
        dest_path (str): caminho que onde os dados serão salvos
    """
    df.coalesce(1).write.mode("overwrite").json(dest_path)


def twitter_transformation(data_src,spark,data_dest,process_date):
    df = spark.read.json(data_src)

    tweet_df = get_tweet_data(df)
    user_df = get_users_data(df)

    table_dest = join(data_dest,"{table_name}",f"process_date={process_date}")
    export_json(tweet_df,table_dest.format(table_name="tweets"))
    export_json(user_df,table_dest.format(table_name="users"))

if __name__=="__main__":


    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)

    args = parser.parse_args()
    spark = SparkSession\
            .builder\
            .appName("twitter_transformation")\
            .config(
                    "spark.driver.extraJavaOptions",
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
                )\
            .getOrCreate()
    
    twitter_transformation(args.src,spark, args.dest, args.process_date)