import sys
sys.path.append("/mnt/e/alura/alura_airflow_Twitter/airflow")
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from os.path import join
from operators.twitter_operator import TwitterOperator
from pathlib import Path
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator




with DAG(dag_id = "TwitterDAG", start_date=days_ago(3),schedule_interval="@daily") as dag:
        # end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
        # start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)   
        BASE_FOLDER = join(
                        str(Path("/mnt/e").expanduser()),
                        "alura/alura_airflow_Twitter/datalake/{stage}/twitter_extract_dataScience/{partition}",
                        )
        PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"

        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
        query = "datascience"
        twitter_extraction = TwitterOperator(file_path =join(BASE_FOLDER.format(stage="Bronze",partition=PARTITION_FOLDER_EXTRACT),
                            "datascience_{{ ds_nodash }}.json") ,
                            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                            query=query,
                            task_id= "twitter_datascience_extract")
        
        twitter_transform = SparkSubmitOperator(
                                task_id="transform_twitter_datascience", 
                                application="/mnt/e/alura/alura_airflow_Twitter/src/sparks/transformation.py",
                                name="twitter_transformation",
                                application_args=["--src",BASE_FOLDER.format(stage="Bronze",partition=PARTITION_FOLDER_EXTRACT),
                                "--dest",BASE_FOLDER.format(stage="Silver",partition=""),
                                "--process-date", "{{ ds }}"]
                                )
        
        twitter_insight = SparkSubmitOperator(task_id="insight_twitter",
                                            application="/mnt/e/alura/alura_airflow_Twitter/src/sparks/insight_tweet.py",
                                            name="insight_twitter",
                                            application_args=["--src", BASE_FOLDER.format(stage="Silver", partition=""),
                                             "--dest", BASE_FOLDER.format(stage="Gold", partition=""),
                                             "--process-date", "{{ ds }}"])

        
twitter_extraction >> twitter_transform >> twitter_insight