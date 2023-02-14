import sys
sys.path.append("/mnt/e/alura/alura_airflow_Twitter/airflow")

from datetime import datetime, timedelta
from airflow import DAG
from os.path import join
from operators.twitter_operator import TwitterOperator
from pathlib import Path
from airflow.utils.dates import days_ago




with DAG(dag_id = "TwitterDAG", start_date=days_ago(2),schedule_interval="@daily") as dag:
        # end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
        # start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)   
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
        query = "datascience"
        to = TwitterOperator(file_path =join("data_lake/twitter_extract_dataScience",
                            "extract_date={{ ds }}",
                            "datascience_{{ ds_nodash }}.json") ,
                            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                            query=query,
                            task_id= "twitter_datascience_extract")
        