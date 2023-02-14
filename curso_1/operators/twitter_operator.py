import sys
sys.path.append("/mnt/e/alura/alura_airflow_Twitter/airflow")

from datetime import datetime, timedelta
import json
from airflow.models import DAG, BaseOperator, TaskInstance
#from hook.twitter_hook import TwitterHook
from hook.twitter_hook import TwitterHook
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):
    template_fields = ["query", "file_path", "start_time", "end_time"]
    def __init__(self,file_path ,end_time,start_time,query,**kwargs):
        self.file_path = file_path
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True,exist_ok=True)
    
    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
        self.create_parent_folder()
        with open(self.file_path,"w") as outputfile:
            for pg in TwitterHook(end_time, start_time, query).run():
                json.dump(pg,outputfile, ensure_ascii=False)
                outputfile.write("\n")

if __name__=="__main__":
    #montando url
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)   
    query = "datascience"

    with DAG(dag_id = "TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(file_path =join("data_lake/twitter_extract_dataScience",
                            f"extract_date={datetime.now().date()}",
                            f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json") ,
                            end_time=end_time,start_time=start_time,query=query,task_id= "task_run_test")
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)