import requests
import os
import pandas as pd
from io import StringIO 

from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from custom_s3_hook import CustomS3Hook

class COVID_DownloadFromSourceOperator(BaseOperator):
    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.custom_s3 = CustomS3Hook(bucket="COVID")
        self.current_time = datetime.now()
        self.current_date = self.current_time .strftime("%Y-%m-%d")

    def execute(self, context):
        self.download_file()
        return self.url

    def download_file(self):
        print("Fazendo download do arquivo: " + self.url)
        r = requests.get(self.url, allow_redirects=True)
        # open(f"/opt/airflow/downloads{os.path.basename(self.url)}", 'wb').write(r.content)
        self.custom_s3.put_object(key=f"downloaded/{self.current_date}/{os.path.basename(self.url)}",buffer=r.content)

        open(f"/opt/airflow/downloads/{os.path.basename(self.url)}", 'wb').write(r.content)
        
        tar_path = f"/opt/airflow/downloads/{os.path.basename(self.url)}"
        chunksize = 10 ** 6
        df = pd.read_csv(tar_path, compression='gzip', header=0, sep="\t", quotechar='"', nrows=2000000)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, quotechar='"')
        self.custom_s3.put_object(key=f"datalake/{os.path.basename(self.url)}", buffer=csv_buffer.getvalue())

        if os.path.isfile(tar_path):
            os.remove(tar_path)