import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from COVID_download_from_source_operator import COVID_DownloadFromSourceOperator

URLS_COVID = {
   'total_cases': 'https://github.com/owid/covid-19-data/blob/master/public/data/cases_deaths/total_cases.csv',
   'excess_mortality': 'https://github.com/owid/covid-19-data/blob/master/public/data/excess_mortality/excess_mortality.csv',
   'covid_hospitalizations': 'https://github.com/owid/covid-19-data/blob/master/public/data/hospitalizations/covid-hospitalizations.csv',
   'total_deaths': 'https://github.com/owid/covid-19-data/blob/master/public/data/jhu/total_deaths.csv',
   'covid_testing': 'https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-all-observations.csv',
   'covid-vaccinations': 'https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/vaccinations.csv',
}


dag1 =  DAG(dag_id=f"ingest_total_cases_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = COVID_DownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_COVID['total_cases'])}", url=URLS_COVID['total_cases'],dag=dag1
)

# TASKS
download_task


dag2 =  DAG(dag_id=f"ingest_excess_mortality_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = COVID_DownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_COVID['excess_mortality'])}", url=URLS_COVID['excess_mortality'],dag=dag2
)

# TASKS
download_task


dag3 =  DAG(dag_id=f"ingest_covid_hospitalizations_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = COVID_DownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_COVID['covid_hospitalizations'])}", url=URLS_COVID['covid_hospitalizations'],dag=dag3
)

# TASKS
download_task

dag4 =  DAG(dag_id=f"ingest_total_deaths_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = COVID_DownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_COVID['total_deaths'])}", url=URLS_COVID['total_deaths'],dag=dag4
)

# TASKS
download_task

dag5 =  DAG(dag_id=f"ingest_covid_testing_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = COVID_DownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_COVID['covid_testing'])}", url=URLS_COVID['covid_testing'],dag=dag5
)

# TASKS
download_task

dag6 =  DAG(dag_id=f"ingest_covid-vaccinations_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = COVID_DownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_COVID['covid-vaccinations'])}", url=URLS_COVID['covid-vaccinations'],dag=dag6
)

# TASKS
download_task
