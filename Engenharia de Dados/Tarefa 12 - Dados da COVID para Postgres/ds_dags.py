import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from ds_convert_to_parquet_operator import ConvertToParquetOperator

URLS_COVID = {
   'total_cases': 'total_cases.csv',
   'excess_mortality': 'excess_mortality.csv',
   'covid_hospitalizations': 'covid-hospitalizations.csv',
   'total_deaths': 'total_deaths.csv',
   'covid_testing': 'covid-testing-all-observations.csv',
   'covid_vaccinations': 'vaccinations.csv',
}



dag1 =  DAG(dag_id=f"ds_total_cases_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"download_{URLS_COVID['total_cases']}", url=URLS_COVID['total_cases'],dag=dag1
)

# TASKS
download_task


dag2 =  DAG(dag_id=f"ds_excess_mortality_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"download_{URLS_COVID['excess_mortality']}", url=URLS_COVID['excess_mortality'],dag=dag2
)

# TASKS
download_task


dag3 =  DAG(dag_id=f"ds_covid_hospitalizations_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"download_{URLS_COVID['covid_hospitalizations']}", url=URLS_COVID['covid_hospitalizations'],dag=dag3
)

# TASKS
download_task

dag4 =  DAG(dag_id=f"ds_total_deaths_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"download_{URLS_COVID['total_deaths']}", url=URLS_COVID['total_deaths'],dag=dag4
)

# TASKS
download_task

dag5 =  DAG(dag_id=f"ds_covid_testing_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"download_{URLS_COVID['covid_testing']}", url=URLS_COVID['covid_testing'],dag=dag5
)

# TASKS
download_task

dag6 =  DAG(dag_id=f"ds_covid_vaccinations_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"download_{URLS_COVID['covid_vaccinations']}", url=URLS_COVID['covid_vaccinations'],dag=dag6
)

# TASKS
download_task

