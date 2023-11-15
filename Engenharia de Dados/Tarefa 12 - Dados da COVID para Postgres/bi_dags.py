import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bi_pg_operator import BIPgOperator


URLS_COVID = {
   'total_cases': 'total_cases.csv',
   'excess_mortality': 'excess_mortality.csv',
   'covid_hospitalizations': 'covid-hospitalizations.csv',
   'total_deaths': 'total_deaths.csv',
   'covid_testing': 'covid-testing-all-observations.csv',
   'covid_vaccinations': 'vaccinations.csv',
}


dag1 =  DAG(dag_id=f"bi_total_cases_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = BIPgOperator(
   task_id=f"download_{URLS_COVID['total_cases']}", url=URLS_COVID['total_cases'], tablename='total_cases', dag=dag1
)

# TASKS
download_task


dag2 =  DAG(dag_id=f"bi_excess_mortality_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = BIPgOperator(
   task_id=f"download_{URLS_COVID['excess_mortality']}", url=URLS_COVID['excess_mortality'], tablename='excess_mortality',dag=dag2
)

# TASKS
download_task


dag3 =  DAG(dag_id=f"bi_covid_hospitalizations_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = BIPgOperator(
   task_id=f"download_{URLS_COVID['covid_hospitalizations']}", url=URLS_COVID['covid_hospitalizations'], tablename='covid_hospitalizations',dag=dag3
)

# TASKS
download_task

dag4 =  DAG(dag_id=f"bi_total_deaths_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = BIPgOperator(
   task_id=f"download_{URLS_COVID['total_deaths']}", url=URLS_COVID['total_deaths'], tablename='total_deaths',dag=dag4
)

# TASKS
download_task

dag5 =  DAG(dag_id=f"bi_covid_testing_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = BIPgOperator(
   task_id=f"download_{URLS_COVID['covid_testing']}", url=URLS_COVID['covid_testing'], tablename='covid_testing',dag=dag5
)

# TASKS
download_task

dag6 =  DAG(dag_id=f"bi_covid_vaccinations_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = BIPgOperator(
   task_id=f"download_{URLS_COVID['covid_vaccinations']}", url=URLS_COVID['covid_vaccinations'], tablename='covid_vaccinations',dag=dag6
)

# TASKS
download_task
