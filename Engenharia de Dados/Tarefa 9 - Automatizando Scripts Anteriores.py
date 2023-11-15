# Tarefa Aula 9 Engenharia de Dados Awari
# Rogerio Veiga Andrade -147/11/2023
# Com base no que foi na aula e com base nas tarefas das aulas anteriores 
# (tratamento de arquivos de Municipios e Estados), a atividade consiste em adaptar os 
# scripts criados para as tarefas anteriores para que sejam executados dentro do Airflow.

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pyspark
import boto3
import pandas as pd
from io import StringIO 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

def municipios_estados():

    # Criando o objeto client
    client = boto3.client('s3', 
        endpoint_url='http://localhost:9001/',
        aws_access_key_id='6DdjRBILhyMi2G2i',
        aws_secret_access_key='9LSWJZT7f0i8FgjzflQQYrOq9tvUQrD1',
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        verify=False,
        region_name='sa-east-1'
    )

    # Lendo o arquivo de novos estados
    estados = pd.read_csv('arquivos/streaming/estados.csv')

    # Criando as novas pastas de acordo com os estados do arquivo
    for uf in estados['uf'].unique():
        client.put_object(Bucket='aula-06', Key=f'{uf}/')
    
    # Lendo a lista de municipios
    municipios = pd.read_csv('arquivos/streaming/municipios.csv')

    # Inner join na tabela de municipios com a original de estaod
    estados = pd.read_json('arquivos/estados.json')
    municipios_estados = municipios.merge(estados, on='codigo_uf', how='inner')

    # Para cada estado presente na lista de municipios, o código lê o arquivo de cidades do estado
    # correspondente, adiciona os municipios no DataFrame, converte pra csv e devolve pro MinIO
    for uf in municipios_estados['uf'].unique():
        df = pd.read_csv(client.get_object(Bucket='aula-06', Key=f'{uf}/cidades.csv')['Body'])
        df = pd.concat([ df, municipios_estados[municipios_estados['uf'] == uf] ])
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        client.put_object(Body=csv_buffer.getvalue(), Bucket='aula-06', Key=f'{uf}/cidades.csv')

with DAG( dag_id = 'municipios_estados',
        start_date = datetime(2021, 1, 1),
        schedule_interval = '* * * * *'
        ) as dag:
    
    task1 = PythonOperator(task_id = 'Primeiro_No',
            python_callable=municipios_estados)

task1