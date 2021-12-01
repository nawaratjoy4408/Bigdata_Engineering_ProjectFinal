import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests


def get_datacovid19_report_today():
    url = 'https://covid19.ddc.moph.go.th/api/Cases/today-cases-by-provinces'
    response = requests.get(url)
    dataset = response.json()
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_datacovid19_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = get_datacovid19_report_today()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='52.45.246.88',user='db_user',passwd='db_user_pass',db='app_db')
        
        cursor = db.cursor()
        
        txn_date = data['txn_date'].replace('\n',' ')
        province = data['province'].replace('\n',' ')
        new_case = data['new_case']
        total_case = data['total_case']
        new_case_excludeabroad = data['new_case_excludeabroad']
        total_case_excludeabroad = data['total_case_excludeabroad']
        new_death = data['new_death']
        total_death = data['total_death']
        #update_date = data['update_date']
        cursor.execute('INSERT INTO covid19_reports (txn_date, province, new_case , total_case , new_case_excludeabroad, total_case_excludeabroad, new_death, total_death)'
                  'VALUES("%s", "%s", "%s","%s", "%s", "%s", "%s", "%s")',
                   (txn_date, province, new_case, total_case , new_case_excludeabroad , total_case_excludeabroad , new_death,total_death))
  
        db.commit()
        print("Record inserted successfully into covid19_reports table")
        cursor.close()
                                       
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    
}
with DAG('covid19_data_pipelineThailand',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for COVID-19 report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_datacovid19_report_today',
        python_callable= get_datacovid19_report_today
    )

    t2 = PythonOperator(
        task_id='save_datacovid19_into_db',
        python_callable= save_datacovid19_into_db
    )

    t1 >> t2

