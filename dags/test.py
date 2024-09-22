from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import datetime
import pandas as pd
import os

current_date = datetime.date.today()
dag = DAG('VK_Task', start_date=datetime.datetime(int(str(current_date)[:4]), int(str(current_date)[5:7]), int(str(current_date)[8:])), schedule_interval='0 7 * * *')



def perform_task2():
    df = pd.read_csv(f'./input/{current_date - datetime.timedelta(1)}.csv', names=['email', 'action', 'date'])
    df = df.drop(['date'], axis=1)

    for i in range(2, 8):
        day = current_date - datetime.timedelta(days=i)
        df1 = pd.read_csv(f'./input/{day}.csv', names=['email', 'action', 'date'])
        df1 = df1.drop(['date'], axis=1)
        df = pd.concat([df, df1])

    action_counts = df.groupby(['email', 'action']).size().unstack(fill_value=0).reset_index()
    action_counts.columns.name = None 
    action_counts = action_counts.rename(columns=lambda x: f'{x.lower()}_count' if x != 'email' and x != 'date' else x)

    file_path = os.path.join('output', f'{current_date}.csv')
    action_counts.to_csv(file_path, index=False)


task1 = BashOperator(
    task_id='task1',
    bash_command=f'python /opt/airflow/script/generate_data.py /opt/airflow/input {current_date - datetime.timedelta(7)} 30 10 2000',
    dag=dag
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=perform_task2,
    dag=dag
)

task1 >> task2
