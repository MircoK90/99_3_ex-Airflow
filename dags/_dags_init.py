from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests, json, os


import sys
import os

RAW_DIR = "/app/raw_files"
CLEAN_DIR = "/app/clean_data"


# Add project root to path so Airflow can find the modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from a_fetch_weather import fetch_weather
from bc_transform_data import transform_data_into_csv
from d_prepare_data import prepare_data
from e_train import train_and_save_model, evaluate_model


# Task 2 and 3:
def task_transform_data(**context):
    task_transform_data_into_csv(n=20, filename="fulldata_container.csv",
                                    raw_dir = RAW_DIR,
                                    clean_dir = CLEAN_DIR)



def task_transform_data_full(**context):
    task_transform_data_into_csv(n=None, filename="fulldata_container.csv",
                                    raw_dir = RAW_DIR,
                                    clean_dir = CLEAN_DIR)
    
# Task 4abc:
def task_train_lr(**context):
    X, y = prepare_data(path=os.path.join(CLEAN_DIR, 'fulldata_container.csv'))
    score = train_and_save_model("LinearRegression", X, y)
    context['ti'].xcom_push(key='score_lr', value=score)
    print(f"LinearRegression score: {score:.4f}")   

def task_train_dt(**context):
    X, y = prepare_data(os.path.join(CLEAN_DIR, 'fulldata.csv'))
    score = evaluate_model("DecisionTreeRegressor", X, y)
    context['ti'].xcom_push(key='score_dt', value=score)
    print(f"DT score: {score:.4f}")

def task_train_rf(**context):
    X, y = prepare_data(os.path.join(CLEAN_DIR, 'fulldata.csv'))
    score = evaluate_model("RandomForestRegressor", X, y)
    context['ti'].xcom_push(key='score_rf', value=score)
    print(f"RF score: {score:.4f}")


with DAG(
    dag_id="03_exam_AirFlow",
    default_args={
        "owner" : "AirFlow",
        "start_date": datetime(2024, 1, 1),
    },
    schedule_interval = timedelta(minutes=2),
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    t2 = PythonOperator(
        task_id="transform_data_into_csv",
        python_callable = task_transform_data
    )
    t3 = PythonOperator(
        task_id="prepare_data",
        python_callable = task_transform_data_full,


    t2a = PythonOperator(
        task_id="transform_data_full",
        python_callable = task_train_lr
    )
    t3 = PythonOperator(
        task_id="prepare_data",
        python_callable = task_train_dt
    )
    t4a = PythonOperator(
        task_id="train_LinearRegression",
        python_callable = train_and_save_model(
        )

    t4b = PythonOperator(
        task_id="train_and_save_model",
        python_callable = train_and_save_model(
            "DecisionTreeRegressor",
            X, y,
            os.path.join(MODEL_DIR, 'best_model.joblib')
        )
    )
    t4c = PythonOperator(