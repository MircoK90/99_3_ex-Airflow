from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from sklearn.linear_model    import LinearRegression
from sklearn.tree            import DecisionTreeRegressor
from sklearn.ensemble        import RandomForestRegressor
import sys, os

RAW_DIR = "/app/raw_files"
CLEAN_DIR = "/app/clean_data" # Model and clean.csv in one Folder togehter

# Add project root to path so Airflow can find the modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from a_fetch_weather import fetch_weather
from bc_transform_data import transform_data_into_csv
from d_prepare_data import prepare_data
from e_train import train_and_save_model, evaluate_model


# Task 2 and 3:
def task_transform_data(**context):
    transform_data_into_csv(n_files=20, filename="data.csv",       # for evolution ogf the Dashboard
                                    raw_dir = RAW_DIR,
                                    clean_dir = CLEAN_DIR)



def task_transform_data_full(**context):
    transform_data_into_csv(n_files=None, filename="fulldata_container.csv",
                                    raw_dir = RAW_DIR,
                                    clean_dir = CLEAN_DIR)
    
# Task 4abc:
def task_train_lr(**context):
    # Create Dataset to trian on
    X, y = prepare_data(path=os.path.join(CLEAN_DIR, 'fulldata_container.csv'))
    # train the model
    score = evaluate_model(LinearRegression(), X, y)    # precalculation without saving it, to see at the end which is best, then its gets saved
    # Save score for eval in Task 5
    context['ti'].xcom_push(key='score_lr', value=score)
    print(f"LinearRegression score: {score:.4f}")   

def task_train_dt(**context):
    X, y = prepare_data(os.path.join(CLEAN_DIR, 'fulldata_container.csv'))
    score = evaluate_model(DecisionTreeRegressor(), X, y)
    context['ti'].xcom_push(key='score_dt', value=score)
    print(f"DT score: {score:.4f}")

def task_train_rf(**context):
    X, y = prepare_data(os.path.join(CLEAN_DIR, 'fulldata_container.csv'))
    score = evaluate_model(RandomForestRegressor(), X, y)
    context['ti'].xcom_push(key='score_rf', value=score)
    print(f"RF score: {score:.4f}")

def task_evaluate_models(**context):
    # pull scores from previous tasks
    ti = context['ti']

    # dict section
    scores = {
        "LinearRegression":         ti.xcom_pull(task_ids='task_train_lr', key='score_lr'),         # xcom_pull not only pull!
        "DecisionTreeRegressor":    ti.xcom_pull(task_ids='task_train_dt', key='score_dt'),
        "RandomForestRegressor":    ti.xcom_pull(task_ids='task_train_rf', key='score_rf'),
    }

    models = {
        "LinearRegression": LinearRegression(),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(),
    }


    best_model = max(scores, key=lambda k: scores[k])
    print(f"Best model: {best_model} with score {scores[best_model]:.4f}")
    X, y = prepare_data(os.path.join(CLEAN_DIR, 'fulldata_container.csv'))
    train_and_save_model(
        models[best_model], X, y,
        os.path.join(CLEAN_DIR, 'best_model.joblib'))



with DAG(
    dag_id="03_exam_AirFlow",
    default_args={
        "owner" : "AirFlow",
        "start_date": datetime(2024, 1, 1),
    },
    schedule_interval = timedelta(minutes=1),
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
    )

    t4a = PythonOperator(
        task_id="task_train_lr",
        python_callable = task_train_lr,
    )

    t4b = PythonOperator(
        task_id="task_train_dt",
        python_callable = task_train_dt,
    )

    t4c = PythonOperator(
        task_id="task_train_rf",
        python_callable = task_train_rf
    )
    t5 = PythonOperator(
        task_id="task_evaluate_models",
        python_callable = task_evaluate_models
    )


    t1 >> [t2, t3]
    t3 >> [t4a, t4b, t4c]
    [t4a, t4b, t4c] >> t5