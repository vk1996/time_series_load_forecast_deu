from datetime import datetime, timedelta, timezone
import pickle
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

__dir__ = os.path.dirname(os.path.abspath(__file__))
#sys.path.append(__dir__)
sys.path.insert(0, os.path.abspath(os.path.join(__dir__, '..')))

from utils import extract,round_off_timestamp,transform,load,feature_col_names


def load_model(model_path):
    with open(model_path, "rb") as f:
        model = pickle.load(f)
        print("Model loaded")
    return model

def run_extract(**kwargs):
    current_time = datetime.now(timezone.utc)
    start_time = round_off_timestamp(current_time - timedelta(minutes=360))
    end_time = round_off_timestamp(current_time - timedelta(minutes=90))
    return extract(start_time, end_time)

def run_transform(**kwargs):
    ti = kwargs['ti']
    extracted_df=ti.xcom_pull(task_ids='extract_task')
    print('Extracted df:',extracted_df)
    extracted_df = extracted_df.drop(extracted_df.index[-1])
    transformed_df=transform(extracted_df)
    print('Transform Success')
    #print('Transformed df:', transformed_df)
    return transformed_df


def run_predict(**kwargs):
    ti = kwargs['ti']
    transformed_df = ti.xcom_pull(task_ids='transform_task')
    feature_series = transformed_df.loc[len(transformed_df) - 1, feature_col_names]
    print('feature series:',feature_series)
    model=load_model("../models/DE_LU_load_prediction_xgboost_model.pkl")
    load_forecasted_value = int(model.predict([feature_series])[0])
    print('Predict Success')
    return load_forecasted_value

def run_load(**kwargs):
    ti = kwargs['ti']
    extracted_df = ti.xcom_pull(task_ids='extract_task')
    transformed_df = ti.xcom_pull(task_ids='transform_task')
    timestamp = transformed_df.loc[len(transformed_df) - 1, ['utc_timestamp']].iloc[0]
    print('timestamp used:',timestamp)
    timestamp = timestamp+timedelta(minutes=15)
    print('timestamp prdicted for:', timestamp)
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    load_actual_value = int(extracted_df.loc[len(extracted_df) - 1, ['DE_load_actual_entsoe_transparency']].iloc[0])
    load_forecasted_value = ti.xcom_pull(task_ids='predict_task')
    load(timestamp, load_forecasted_value, load_actual_value)
    print('Load Success')






with DAG(
    'load_forecast_de_dag_2',
    #default_args=default_args,
    start_date=datetime(2025, 3, 20),
    schedule_interval="1,16,31,46 * * * *",
    catchup=False,
    description='load_forecast_de_dag'
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=run_extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=run_transform
    )

    predict_task = PythonOperator(
        task_id='predict_task',
        python_callable=run_predict
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=run_load
    )

    #extract_task >> transform_task

    extract_task >> transform_task >> predict_task >> load_task
