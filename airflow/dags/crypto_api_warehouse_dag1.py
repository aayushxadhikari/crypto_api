from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract(**context):
    # mock extraction for now
    return [{"symbol": "BTC"}, {"symbol": "ETH"}]


def land(**context):
    # placeholder for landing raw data
    # you can pull XCom from extract using context['ti'].xcom_pull()
    pass


def normalize(**context):
    # placeholder for transform/normalize
    pass


def scd_and_load(**context):
    # placeholder for SCD + load into warehouse
    pass


with DAG(
    dag_id="crypto_api_warehouse",
    default_args=default_args,
    schedule_interval="0 * * * *",  # every hour at minute 0
    start_date=datetime(2025, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "warehouse"],
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="land", python_callable=land)
    t3 = PythonOperator(task_id="normalize", python_callable=normalize)
    t4 = PythonOperator(task_id="scd_and_load", python_callable=scd_and_load)

    t1 >> t2 >> t3 >> t4
