"""DAG : verification HDFS -> batch -> entrainement -> rapport."""
import json, urllib.request
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def check_hdfs():
    """Verifie via WebHDFS que des donnees existent dans /lake/transactions."""
    url = "http://namenode:9870/webhdfs/v1/lake/transactions?op=LISTSTATUS"
    with urllib.request.urlopen(url, timeout=20) as r:
        items = json.load(r)["FileStatuses"]["FileStatus"]
    if not items:
        raise ValueError("HDFS /lake/transactions est vide : lance le generateur et attends le streaming.")
    print(f"{len(items)} entrees dans /lake/transactions")


with DAG(
    dag_id="fraud_retrain",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["fraud", "mlops"],
) as dag:
    check_data = PythonOperator(task_id="check_hdfs", python_callable=check_hdfs)
    batch = BashOperator(task_id="batch_job", bash_command="python /opt/project/batch/batch_job.py")
    train = BashOperator(task_id="train_and_log", bash_command="python /opt/project/spark/train.py")
    report = BashOperator(task_id="make_report", bash_command="python /opt/project/report/make_report.py")
    check_data >> batch >> train >> report
