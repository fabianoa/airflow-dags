from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from datetime import datetime, timedelta

DAG_NAME = 'my_kubernetes_dag'
default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

pod_arguments = {
    'image': 'python:3.8',
    'cmds': ['python', '-c', 'print("Hello, world!")'],
    'name': 'my-pod'
}

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

task = KubernetesPodOperator(
    dag=dag,
    task_id='my_kubernetes_task',
    name='my-kubernetes-task',
    namespace='airflow-bigdata',
    image=pod_arguments['image'],
    cmds=pod_arguments['cmds'],
    labels={'mylabel': 'myvalue'},
    volumes=[],
    volume_mounts=[],
    env_vars={},
    is_delete_operator_pod=True,
    hostnetwork=False
)

task
