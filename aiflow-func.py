from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow_fun',
    default_args=default_args,
    description='Show complex DAG in Airflow UI',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
)

complex_dag_fun_1 = BashOperator(
    task_id='starter',
    bash_command='for i in {1..10}; do sleep 1 && echo "slept $i" E; done',
    dag=dag,
)

complex_dag_fun_2 = BashOperator(
    task_id='leaf_node1',
    bash_command='for i in {1..5}; do sleep 1 && echo "slept $i" E; done',
    dag=dag,
)

complex_dag_fun_3 = BashOperator(
    task_id='leaf_node_2',
    bash_command='for i in {1..10}; do sleep 1 && echo "slept $i" E; done',
    dag=dag,
)

complex_dag_fun_4 = BashOperator(
    task_id='bottleneck_node',
    bash_command='for i in {1..1}; do sleep 1 && echo "slept $i" E; done',
    dag=dag,
)

complex_dag_fun_5 = BashOperator(
    task_id='terminal_node',
    bash_command='for i in {1..10}; do sleep 1 && echo "slept $i" E; done',
    dag=dag,
)

complex_dag_fun_6 = BashOperator(
    task_id='parallel1',
    bash_command='for i in {1..10}; do sleep 1 && echo "slept $i" E; done',
    dag=dag,
)

complex_dag_fun_7 = BashOperator(
    task_id='parallel2',
    bash_command='for i in {1..10}; do sleep 1 && echo "slept $i" E; done',
    dag=dag,
)

complex_dag_fun_8 = BashOperator(
    task_id='parallel3',
    bash_command='for i in {1..10}; do sleep 1 && echo "slept 1 sec the $i th time"; done',
    dag=dag,
)

complex_dag_fun_1 >> complex_dag_fun_2
complex_dag_fun_1 >> complex_dag_fun_3
complex_dag_fun_2 >> complex_dag_fun_4
complex_dag_fun_3 >> complex_dag_fun_4
complex_dag_fun_4 >> complex_dag_fun_5

complex_dag_fun_6 >> complex_dag_fun_5
complex_dag_fun_7 >> complex_dag_fun_5
complex_dag_fun_8 >> complex_dag_fun_5