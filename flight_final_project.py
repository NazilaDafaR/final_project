from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

#default argumen untuk dag
default_args = {
  'owner': 'Nazila', 
  'depends_on_past': False,
  'email': ['dafanazila0711@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 0,
  'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'read_postgres_data',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024,11,20), 
    catchup= False
)

# Task 1: Membaca data dari PostgreSQL
read_data = PostgresOperator(
    task_id='read_data',
    postgres_conn_id='postgres_conn',
    sql='SELECT * FROM flight_customer_booking;',
    do_xcom_push=True,
    dag=dag,
)

# Task 2: Menjalankan Spark job dengan BashOperator
spark_job = BashOperator(
    task_id='spark_job',
    bash_command='spark-submit --jars /home/hadoop/postgresql-42.2.26.jar /home/hadoop/airflow/scripts/pyspark_flight_final_project.py jdbc:postgresql://localhost:5432/msib_7 postgres post123',
    dag=dag,
)

# Task 3: Memberitahukan jika proses data yang sudah dianalisis berhasil disimpan pada db baru
done_task = BashOperator(
    task_id = 'done_postgres',
    bash_command='echo "pyspark_sql insert data done"',
    dag=dag,
)

read_data >> spark_job >> done_task