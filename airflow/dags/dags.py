import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="spark_dag_test",
    default_args={
        "owner": "airflow",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id = "start",
    python_callable = lambda: print("DAG started"),
    dag = dag
)

spark_job = SparkSubmitOperator(
    task_id = "spark_job",
    application = "jobs/test1/target/scala-2.12/test1_2.12-0.1.0-SNAPSHOT.jar",  # Update with your Spark
    conn_id = "spark_conn",
    dag = dag
)

end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("DAG ended"),
    dag = dag
)

start >> spark_job >> end 