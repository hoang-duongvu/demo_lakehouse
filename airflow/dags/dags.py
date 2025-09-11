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
    application = "jobs/test1/target/scala-2.12/test1_2.12-0.1.0-SNAPSHOT.jar",
    java_class="org.test.app.Pipeline_02_opt", 
    conn_id = "spark_conn",
    packages= "com.typesafe:config:1.4.3,io.trino:trino-jdbc:422,io.prestosql:presto-jdbc:350,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
    env_vars={
        # "RUN_DATE": "{{ ds }}",
        # "RUN_HOUR": "{{ data_interval_start.in_timezone('Asia/Bangkok').strftime('%-H') }}"
        "RUN_DATE": "2023-10-01",
        "RUN_HOUR": "5"
        # => return "5". Muốn return "05" thì %H thay vì %-H
    },
    dag = dag
)

end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("DAG ended"),
    dag = dag
)

start >> spark_job >> end 
 