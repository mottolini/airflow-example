from kubernetes.client import models as k8s
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['marco@proxim.ai'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'clustering-pipeline-schema', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='spark',
                          image="ottolini/spark-submit:spark3.1.1-hadoop3.2",
                          cmds=["/download_jar_and_submit.sh"],
                          image_pull_policy="Always",
                          env_vars={"MINIO_ENDPOINT": "http://minio.vvp.svc.cluster.local:9000",
                                    "PYTHON_DEP": "proximai-data/experiments/job_emr/data-analysis-venv.tar.gz",
                                    "PYTHON_SCRIPT": "proximai-data/experiments/job_emr/clustering_pipeline_schema.py",
                                    "AWS_ACCESS_KEY_ID": "admin",
                                    "AWS_SECRET_ACCESS_KEY": "WS46advKR",
                                    "ACCESS_KEY": "admin",
                                    "SECRET_KEY": "WS46advKR",
                                    "ENDPOINT": "http://minio.vvp.svc.cluster.local:9000",
                                    "WRITE_PATH_COMPACTOR": "s3a://proximai-data/datalake/compacted/samples2/"
                                   },
                          arguments=[
                              "--conf", "spark.driver.AWS_ACCESS_KEY_ID=admin",
                              "--conf", "spark.driver.AWS_SECRET_ACCESS_KEY=WS46advKR",
                              "--conf", "spark.driver.AWS_S3_ENDPOINT=http://minio.vvp.svc.cluster.local:9000",
                              "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
                              "--deploy-mode", "client",
                              "--py-files", "data-analysis-venv.tar.gz",
                              "/clustering_pipeline_schema.py"
                          ],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )


passing.set_upstream(start)
