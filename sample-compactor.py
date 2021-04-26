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
    'kubernetes_sample', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='spark',
                          image="paktek123/spark-submit:latest",
                          cmds=["/download_jar_and_submit.sh"],
                          image_pull_policy="Always",
                          env_vars={"MINIO_ENDPOINT": "http://minio.vvp.svc.cluster.local:9000",
                                    "JAR_PREFIX": "proximai-data/experiments/job_emr/sample-compactor-assembly-0.5.jar",
                                    "AWS_ACCESS_KEY_ID": "admin",
                                    "AWS_SECRET_ACCESS_KEY": "WS46advKR"
                                   },
                          arguments=["--class", "nttdata.samplecompactor.Main", "/sample-compactor-assembly-0.5.jar"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )


passing.set_upstream(start)
