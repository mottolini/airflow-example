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

init_environments = [k8s.V1EnvVar(name='MINIO_ENDPOINT', value='minio.vvp.svc:9000'), 
                     k8s.V1EnvVar(name='JAR_PREFIX', value='proximai-data/experiments/job_emr'),
                     k8s.V1EnvVar(name='AWS_ACCESS_KEY_ID', value='admin'),
                     k8s.V1EnvVar(name='AWS_SECRET_ACCESS_KEY', value='WS46advKR')]

start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='spark',
                          image="paktek123/spark-submit:latest",
                          cmds=["/download_jar_and_submit.sh"],
                          env=init_environments,
                          arguments=["--class", "nttdata.samplecompactor.Main", "/sample-compactor-assembly-0.5.jar"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )


passing.set_upstream(start)
