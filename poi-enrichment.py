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
    'poi_enrichment', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='spark',
                          image="ottolini/spark-submit:spark3.1.1-hadoop3.2",
                          cmds=["/download_jar_and_submit.sh"],
                          image_pull_policy="Always",
                          startup_timeout_seconds=300,
                          resources={'request_memory': '3Gi',
                                     'request_cpu': '200m',
                                     'limit_memory': '3Gi',
                                     'limit_cpu': 1},
                          env_vars={"MINIO_ENDPOINT": "http://minio.vvp.svc.cluster.local:9000",
                                    "JAR_PREFIX": "proximai-data/experiments/job_emr/poi-enricher-assembly-0.6.3.jar",
                                    "AWS_ACCESS_KEY_ID": "admin",
                                    "AWS_SECRET_ACCESS_KEY": "WS46advKR",
                                    "ACCESS_KEY": "admin",
                                    "SECRET_KEY": "WS46advKR",
                                    "ENDPOINT": "http://minio.vvp.svc.cluster.local:9000",
                                    "WRITE_PATH_COMPACTOR": "s3a://proximai-data/datalake/inferreddata/enrichedPoi/"
                                   },
                          arguments=[
                              "--verbose",
                              "--deploy-mode", "client",
                              "--class", "ai.proxim.PoiEnrichment",
                              "/poi-enricher-assembly-0.6.3.jar"
                          ],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )


passing.set_upstream(start)
