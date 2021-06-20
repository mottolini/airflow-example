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
                          image="ottolini/spark-submit:latest",
                          cmds=["/download_jar_and_submit.sh"],
                          image_pull_policy="Always",
                          env_vars={"MINIO_ENDPOINT": "http://minio.vvp.svc.cluster.local:9000",
                                    "JAR_PREFIX": "proximai-data/experiments/job_emr/sample-compactor-assembly-0.5.3.jar",
                                    "AWS_ACCESS_KEY_ID": "admin",
                                    "AWS_SECRET_ACCESS_KEY": "WS46advKR",
                                    "ACCESS_KEY": "ENV_DIRECT"
                                   },
                          arguments=[
                              "--conf", "spark.executorEnv.TEST=TEST_EXECUTOR",
                              "--conf", "ACCESS_KEY=CONF_DIRECT",
                              "--conf", "spark.executorEnv.AWS_ACCESS_KEY_ID=admin",
                              "--conf", "spark.executorEnv.AWS_SECRET_ACCESS_KEY=WS46advKR",
                              "--conf", "spark.executorEnv.ACCESS_KEY=pippo",
                              "--conf", "spark.executorEnv.AWS_S3_ENDPOINT=http://minio.vvp.svc.cluster.local:9000",
                              "--conf", "spark.executorEnv.WRITE_PATH_COMPACTOR=s3a://proximai-data/datalake/compacted/samples/",
                              "--conf", "spark.executorEnv.READ_PATH_COMPACTOR=s3a://proximai-data/datalake/rawdata/samples/",
                              "--conf", "spark.executorEnv.MAX_RECORD_FILE_COMPACTOR=1000000000",
                              "--conf", "spark.driver.TEST=TEST_DRIVER",
                              "--conf", "spark.driver.ACCESS_KEY=pollo",
                              "--conf", "spark.driver.AWS_ACCESS_KEY_ID=admin",
                              "--conf", "spark.driver.AWS_SECRET_ACCESS_KEY=WS46advKR",
                              "--conf", "spark.driver.AWS_S3_ENDPOINT=http://minio.vvp.svc.cluster.local:9000",
                              "--conf", "spark.driver.WRITE_PATH_COMPACTOR=s3a://proximai-data/datalake/compacted/samples/",
                              "--conf", "spark.driver.READ_PATH_COMPACTOR=s3a://proximai-data/datalake/rawdata/samples/",
                              "--conf", "spark.driver.MAX_RECORD_FILE_COMPACTOR=1000000000",
                              "--deploy-mode", "client",
                              "--class", "nttdata.samplecompactor.Main",
                              "/sample-compactor-assembly-0.5.3.jar"
                          ],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )


passing.set_upstream(start)
