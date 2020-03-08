import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

logger = logging.getLogger(__name__)


def hello(**context):

    payload = context["dag_run"].conf
    logger.debug("payload: %s", payload)

    # do something about payload
    # here just log payload as an example
    logger.info("Greeting from %s", payload["from"])


dag = DAG(
    dag_id="hello",
    # Important to set schedule_internval to None,
    # ensure this job will be only triggered by dispatch_dag_job
    schedule_interval=None,
    # execution params for this example
    start_date=datetime(2018, 1, 1),
)
hello_operator = PythonOperator(
    task_id="hello_operator", dag=dag, python_callable=hello, provide_context=True
)
