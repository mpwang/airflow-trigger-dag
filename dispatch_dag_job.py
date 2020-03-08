import json
import logging
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import timezone

logger = logging.getLogger(__name__)


def get_trigger_dag_counts():
    # read data from external database or url
    return 1


def get_trigger_dags():
    # read data from external database or url
    return [{"target_dag_id": "hello", "payload": {"from": "world"}}]


class JobSensorOperator(BaseSensorOperator):
    def poke(self, context):
        try:
            pending_count = get_trigger_dag_counts()

        except Exception as e:
            logger.exception(str(e))
            raise e
        else:
            logger.debug("found %s pending_jobs", pending_count)
            return pending_count > 0


def dispatch_jobs(**context):
    logger.debug("context: %s", context)

    pending_dags: List[dict] = get_trigger_dags()
    for pending_dag in pending_dags:
        dro = DagRunOrder(run_id="trig__" + timezone.utcnow().isoformat())
        dro.payload = pending_dag["payload"]

        # trigger airflow dag
        trigger_dag(
            dag_id=pending_dag["target_dag_id"],
            run_id=dro.run_id,
            conf=json.dumps(dro.payload),
            execution_date=None,
            replace_microseconds=False,
        )

        logger.info("Triggered: %s", pending_dag["target_dag_id"])


dag = DAG(
    dag_id="dispatch_dag_job",
    schedule_interval=timedelta(seconds=5),
    description="",
    max_active_runs=1,
    # execution params for this example
    catchup=False,
    start_date=datetime(2018, 1, 1),
)

job_sensor = JobSensorOperator(task_id="job_sensor", poke_interval=5)
job_dispatcher = PythonOperator(
    task_id="job_dispatcher",
    dag=dag,
    python_callable=dispatch_jobs,
    provide_context=True,
)

job_sensor.set_downstream(job_dispatcher)
