# Example of Airflow how to dynamicallly tigger another DAG

read data from external source (database or url), and trigger another DAG base on the return value.

# how it works

`dispatch_dag_job` use sensor to regularly poke pendings DAGs to be triggered

use airflow intetrnal api `trigger_dag` to anther DAG


```python
trigger_dag(
    # specify which dag to trigger
    dag_id=pending_dag["target_dag_id"],
    run_id=dro.run_id,
    # DAG dispatch_dag_job pass data to the target DAG
    conf=json.dumps(dro.payload),
    execution_date=None,
    replace_microseconds=False,
)
```


schedule_interval of DAG `hello` is set to None

```python
# DAG hello read the data passed by dispatch_dag_job
payload = context["dag_run"].conf
```