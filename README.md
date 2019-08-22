# airflow_plugins
airflow plugins

ImpalaOperator
UFileSensor


example:
```
from airflow.sensors import UFileSensor
t=UFileSensor(
    task_id='t',
    filepath='xx',
    bucket_name='xx',
    poke_interval=60,
    dag=dag
)
```
```
from airflow.operators import ImpalaOperator
t = ImpalaOperator(
    task_id='t',
    hql=hql,
    schema='schema',
    priority_weight=50,
    dag=dag
)
```
