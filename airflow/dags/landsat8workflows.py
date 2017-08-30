"""Airflow DAGs for managing Landsat8 workflows."""

import datetime as dt
from getpass import getuser

from airflow import DAG
from airflow.operators import PythonLazyOperator

DEFAULT_ARGS = {
    "owner": getuser(),
    "depends_on_past": False,
    "provide_context": True,
    "start_date": dt.datetime(2017, 1, 1),
    # provide the download_dir as a config parameter in airflow.cfg
}

scene_list_dag = DAG(
    "landsat8_scene_list_updater",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=dt.timedelta(hours=1),
    schedule_interval=dt.timedelta(days=1),
    # provide the catchup=False argument by setting it in airflow.cfg
)

update_database = PythonLazyOperator(
    task_id="update_database",
    python_callable="taskcallbacks/landsat8scenelist.py:update_database",
)
