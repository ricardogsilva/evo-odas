from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import BaseOperator
from airflow.operators import BashOperator
from airflow.operators import DownloadSceneList
from airflow.operators import ExtractSceneList
from airflow.operators import UpdateSceneList

from landsat8.secrets import postgresql_credentials

landsat8_scene_list = DAG(
    'Landsat8_Scene_List',
    description='DAG for downloading, extracting, and importing scene_list.gz '
                'into postgres db',
    default_args={
        'start_date': datetime(2017, 1, 1),
        'owner': 'airflow',
        'depends_on_past': False,
        'provide_context': True,
        'email': ['xyz@xyz.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'max_threads': 1,
        'download_dir': '/var/data/download/'
    },
    dagrun_timeout=timedelta(hours=1),
    schedule_interval=timedelta(days=1),
    catchup=False
)

download_scene_list_gz = DownloadSceneList(
    task_id='download_scene_list_gz_task',
    download_url='http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz',
    dag=landsat8_scene_list
)

extract_scene_list = ExtractSceneList(
    task_id='extract_scene_list_task',
    dag=landsat8_scene_list
)

update_scene_list_db = UpdateSceneList(
    task_id='update_scene_list_task',
    pg_dbname=postgresql_credentials['pg_dbname'],
    pg_hostname=postgresql_credentials['hostname'],
    pg_port=postgresql_credentials['port'],
    pg_username=postgresql_credentials['username'],
    pg_password=postgresql_credentials['password'],
    dag=landsat8_scene_list
)

download_scene_list_gz.set_downstream(extract_scene_list)
extract_scene_list.set_downstream(update_scene_list_db)
