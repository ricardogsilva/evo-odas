from datetime import datetime, timedelta
from getpass import getuser
import os

from airflow import DAG
from airflow.operators import DownloadSceneList
from airflow.operators import ExtractSceneList
from airflow.operators import UpdateSceneList

from landsat8.secrets import postgresql_credentials

DOWNLOAD_URL = 'http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz'
DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "download")

landsat8_scene_list = DAG(
    'Landsat8_Scene_List',
    description='DAG for downloading, extracting, and importing scene_list.gz '
                'into postgres db',
    default_args={
        'start_date': datetime(2017, 1, 1),
        'owner': getuser(),
        'depends_on_past': False,
        'provide_context': True,
        'email': ['xyz@xyz.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'max_threads': 1,
    },
    dagrun_timeout=timedelta(hours=1),
    schedule_interval=timedelta(days=1),
    catchup=False
)

# more info on Landsat products on AWS at:
# https://aws.amazon.com/public-datasets/landsat/
download_scene_list_gz = DownloadSceneList(
    task_id='download_scene_list_gz_task',
    download_url=DOWNLOAD_URL,
    download_dir=DOWNLOAD_DIR,
    dag=landsat8_scene_list
)

extract_scene_list = ExtractSceneList(
    task_id='extract_scene_list_task',
    path_to_extract=os.path.join(
        DOWNLOAD_DIR,
        DOWNLOAD_URL.rpartition("/")[-1]
    ),
    dag=landsat8_scene_list
)

update_scene_list_db = UpdateSceneList(
    task_id='update_scene_list_task',
    scene_list_path=os.path.join(
        DOWNLOAD_DIR,
        "{}.csv".format(os.path.splitext(DOWNLOAD_URL.rpartition("/")[-1]))
    ),
    pg_dbname=postgresql_credentials['dbname'],
    pg_hostname=postgresql_credentials['hostname'],
    pg_port=postgresql_credentials['port'],
    pg_username=postgresql_credentials['username'],
    pg_password=postgresql_credentials['password'],
    dag=landsat8_scene_list
)

download_scene_list_gz.set_downstream(extract_scene_list)
extract_scene_list.set_downstream(update_scene_list_db)
