from collections import namedtuple
from datetime import datetime
from datetime import timedelta
import logging

from airflow.models import DAG
from airflow.operators import BaseOperator
from airflow.operators import BashOperator
from airflow.operators import GDALAddoOperator
from airflow.operators import GDALTranslateOperator
from airflow.operators import Landsat8DownloadOperator
from airflow.operators import Landsat8MTLReaderOperator
from airflow.operators import Landsat8ProductDescriptionOperator
from airflow.operators import Landsat8ProductZipFileOperator
from airflow.operators import Landsat8SearchOperator
from airflow.operators import Landsat8ThumbnailOperator
from airflow.operators import PythonOperator

from landsat8.secrets import postgresql_credentials

Area = namedtuple("Area", [
    name,
    path,
    row,
    bands,  # make this default to [1]
])

XComPull = namedtuple("XCom", [
    pull_task_id,
    pull_key_srcfile,
])


def generate_dag(area, default_args):
    dag = DAG(
        "Search_{}_Landsat8".format(area.name),
        description="DAG for searching {} AOI in Landsat8 data from "
                    "scenes_list".format(area.name),
        default_args=default_args,
        dagrun_timeout=timedelta(hours=1),
        schedule_interval=timedelta(days=1),
        catchup=False
    )
    search_task = Landsat8SearchOperator(
        task_id='landsat8_search_{}'.format(area.name),
        cloud_coverage=90.9,
        path=area.path,
        row=area.row,
        pgdbname=postgresql_credentials['dbname'],
        pghostname=postgresql_credentials['hostname'],
        pgport=postgresql_credentials['port'],
        pgusername=postgresql_credentials['username'],
        pgpassword=postgresql_credentials['password'],
        dag=dag
    )
    download_task = Landsat8DownloadOperator(
        task_id='landsat8_download_{}'.format(area.name),
        download_dir="/var/data/download",
        bands=area.bands,
        dag=dag
    )
    translate_task = GDALTranslateOperator(
        task_id='landsat8_translate_{}'.format(area.name),
        xcom=XComPull(
            task_id="landsat8_download_{}".format(area.name), 
            key_srcfile="download_dir"
        ),
        working_dir="/var/data/download",
        blockx_size="512",
        blocky_size="512",
        tiled="YES",
        b="1",
        ot="UInt16",
        of="GTiff",
        dag=dag
    )
    addo_task = GDALAddoOperator(
        task_id='landsat8_addo_{}'.format(area.name),
        xcom=XComPull(
            task_id="landsat8_translate_{}".format(area.name), 
            key_srcfile="translated_scenes_dir"
        ),
        resampling_method="average",
        max_overview_level=128,
        compress_overview="PACKBITS",
        photometric_overview="MINISBLACK",
        interleave_overview="",
        dag=dag
    )
    product_json_task = Landsat8MTLReaderOperator(
        task_id='landsat8_product_json',
        loc_base_dir='/efs/geoserver_data/coverages/landsat8/daraa',
        metadata_xml_path='./geo-solutions-work/evo-odas/metadata-ingestion/'
                          'templates/metadata.xml',
        dag=dag
    )
    product_thumbnail_task = Landsat8ThumbnailOperator(
        task_id='landsat8_product_thumbnail',
        thumb_size_x="64",
        thumb_size_y="64",
        dag=dag
    )
    product_description_task = Landsat8ProductDescriptionOperator(
        description_template='./geo-solutions-work/evo-odas/metadata-ingestion/'
                             'templates/product_abstract.html',
        task_id='landsat8_product_description',
        dag=dag
    )

    product_zip_task = Landsat8ProductZipFileOperator(
        task_id='landsat8_product_zip',
        dag=dag
    )
    download_task.set_upstream(search_task)
    translate_task.set_upstream(download_task)
    addo_task.set_upstream(translate_task)
    product_json_task.set_upstream(addo_task)
    product_thumbnail_task.set_upstream(product_json_task)
    product_description_task.set_upstream(product_thumbnail_task)
    product_zip_task.set_upstream(product_description_task)
    return dag


AREAS = [
    Area(name="daraa", path=0, row=0, bands=[1])
]

for area in AREAS:
    dag = generate_dag(area, default_args={
        'start_date': datetime(2017, 1, 1),
        'owner': 'airflow',
        'depends_on_past': False,
        'provide_context': True,
        'email': ['xyz@xyz.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'max_threads': 1,
    }
    globals()[dag.dag_id] = dag
