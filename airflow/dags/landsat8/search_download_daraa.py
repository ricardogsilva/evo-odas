from collections import namedtuple
from datetime import datetime
from datetime import timedelta
import logging

from airflow.models import DAG
from airflow.operators import GDALAddoOperator
from airflow.operators import GDALTranslateOperator
from airflow.operators import Landsat8DownloadOperator
from airflow.operators import Landsat8MTLReaderOperator
from airflow.operators import Landsat8ProductDescriptionOperator
from airflow.operators import Landsat8ProductZipFileOperator
from airflow.operators import Landsat8SearchOperator
from airflow.operators import Landsat8ThumbnailOperator

from landsat8.secrets import postgresql_credentials

Landsat8Area = namedtuple("Landsat8Area", [
    "name",
    "path",
    "row",
    "bands"
])


def generate_dag(area, default_args):
    """Generate Landsat8 ingestion DAGs.

    Parameters
    ----------
    area: Landsat8Area
        Configuration parameters for the Landsat8 area to be downloaded
    default_args: dict
        Default arguments for all tasks in the DAG.

    """

    dag = DAG(
       "Search_{}_Landsat8".format(area.name),
        description="DAG for searching and ingesting {} AOI in Landsat8 data "
                    "from scene_list".format(area.name),
        default_args=default_args,
        dagrun_timeout=timedelta(hours=1),
        schedule_interval=timedelta(days=1),
        catchup=False,
        params={
            "area": area,
        }
    )
    search_task = Landsat8SearchOperator(
        task_id='landsat8_search_{}'.format(area.name),
        area=area,
        cloud_coverage=90.9,
        db_credentials=postgresql_credentials,
        dag=dag
    )
    download_thumbnail = Landsat8DownloadOperator(
        task_id="download_thumbnail",
        download_dir="/var/data/download",
        get_inputs_from=search_task.task_id,
        url_fragment="thumb_smal.jpg",
        dag=dag
    )
    product_thumbnail_task = Landsat8ThumbnailOperator(
        task_id='landsat8_product_thumbnail',
        thumb_size_x="64",
        thumb_size_y="64",
        dag=dag
    )
    download_metadata = Landsat8DownloadOperator(
        task_id="download_metadata",
        download_dir="/var/data/download",
        get_inputs_from=search_task.task_id,
        url_fragment="MTL.txt",
        dag=dag
    )
    product_json_task = Landsat8MTLReaderOperator(
        task_id='landsat8_product_json',
        loc_base_dir='/efs/geoserver_data/coverages/landsat8/daraa',
        metadata_xml_path='./geo-solutions-work/evo-odas/metadata-ingestion/'
                          'templates/metadata.xml',
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
    for band in area.bands:
        download_band = Landsat8DownloadOperator(
            task_id="download_band{}".format(band),
            download_dir="/var/data/download",
            get_inputs_from=search_task.task_id,
            url_fragment="B{}.TIF".format(band),
            dag=dag
        )
        translate = GDALTranslateOperator(
            task_id="translate_band{}".format(band),
            get_inputs_from=download_band.task_id,
            dag=dag
        )
        addo = GDALAddoOperator(
            task_id="add_overviews_band{}".format(band),
            get_inputs_from=translate.task_id,
            resampling_method="average",
            max_overview_level=128,
            compress_overview="PACKBITS",
            dag=dag
        )
        download_band.set_upstream(search_task)
        translate.set_upstream(download_band)
        addo.set_upstream(translate)
        product_zip_task.set_upstream(addo)


    download_thumbnail.set_upstream(search_task)
    download_metadata.set_upstream(search_task)
    product_json_task.set_upstream(download_metadata)
    product_thumbnail_task.set_upstream(download_thumbnail)
    product_description_task.set_upstream(download_metadata)
    product_zip_task.set_upstream(product_description_task)
    product_zip_task.set_upstream(product_json_task)
    product_zip_task.set_upstream(product_thumbnail_task)
    return dag


AREAS = [
    Landsat8Area(name="daraa", path=174, row=37, bands=[1, 2, 3]),
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
    })
    globals()[dag.dag_id] = dag
