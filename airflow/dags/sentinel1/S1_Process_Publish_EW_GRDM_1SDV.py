from airflow import DAG
from datetime import datetime
from sentinel1.s1_grd_subdag_factory import gdal_processing_sub_dag
from airflow.operators import ZipInspector, MockDownload, SubDagOperator

default_args = {
    'start_date': datetime(2017, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['airflow@evoodas.dlr.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    'download_dir': '/var/data/download',
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/germany.geojson',
    'startdate': '2017-05-10T10:30:00Z',
    'enddate': '2017-05-10T10:31:00Z',
    'platformname': 'Sentinel-2',
    'identifier': 'S2A_MSIL1C_20170510T103031_N0205_R108_T32UPV_20170510T103025'
}

INTERVAL = '0 11 * * *'
DATE = datetime(2017, 5, 4)
DAG_NAME = 'S1_Process_Publish_EW_GRDM_1SDV'
SUBDAG_NAME = 'sentinel1_gdal'

main_dag = DAG(DAG_NAME, description='Sentinel1 ingestion flow',
                          schedule_interval=INTERVAL,
                          start_date=DATE,
                          catchup=False)

#search_task = DHUSSearchOperator(task_id='dhus_search_task', dag=dag)

#download_task = DHUSDownloadOperator(task_id='dhus_download_task', dag=dag)

mock_download_task = MockDownload(
    downloaded_path='/var/data/download/Sentinel-1/GRD/S1A_IW_GRDH_1SDV_20170612T142435_20170612T142500_017002_01C507_4338.zip',
    task_id='download',
    dag=main_dag
)

zip_task = ZipInspector(
    extension_to_search='tiff',
    task_id='zip_inspector',
    dag=main_dag
)

sentinel1_gdal_task = SubDagOperator(
    subdag = gdal_processing_sub_dag(DAG_NAME, SUBDAG_NAME, DATE, INTERVAL),
    task_id=SUBDAG_NAME,
    dag=main_dag
)

#zip_task >> sentinel1_gdal_task >> add_granule_task
mock_download_task >> zip_task >> sentinel1_gdal_task
