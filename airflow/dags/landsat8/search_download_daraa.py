from airflow.models import DAG
from airflow.operators import BashOperator, Landsat8SearchOperator, Landsat8DownloadOperator
import logging
from datetime import datetime
from datetime import timedelta
import pgsqlConfig as PGSQL


##################################################
# General and shared configuration between tasks
##################################################
daraa_default_args = {
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
######################################################
# Task specific configuration
######################################################
daraa_search_args = {\
	#'acquisition_date': '2017-04-11 05:36:29.349932',
	'cloud_coverage': 90.9,
	'path': 174,
	'row' : 37,
	'pgdbname':PGSQL.psql_options['dbname'],
	'pghostname':PGSQL.psql_options['hostname'],
	'pgport':PGSQL.psql_options['port'],
	'pgusername':PGSQL.psql_options['username'],
	'pgpassword':PGSQL.psql_options['password'],
}


daraa_download_args = {\
	'download_dir': '/home/moataz/airflow/data/downloads/',
	'number_of_bands' : 2
}

# DAG definition
daraa_dag = DAG('Search_daraa_Landsat8', 
	description='DAG for searching Daraa AOI in Landsat8 data from scenes_list',
	default_args=daraa_default_args,
	dagrun_timeout=timedelta(hours=1),
	schedule_interval=timedelta(days=1),
	catchup=False)

# Landsat Search Task Operator
search_daraa_task = Landsat8SearchOperator(\
		task_id = 'landsat8_search_daraa_task',
		cloud_coverage = daraa_search_args['cloud_coverage'], 
		path = daraa_search_args['path'], 
		row = daraa_search_args['row'], 
		pgdbname = daraa_search_args['pgdbname'], 
		pghostname = daraa_search_args['pghostname'], 
		pgport = daraa_search_args['pgport'], 
		pgusername = daraa_search_args['pgusername'], 
		pgpassword = daraa_search_args['pgpassword'], 
		dag = daraa_dag
)

# Landsat Download Task Operator
download_daraa_task = Landsat8DownloadOperator(\
		task_id= 'landsat8_download_daraa_task', 
		download_dir = daraa_download_args['download_dir'] ,
		number_of_bands = daraa_download_args['number_of_bands'], 
		dag = daraa_dag
)

search_daraa_task >> download_daraa_task 
