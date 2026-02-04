from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

from datawarehouse.dwh import stagin_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("America/Mexico_City")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email":"data@engineers.com",
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    #"end_date": datetime(2026, 12, 31, tzinfo=local_tz),
}

# Variable
stagin_schema = 'staging'
core_schema = 'core'

with DAG(
    dag_id = 'produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule = '0 14 * * *',
    catchup = False
) as dag:
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    # Set task dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task

with DAG(
    dag_id = 'update_db',
    default_args=default_args,
    description='DAG to process JSON data into Data Warehouse',
    schedule = '0 15 * * *',
    catchup = False
) as dag:
    
    update_staging = stagin_table()
    update_core = core_table()

    # Set task dependencies
    update_staging >> update_core 

with DAG(
    dag_id = 'data_quality',
    default_args=default_args,
    description='DAG to check data quality on both layers in the db using Soda',
    schedule = '0 16 * * *',
    catchup = False
) as dag:
    
    soda_validate_staging = yt_elt_data_quality(stagin_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Set task dependencies
    soda_validate_staging >> soda_validate_core 
