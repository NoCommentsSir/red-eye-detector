from scripts.services.data_loading.installer import load_dataset_from_src
from scripts.services.data_loading.raw_data_loader import load_images_to_db
from scripts.services.data_loading.load_csv_tables import load_images_bbox, load_images_eyes_coord
from scripts.services.data_loading.eye_cropp import batch_process_images
from scripts.services.data_loading.audit_dataset import report_maker
from scripts.connect.database import minio_client, SessionLocal

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable


PATH = Variable.get("IMAGES_PATH", default_var="/opt/airflow/data/celeba/img_align_celeba/img_align_celeba")
folder = Path(PATH)
MINIO_BUCKET_NAME = Variable.get("MINIO_BUCKET_NAME", default_var="red-eye-detection")
BATCH_SIZE = int(Variable.get("BATCH_SIZE", default_var=10))
MINIO_CLIENT = minio_client
pg_conn = BaseHook.get_connection("image_db_strore")
pg_comm_params = {
    'host': pg_conn.host,
    'port': pg_conn.port,
    'dbname': pg_conn.schema,
    'user': pg_conn.login,
    'password': pg_conn.password
}

def f_load_images_to_db(folder, bucket_name, batch_size):
    with SessionLocal() as postgre_client:
        load_images_to_db(minio_client=MINIO_CLIENT, postgres_client=postgre_client, folder=folder, bucket_name=bucket_name, batch_size=batch_size)

def f_batch_process_images():
    with SessionLocal() as postgre_client:
        batch_process_images(postgre_client)

with DAG(
    dag_id="data_prepare_dag",
    start_date=datetime(2026, 4, 23),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    load_dataset_from_kaggle = PythonOperator(
        task_id='kaggle_loader',
        python_callable=load_dataset_from_src,
        op_kwargs={'dataset': "jessicali9530/celeba-dataset", 'dir': "/opt/airflow/data/celeba"}
    )

    store_images = PythonOperator(
        task_id='postgre_minio_loader',
        python_callable=f_load_images_to_db,
        op_kwargs={
            'folder': folder,
            'bucket_name': MINIO_BUCKET_NAME,
            'batch_size': BATCH_SIZE
        }
    )

    store_images_bbox_conf = PythonOperator(
        task_id='img_bbox_conf_loader',
        python_callable=load_images_bbox,
        op_kwargs={
            'file_path': "data/celeba/list_bbox_celeba.csv",
            'pg_params': pg_comm_params
        }
    )

    store_images_eyes_conf = PythonOperator(
        task_id='img_eyes_conf_loader',
        python_callable=load_images_eyes_coord,
        op_kwargs={
            'file_path': "data/celeba/list_landmarks_align_celeba.csv",
            'pg_params': pg_comm_params
        }
    )

    cropp_eye_from_images = PythonOperator(
        task_id='images_eye_cropper',
        python_callable=f_batch_process_images
    )

    make_report = PythonOperator(
        task_id='report_maker',
        python_callable=report_maker
    )


    load_dataset_from_kaggle >> store_images >> [store_images_bbox_conf, store_images_eyes_conf] >> cropp_eye_from_images >> make_report

