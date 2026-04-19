from minio import Minio, S3Error
from pathlib import Path
from hashlib import sha256
import os, datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
from dotenv import load_dotenv

from scripts.connect.database import minio_client, SessionLocal
from scripts.connect.models import Image

MAX_COUNT = 2000

load_dotenv()
PATH = os.getenv("IMAGES_PATH", "data/celeba/img_align_celeba/img_align_celeba")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "red-eye-detection")
folder = Path(PATH)

def compute_minio_key(file_path:Path) -> str:
    """Генерация ключа хранения"""
    coder = sha256()

    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(8192), b""):
            coder.update(chunk)

    return coder.hexdigest()

def check_file_in_minio(minio_client:Minio, bucket_name:str, file_name:str) -> bool:
    """Проверка на существование файла в хранилище Minio"""
    try:
        minio_client.stat_object(bucket_name, file_name)
        return True
    except S3Error as e:
        error_code = getattr(e, 'code', "")
        if error_code in {'NoSuchKey', 'NoSuchObject', 'NoSuchBucket'}:
            return False
        raise

def build_minio_key(file_path:Path, file_uid:str, source:str = 'celebA') -> str:
    """Строит ключ для Minio"""
    ext = file_path.suffix.lower()
    return f'raw/{source}/{file_uid}{ext}'

def load_file_to_minio(client:Minio, bucket_name:str, file_path:Path, source:str = 'celebA') -> str:
    """Загружает трек из папки в MinIO"""
    uid = compute_minio_key(file_path)
    minio_name = build_minio_key(file_path, uid, source)

    if check_file_in_minio(client, bucket_name, minio_name):
        print(f'Skip existing: {minio_name}')
        return minio_name

    try:
        client.fput_object(
            bucket_name,
            minio_name,
            str(file_path),
            content_type='image/jpeg'
        )
        return minio_name
    except Exception as e:
        print(f"Failed to load a file: {e}")
        raise
    
def load_file_to_postgres(client:Session, file_name:str, uid:str, split:str) -> int:
    """Загружает изображение из папки в Postgres"""
    image = Image(source_name='celebA', image_name=file_name, image_minio_key=uid, split=split, created_date=datetime.date.today(), state='new')
    client.add(image)
    client.commit()
    return image.image_id

def load_images_to_db(minio_client:Minio, postgres_client:Session, folder:Path, bucket_name:str):
    if not minio_client.bucket_exists(bucket_name):
        print("Such bucket not exists!")
        return
    
    cnt = 0
    for item in folder.iterdir():
        try:
            cnt += 1
            split = 'train'
            minio_key = load_file_to_minio(minio_client, bucket_name, item, 'celebA')
            if cnt > MAX_COUNT:
                return
            load_file_to_postgres(postgres_client, item.name, minio_key, split,)
        except SQLAlchemyError as se:
            print(f"Failed to load a file to Postgres: {se}")
            minio_client.remove_object(bucket_name, minio_key)
        except Exception as e:
            print(f"Failed to load a file: {e}")
        
if __name__ == '__main__':
    minio_client = minio_client
    bucket_name = MINIO_BUCKET_NAME
    with SessionLocal() as postgre_client:
        load_images_to_db(minio_client, postgre_client, folder, bucket_name)

