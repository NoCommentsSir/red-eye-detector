from minio import Minio, S3Error
from pathlib import Path
from hashlib import sha256
import os, datetime, re
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
from dotenv import load_dotenv

from scripts.connect.database import minio_client, SessionLocal
from scripts.connect.models import Image

BATCH_SIZE = 2000

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
        return minio_name, uid

    try:
        client.fput_object(
            bucket_name,
            minio_name,
            str(file_path),
            content_type='image/jpeg'
        )
        return minio_name, uid
    except Exception as e:
        print(f"Failed to load a file: {e}")
        raise
    
def load_file_to_postgres(client:Session, file_name:str, minio_name:str, split:str, uid:str) -> int:
    """Загружает изображение из папки в Postgres"""
    name, format = os.path.splitext(file_name)
    image_number = int(name)
    image = Image(
        source_name='celebA', 
        image_name=file_name, 
        image_minio_key=minio_name, 
        split=split, 
        created_date=datetime.date.today(), 
        state='new', 
        hash=uid,
        image_number=image_number
    )
    client.add(image)
    client.commit()
    return image.image_id

def get_image_number(filename: str) -> int | None:
    """
    Извлекает числовой индекс из имени файла формата '00001.jpg'.
    Возвращает None, если формат не подходит.
    """
    match = re.match(r"^(\d+)\.\w+$", filename)
    if match:
        return int(match.group(1))
    return None

def load_images_to_db(minio_client: Minio, postgres_client: Session, folder: Path, bucket_name: str):
    """
    Загружает изображения батчами.
    Логика:
    1. Находит максимальный image_number в БД.
    2. Пропускает все файлы <= этого номера.
    3. Загружает следующие файлы, пока счетчик новых загрузок не достигнет batch_size.
    """
    
    if not minio_client.bucket_exists(bucket_name):
        print(f"Bucket '{bucket_name}' does not exist!")
        return

    max_id_result = postgres_client.query(func.max(Image.image_number)).scalar()
    last_loaded_number = max_id_result if max_id_result is not None else 0

    print(f"Last loaded image number in DB: {last_loaded_number}. Starting batch processing...")
    cnt_loaded = 0  

    for item in folder.iterdir():
        if not item.is_file():
            continue

        current_number = get_image_number(item.name)
        
        if current_number is None:
            print(f"Skipping file with invalid name format: {item.name}")
            continue
            
        if current_number <= last_loaded_number:
            continue

        if cnt_loaded >= BATCH_SIZE:
            print(f"Batch limit ({BATCH_SIZE}) reached. Stopping.")
            break

        try:
            split = 'train'
            
            minio_key, uid = load_file_to_minio(minio_client, bucket_name, item, 'celebA')

            load_file_to_postgres(postgres_client, item.name, minio_key, split, uid)
            
            cnt_loaded += 1
            
            if cnt_loaded % 100 == 0:
                print(f"Loaded {cnt_loaded} files in this batch...")

        except SQLAlchemyError as se:
            print(f"Failed to load file {item.name} to Postgres: {se}")
            postgres_client.rollback()
            try:
                minio_client.remove_object(bucket_name, minio_key)
            except Exception as e_minio:
                print(f"Failed to cleanup MinIO object {minio_key}: {e_minio}")
                
        except Exception as e:
            print(f"Unexpected error loading file {item.name}: {e}")
            postgres_client.rollback()

    print(f"Batch job finished. Total new files loaded: {cnt_loaded}")
        
if __name__ == '__main__':
    minio_client = minio_client
    bucket_name = MINIO_BUCKET_NAME
    with SessionLocal() as postgre_client:
        load_images_to_db(minio_client, postgre_client, folder, bucket_name)

