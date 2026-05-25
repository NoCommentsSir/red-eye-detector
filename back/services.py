from minio import Minio, S3Error
from pathlib import Path
import os, datetime
import io, uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
from dotenv import load_dotenv
import confluent_kafka, json
from confluent_kafka import Producer

from confluent_kafka.admin import AdminClient, NewTopic
from scripts.connect.models import InferenceImage, Tasks

INPUT_BUCKET = os.getenv("MINIO_BUCKET_NAME", "red-eye-detection")

def load_image_to_minio(
    client: Minio,
    bucket_name: str,
    file_bytes: bytes,
    original_filename: str | None = None,
    content_type: str = "image/jpeg",
    path: str = 'input'
) -> str:
    uid = str(uuid.uuid4())

    suffix = ".jpg"
    if original_filename:
        file_suffix = Path(original_filename).suffix
        if file_suffix:
            suffix = file_suffix.lower()

    object_name = f"{path}/{uid}{suffix}"

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(file_bytes),
            length=len(file_bytes),
            content_type=content_type,
        )

        return object_name

    except S3Error as e:
        print(f"Error uploading {object_name} to MinIO: {e}")
        raise

def load_image_to_db(
    db: Session,
    image_name: str,
    image_minio_key: str
) -> InferenceImage:
    try:
        created_date = datetime.datetime.now().date()
        new_image = InferenceImage(
            image_name=image_name,
            image_minio_input_key=image_minio_key,
            image_minio_output_key=None,
            created_date=created_date,
        )
        db.add(new_image)
        db.commit()
        db.refresh(new_image)
        return new_image
    except SQLAlchemyError as e:
        db.rollback()
        print(f"Error inserting image into database: {e}")
        raise

def create_task(db: Session, image_id:int) -> Tasks:
    try:
        created_date = datetime.datetime.now().date()
        new_task = Tasks(
            image_id=image_id,
            status='created',
            created_date=created_date,
        )
        db.add(new_task)
        db.commit()
        db.refresh(new_task)
        return new_task
    except SQLAlchemyError as e:
        db.rollback()
        print(f"Error inserting task into database: {e}")
        raise

def send_kafka_message(
    kafka_host: str,
    kafka_topic: str,
    message: dict,
) -> None:
    admin_client = AdminClient({
        "bootstrap.servers": kafka_host,
    })

    existing_topics = admin_client.list_topics(timeout=10).topics

    if kafka_topic not in existing_topics:
        new_topic = NewTopic(
            topic=kafka_topic,
            num_partitions=1,
            replication_factor=1,
        )

        futures = admin_client.create_topics([new_topic])

        try:
            futures[kafka_topic].result()
            print(f"Topic created: {kafka_topic}")
        except Exception as exc:
            # Если топик создался параллельно другим процессом — не критично
            print(f"Topic creation skipped or failed: {exc}")

    producer = Producer({
        "bootstrap.servers": kafka_host,
    })

    try:
        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")

        producer.produce(
            topic=kafka_topic,
            value=payload,
        )

        producer.flush()

    except Exception as exc:
        print(f"Произошла ошибка отправки данных в Kafka: {exc}")
        raise
    
if __name__ == '__main__':
    pass