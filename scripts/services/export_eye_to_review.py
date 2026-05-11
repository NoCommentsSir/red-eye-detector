from minio import Minio
from minio.error import S3Error 
from minio.commonconfig import CopySource
import csv, os
from dotenv import load_dotenv
from requests import session
load_dotenv()

from scripts.connect.database import SessionLocal, minio_client
from scripts.connect.models import CroppedEye, Image
from sqlalchemy import select
from sqlalchemy.orm import Session

IMAGES_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", 'red-eye-detection')
BATCH_SIZE = 300

def make_dataset_to_review(client:Minio, bucket_name:str, path_to_csv: str, session: Session, batch_to_review: int = 500):
    try:
        client.make_bucket(bucket_name)
    except ValueError:
        print("Invalid bucket name")
    except S3Error:
        print("This bucket already exists")

    stmt = select(CroppedEye, Image.image_name).join(Image, CroppedEye.image_id == Image.image_id).where(CroppedEye.is_valid_eye == None).order_by(CroppedEye.minio_key).limit(batch_to_review)
    results = session.execute(stmt).all()
    with open(path_to_csv, 'w', newline='') as csvfile:
        fieldnames = ['eye_id', 'image_name', 'image_id', 'eye_type', 'minio_key', 'is_valid_eye', 'rejecting_reason']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for eye, image_name in results:
            writer.writerow({
                'eye_id': eye.eye_id,
                'image_name': image_name,
                'image_id': eye.image_id,
                'eye_type': eye.eye_type,
                'minio_key': eye.minio_key,
                'is_valid_eye': eye.is_valid_eye,
                'rejecting_reason': eye.rejecting_reason
            })
            dirs = eye.minio_key.split('/')
            dirs[1] = 'eyes_to_review'
            new_key = '/'.join(dirs)
            client.copy_object(bucket_name, new_key, CopySource(bucket_name, eye.minio_key))
    print(f"Exported {len(results)} records to {path_to_csv}")

if __name__ == "__main__":
    with SessionLocal() as session:
        make_dataset_to_review(minio_client, IMAGES_BUCKET_NAME, 'files/eyes_to_review_1.csv', session, BATCH_SIZE)
        

    


    