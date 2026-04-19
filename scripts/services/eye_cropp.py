from minio import Minio, S3Error
import pandas as pd
import numpy as np
import io, os, cv2 as cv
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

from scripts.connect.database import minio_client, SessionLocal
from scripts.connect.models import Image, CroppedEye, ImageEyesCoords

TARGET_W = 128
TARGET_H = 96

load_dotenv()
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "red-eye-detection")

class Eye:
    x: int
    y: int

    def __init__(self, x:int, y:int):
        self.x = x
        self.y = y

class EyeBox:
    x1: int
    y1: int
    x2: int
    y2: int

    def __init__(self, x1:int, y1:int, x2:int, y2:int):
        self.x1 = x1
        self.y1 = y1
        self.x2 = x2
        self.y2 = y2

def get_eyes_coords_from_db(image_id: int, db: Session) -> tuple:
    """Получаем координаты глаз из базы данных PostgreSQL"""
    coords = db.query(ImageEyesCoords).filter(ImageEyesCoords.image_id == image_id).first()
    if not coords:
        raise ValueError(f"No eye coordinates found for image_id {image_id}")

    left_eye = Eye(coords.lefteye_x, coords.lefteye_y)
    right_eye = Eye(coords.righteye_x, coords.righteye_y)
    return (left_eye, right_eye)

def calculate_distance(left_x:int, left_y:int, right_x:int, right_y:int) -> float:
    return float(np.sqrt((left_x - right_x)**2 + (left_y - right_y)**2))

def get_eye_box(eye:Eye, distance:float, w_scale:float = 0.8, h_scale = 0.55):
    crop_w = int(round(distance * w_scale))
    crop_h = int(round(distance * h_scale))
    x1 = eye.x - crop_w // 2
    y1 = eye.y - crop_h // 2
    x2 = x1 + crop_w
    y2 = y1 + crop_h
    return EyeBox(x1, y1, x2, y2)

def decode_image_from_bytes(image:bytes) -> np.ndarray:
    arr = np.frombuffer(image, dtype=np.uint8)
    img = cv.imdecode(arr, cv.IMREAD_COLOR)
    if img is None:
        raise ValueError("Error with loading an image!")
    return img

def crop_eye_image(image: np.ndarray, eye_box: EyeBox) -> np.ndarray:
    h, w = image.shape[:2]
    x1 = max(0, eye_box.x1)
    y1 = max(0, eye_box.y1)
    x2 = min(w, eye_box.x2)
    y2 = min(h, eye_box.y2)

    if x1 >= x2 or y1 >= y2:
        raise ValueError("Invalid crop coordinates")

    cropped = image[y1:y2, x1:x2]
    resized = cv.resize(cropped, (TARGET_W, TARGET_H))
    return resized

def save_eye_to_minio(eye_image: np.ndarray, image_id: str, eye_type: str) -> str:
    success, buffer = cv.imencode('.png', eye_image)
    if not success:
        raise ValueError("Failed to encode image")

    object_name = f"raw/eyes/{image_id}_{eye_type}.png"
    try:
        minio_client.put_object(
            f"{MINIO_BUCKET_NAME}",
            object_name,
            io.BytesIO(buffer.tobytes()),
            len(buffer)
        )
        return object_name
    except S3Error as e:
        raise Exception(f"MinIO error: {str(e)}")

def process_image_eyes(image_id: int, hash:str, db: Session) -> bool:
    try:
        image_obj = db.query(Image).filter(Image.image_id == image_id).first()
        if not image_obj:
            print(f"Image {image_id} not found in database")
            return False

        response = minio_client.get_object(f'{MINIO_BUCKET_NAME}', image_obj.image_minio_key)
        image_bytes = response.read()
        image = decode_image_from_bytes(image_bytes)

        # Получаем координаты глаз из базы данных PostgreSQL
        left_eye, right_eye = get_eyes_coords_from_db(image_id, db)
        distance = calculate_distance(left_eye.x, left_eye.y, right_eye.x, right_eye.y)

        left_box = get_eye_box(left_eye, distance)
        right_box = get_eye_box(right_eye, distance)

        left_cropped = crop_eye_image(image, left_box)
        right_cropped = crop_eye_image(image, right_box)

        left_path = save_eye_to_minio(left_cropped, hash, "left")
        right_path = save_eye_to_minio(right_cropped, hash, "right")

        # Создаем записи в таблице CroppedEye
        left_eye_record = CroppedEye(
            image_id=image_obj.image_id,
            eye_type="left",
            minio_key=left_path,
            width=TARGET_W,
            height=TARGET_H,
            is_valid_eye=True,
            quality_score=0.0,
            has_red_eye=None,
            processed_date=datetime.utcnow()
        )

        right_eye_record = CroppedEye(
            image_id=image_obj.image_id,
            eye_type="right",
            minio_key=right_path,
            width=TARGET_W,
            height=TARGET_H,
            is_valid_eye=True,
            quality_score=0.0,
            has_red_eye=None,
            processed_date=datetime.utcnow()
        )

        db.add(left_eye_record)
        db.add(right_eye_record)
        db.commit()

        print(f"Successfully processed eyes for image {image_id}")
        return True

    except SQLAlchemyError as e:
        db.rollback()
        print(f"Database error: {str(e)}")
        return False
    except Exception as e:
        print(f"Error processing image {image_id}: {str(e)}")
        return False

def batch_process_images(db: Session) -> None:
    """
    Обрабатывает пакет изображений для вырезания глаз.
    
    Логика:
    1. Выбирает изображения со статусом 'pending' (или NULL), лимитируя выборку batch_size.
    2. Для каждого изображения вызывает процесс кроппинга.
    3. Если кроппинг успешен -> обновляет статус изображения на 'cropped'.
    4. Если ошибка -> статус не меняется (останется 'pending'), чтобы повторить позже.
    """

    images_to_process = db.query(Image.image_id, Image.hash).filter(
        Image.state == 'new'
    ).all()

    if not images_to_process:
        print("No images pending for eye cropping.")
        return

    print(f"Starting batch processing for {len(images_to_process)} images...")

    processed_count = 0
    error_count = 0

    for image_id, hash_val in images_to_process:
        try:
            process_image_eyes(image_id, hash_val, db)
            
            db.query(Image).filter(Image.image_id == image_id).update(
                {"state": 'cropped'}
            )
            processed_count += 1
            db.commit() 
            
        except Exception as e:
            error_count += 1
            print(f"Error processing image {image_id}: {str(e)}")
            db.rollback()

    print(f"Batch finished. Successfully processed: {processed_count}, Errors: {error_count}")

if __name__ == '__main__':
    with SessionLocal() as conn:
        batch_process_images(conn)