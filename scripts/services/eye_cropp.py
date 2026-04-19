from minio import Minio, S3Error
from pandas import DataFrame
import pandas as pd
import numpy as np
import io, cv2 as cv
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
from dotenv import load_dotenv
from datetime import datetime

from scripts.connect.database import minio_client, SessionLocal
from scripts.connect.models import Image, CroppedEye  # Добавлен импорт CroppedEye

DF_LANDMARKS = pd.read_csv('data\\celeba\\list_landmarks_align_celeba.csv')
TARGET_W = 128
TARGET_H = 96

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

def get_eyes_coords(file_name:str, df:DataFrame) -> tuple:
    obj = df.query(f'image_id = {file_name}')
    left_eye = Eye(obj['lefteye_x'].values[0], obj['lefteye_y'].values[0])
    right_eye = Eye(obj['righteye_x'].values[0], obj['righteye_y'].values[0])
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
    
    object_name = f"eyes/{image_id}_{eye_type}.png"
    try:
        minio_client.put_object(
            "red-eye-detector",
            object_name,
            io.BytesIO(buffer.tobytes()),
            len(buffer)
        )
        return object_name
    except S3Error as e:
        raise Exception(f"MinIO error: {str(e)}")

def process_image_eyes(image_id: int, db: Session) -> bool:  # image_id теперь int
    try:
        image_obj = db.query(Image).filter(Image.image_id == image_id).first()  # Исправлено поле
        if not image_obj:
            print(f"Image {image_id} not found in database")
            return False
        
        response = minio_client.get_object("red-eye-detector", image_obj.image_minio_key)  # Исправлено поле
        image_bytes = response.read()
        image = decode_image_from_bytes(image_bytes)
        
        left_eye, right_eye = get_eyes_coords(image_id, DF_LANDMARKS)
        distance = calculate_distance(left_eye.x, left_eye.y, right_eye.x, right_eye.y)
        
        left_box = get_eye_box(left_eye, distance)
        right_box = get_eye_box(right_eye, distance)
        
        left_cropped = crop_eye_image(image, left_box)
        right_cropped = crop_eye_image(image, right_box)
        
        left_path = save_eye_to_minio(left_cropped, str(image_id), "left")
        right_path = save_eye_to_minio(right_cropped, str(image_id), "right")
        
        # Создаем записи в таблице CroppedEye вместо сохранения в Image
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
    """Process all images that don't have cropped eyes yet"""
    try:
        # Получаем изображения, для которых еще нет вырезанных глаз
        images_with_no_eyes = db.query(Image.image_id).outerjoin(
            CroppedEye, Image.image_id == CroppedEye.image_id
        ).filter(CroppedEye.eye_id == None).all()
        
        for (image_id,) in images_with_no_eyes:
            process_image_eyes(image_id, db)
            
    except Exception as e:
        print(f"Batch processing error: {str(e)}")

if __name__ == '__main__':
    with SessionLocal() as conn:
        batch_process_images(conn)