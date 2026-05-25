import os, json
import requests
from PIL import Image
import numpy as np
import requests
from io import BytesIO
import json, sys
from confluent_kafka import KafkaError, KafkaException, Consumer
from dotenv import load_dotenv
from cv2 import resize, INTER_NEAREST
import time 

from scripts.connect.database import get_minio_client, SessionLocal
from worker.eye_centers_finding import find_eye_centers
from scripts.connect.database import get_minio_client
from scripts.services.data_loading.eye_cropp import get_eye_box, Eye, EyeBox, calculate_distance, crop_eye_image
from scripts.connect.models import InferenceImage, Tasks
from back.services import load_image_to_minio

load_dotenv()
FORCE = True
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "red-eye-detection")
BENTO_HOST = os.getenv("BENTO_HOST", "http://localhost:3000")
API_HOST = os.getenv("API_HOST", "http://localhost:3000")
RUNNING = True
MINIO = get_minio_client()

def get_output_image(eye_bbox:EyeBox, source_image:Image, mask:Image, darken_factor:float = 0.2) -> np.array:
    crop_h = eye_bbox.y2 - eye_bbox.y1
    crop_w = eye_bbox.x2 - eye_bbox.x1
    source_image_arr = np.array(source_image)
    mask_arr = np.array(mask)

    mask_resized = resize(
        mask_arr,
        (crop_w, crop_h),
        interpolation=INTER_NEAREST,
    )

    binary_mask = mask_resized > 127
    source_eye_bbox = source_image_arr[eye_bbox.y1:eye_bbox.y2, eye_bbox.x1:eye_bbox.x2]

    source_eye_bbox[binary_mask] = (
        source_eye_bbox[binary_mask] * darken_factor
    ).astype(np.uint8)

    source_image_arr[eye_bbox.y1:eye_bbox.y2, eye_bbox.x1:eye_bbox.x2] = source_eye_bbox

    return source_image_arr



def image_processing(minio_key:str) -> Image:
    client = get_minio_client()
    image_resp = client.get_object(MINIO_BUCKET_NAME, minio_key)
    image = Image.open(image_resp).convert('RGB')
    output_image = image.copy()
    im_arr = np.array(image)
    eyes = find_eye_centers(image)
    left_eye = Eye(eyes['left'][0], eyes['left'][1])
    right_eye = Eye(eyes['right'][0], eyes['right'][1])
    distance = calculate_distance(left_eye.x, left_eye.y, right_eye.x, right_eye.y)

    left_bbox = get_eye_box(left_eye, distance)
    right_bbox = get_eye_box(right_eye, distance)

    left_cropped = crop_eye_image(im_arr, left_bbox)
    right_cropped = crop_eye_image(im_arr, right_bbox)

    left_eye_image = Image.fromarray(left_cropped)
    right_eye_image = Image.fromarray(right_cropped)
    left_buff = BytesIO()
    right_buff = BytesIO()
    left_eye_image.save(left_buff, format='PNG')
    right_eye_image.save(right_buff, format='PNG')
    left_buff.seek(0)
    right_buff.seek(0)

    with requests.Session() as session:

        left_eye_valid = session.post(f"{BENTO_HOST}/eye_validation", files={"image": ("crop.png", left_buff, "image/png")})
        left_eye_valid.raise_for_status()
        left_resp = json.loads(left_eye_valid.content)
        print(left_resp)
        if FORCE or left_resp.get('is_valid') == 1:
            left_buff.seek(0)
            left_eye_mask = session.post(f"{BENTO_HOST}/red_eye_segmentation", files={"image": ("crop.png", left_buff, "image/png")})
            left_eye_mask.raise_for_status()
            mask = Image.open(BytesIO(left_eye_mask.content))
            output_image_arr = get_output_image(left_bbox, output_image, mask)
            output_image = Image.fromarray(output_image_arr)


        right_eye_valid = session.post(f"{BENTO_HOST}/eye_validation", files={"image": ("crop.png", right_buff, "image/png")})
        right_eye_valid.raise_for_status()
        right_resp = json.loads(right_eye_valid.content)
        print(right_resp)
        if FORCE or right_resp.get('is_valid') == 1:
            right_buff.seek(0)
            right_eye_mask = session.post(f"{BENTO_HOST}/red_eye_segmentation", files={"image": ("crop.png", right_buff, "image/png")})
            right_eye_mask.raise_for_status()
            mask = Image.open(BytesIO(right_eye_mask.content))
            output_image_arr = get_output_image(right_bbox, output_image, mask)
            output_image = Image.fromarray(output_image_arr)
    
    return output_image


def get_kafka_message(kafka_host:str, kafka_topic:str):

    consumer = Consumer({
        "bootstrap.servers": kafka_host,
        "group.id": 'worker_group_2',
        "auto.offset.reset": "earliest"
        
    })

    consumer.subscribe([kafka_topic])
    print(consumer)
    
    while RUNNING:
        db = SessionLocal()
        print("Итерация")
        
        msg = consumer.poll(timeout=0.5)

        if not msg:
            print(f"Нет сообщения {msg}")
            time.sleep(5)
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                    (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        
        else:
            payload = json.loads(msg.value().decode("utf-8"))
            task_id = payload['task_id']
            print('Прочитали таску')
            with requests.Session() as s:  
                resp = s.get(f'{API_HOST}/api/tasks/{task_id}')
                resp_content = json.loads(resp.content)
                print(f"Статус таски: {resp_content['status']}")
                if resp_content['status'] == 'created' or resp_content['status'] == 'processing':
                    task = db.query(Tasks).filter(Tasks.task_id == task_id).first()
                    task.status = 'processing'
                    db.commit()
                    image_id = payload['image_id']
                    img = db.query(InferenceImage).filter(InferenceImage.image_id == image_id).first()
                    image_input_path = img.image_minio_input_key
                    output_image = image_processing(image_input_path)
                    if output_image:
                        print('Картинка сгенерирована')
                    minio_path_parts = list(image_input_path.split('/'))
                    out_buf = BytesIO()
                    output_image.save(out_buf, format="PNG")
                    out_buf.seek(0)
                    object_name = load_image_to_minio(
                        client=MINIO,
                        bucket_name="red-eye-detection",
                        file_bytes=out_buf.getvalue(),
                        original_filename=minio_path_parts[-1],
                        content_type="image/png",
                        path='output'
                    )
                    print("Новая картинка в object_name")
                    img.image_minio_output_key = object_name
                    task.status = 'processed'
                    db.commit() 
                    print(object_name)

            consumer.commit(msg)

if __name__ == '__main__':
    kafka_host = os.getenv("KAFKA_HOST", "127.0.0.1:9092")
    print(os.getenv("KAFKA_HOST"))
    kafka_topic = os.getenv("KAFKA_TOPIC", "red-eye-detection")

    get_kafka_message(kafka_host, kafka_topic)