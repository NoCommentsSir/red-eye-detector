import os, json
from typing import Annotated
import mimetypes
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, UploadFile, status, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from minio import Minio, S3Error
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from scripts.connect.database import get_minio_client, get_db
from scripts.connect.models import InferenceImage, Tasks
from back.services import load_image_to_minio, load_image_to_db, create_task, send_kafka_message
from dotenv import load_dotenv

load_dotenv()
KAFKA_HOST = os.getenv("KAFKA_HOST", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "red-eye-detection")

load_dotenv()
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "red-eye-detection")

def _load_cors_origins() -> list[str]:
    origins = os.getenv("CORS_ALLOWED_ORIGINS")
    if origins:
        return [origin.strip() for origin in origins.split(",") if origin.strip()]

    return [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]

api = FastAPI(title="Audioseeker API")
api.add_middleware(
    CORSMiddleware,
    allow_origins=_load_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
    
async def upload_image(minio_client: Minio, file: UploadFile, path:str):
    file_bytes = await file.read()

    object_name = load_image_to_minio(
        client=minio_client,
        bucket_name="red-eye-detection",
        file_bytes=file_bytes,
        original_filename=file.filename,
        content_type=file.content_type or "image/jpeg",
        path=path
    )

    return object_name

def iter_minio_object(minio_response, chunk_size: int = 1024 * 1024):
    try:
        for chunk in minio_response.stream(chunk_size):
            yield chunk
    finally:
        minio_response.close()
        minio_response.release_conn()

@api.get("/")
def read_root():
    return HTMLResponse(content="<h2>Hello!</h2>")

@api.post(
    "/api/images/upload",
    status_code=status.HTTP_201_CREATED,
)
async def insert_image(
    file: Annotated[UploadFile, File(...)],
    db: Session = Depends(get_db),
    minio: Minio = Depends(get_minio_client)
):
    try:
        dct = {}
        minio_key = await upload_image(minio, file, 'input')
        new_image = load_image_to_db(db, file.filename, minio_key)
        task = create_task(db, new_image.image_id)
        dct['task_id'] = task.task_id
        dct['image_id'] = new_image.image_id
        dct['status'] = task.status
        send_kafka_message(KAFKA_HOST, KAFKA_TOPIC, dct)
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    except S3Error as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return dct

@api.get("/api/tasks/{task_id}")
def get_task(
    task_id: int,
    db: Session = Depends(get_db)
):
    task = db.query(Tasks).filter(Tasks.task_id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found") 
    return {
        'task_id':task.task_id,
        'image_id':task.image_id,
        'status':task.status
    }
    
    
@api.get("/api/images/{image_id}/result")
def det_output_image(
    image_id: int,
    db: Session = Depends(get_db),
    minio: Minio = Depends(get_minio_client),
):
    image = db.query(InferenceImage).filter(InferenceImage.image_id == image_id).first()

    if not image:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Image not found",
        )
    
    task = db.query(Tasks).filter(Tasks.image_id == image_id).first()

    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )
    
    if task.status != 'processed':
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Task still not processed. Current status: {task.status}",
        )
    
    try:
        output = minio.get_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=image.image_minio_output_key
        )

        media_type, _ = mimetypes.guess_type(image.image_minio_output_key)
        if media_type is None:
            media_type = "image/jpeg"

        filename = image.image_minio_output_key.split("/")[-1]

        return StreamingResponse(
            iter_minio_object(output),
            media_type=media_type,
            headers={
                "Content-Disposition": f'inline; filename="{filename}"'
            },
        )

    except S3Error as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc