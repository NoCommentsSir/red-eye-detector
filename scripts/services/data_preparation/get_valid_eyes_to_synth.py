import io
import os
from pathlib import Path
from PIL import Image
from scripts.connect.database import SessionLocal, get_minio_client
from scripts.connect.models import CroppedEye

MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "red-eye-detection")

OUTPUT_DIR = Path("data/processed")
CLEAN_DIR = OUTPUT_DIR / "clean"

def select_valid_eye_crops(session, limit: int = 300):
    """Выбираем только валидные eye crops из PostgreSQL."""
    return (
        session.query(CroppedEye)
        .filter(CroppedEye.is_valid_eye == 1)
        .limit(limit)
        .all()
    )


def download_eye_image(minio_client, minio_key: str) -> Image.Image:
    response = minio_client.get_object(MINIO_BUCKET_NAME, minio_key)
    try:
        image_bytes = response.read()
    finally:
        response.close()
        response.release_conn()

    return Image.open(io.BytesIO(image_bytes)).convert("RGB")

def generate_red_eye_segmentation_dataset(
    limit: int = 500,
):
    session = SessionLocal()
    minio_client = get_minio_client()

    sample_id = 0

    try:
        valid_eyes = select_valid_eye_crops(session, limit=limit)

        if not valid_eyes:
            raise RuntimeError("Не найдено валидных cropped_eyes.")

        for eye in valid_eyes:
            try:
                clean_image = download_eye_image(minio_client, eye.minio_key)
            except Exception as exc:
                print(f"Ошибка загрузки {eye.minio_key}: {exc}")
                continue

            clean_path = CLEAN_DIR / f"{eye.minio_key[9:]}"

            clean_image.save(clean_path)
            sample_id += 1

    finally:
        session.close()


if __name__ == "__main__":
    generate_red_eye_segmentation_dataset(
        limit=300,
    )