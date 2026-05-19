#!/usr/bin/env python3
import csv, os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from scripts.connect.database import SessionLocal, minio_client
from scripts.connect.models import Image, CroppedEye
from dotenv import load_dotenv

load_dotenv()
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "red-eye-detection")
from sqlalchemy import select

REPORT_DIR = Path("audit_reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

@dataclass
class AuditIssue:
    entity_type: str
    entity_id: int
    image_id: Optional[int]
    object_name: str
    issue: str

def minio_exists(client, bucket_name: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except Exception:
        return False

def normalize_object_name(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.strip().lstrip("/")

def audit_images(session) -> List[AuditIssue]:
    issues: List[AuditIssue] = []
    images = session.execute(select(Image)).scalars().all()

    for image in images:
        object_name = normalize_object_name(getattr(image, "image_minio_key", None))

        if not object_name:
            issues.append(
                AuditIssue(
                    "image",
                    image.image_id,
                    image.image_id,
                    "",
                    "missing_object_name_in_db",
                )
            )
            continue

        if not minio_exists(minio_client, MINIO_BUCKET_NAME, object_name):
            issues.append(
                AuditIssue(
                    "image",
                    image.image_id,
                    image.image_id,
                    object_name,
                    "missing_in_minio",
                )
            )

    return issues


def audit_cropped_eyes(session) -> List[AuditIssue]:
    issues: List[AuditIssue] = []
    eyes = session.execute(select(CroppedEye)).scalars().all()

    for eye in eyes:
        object_name = normalize_object_name(getattr(eye, "minio_key", None))

        if not object_name:
            issues.append(
                AuditIssue(
                    "cropped_eye",
                    eye.eye_id,
                    getattr(eye, "image_id", None),
                    "",
                    "missing_object_name_in_db",
                )
            )
            continue

        if not minio_exists(minio_client, MINIO_BUCKET_NAME, object_name):
            issues.append(
                AuditIssue(
                    "cropped_eye",
                    eye.eye_id,
                    getattr(eye, "image_id", None),
                    object_name,
                    "missing_in_minio",
                )
            )

    return issues
def write_csv(path: Path, rows: Iterable[AuditIssue]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["entity_type", "entity_id", "image_id", "object_name", "issue"])
        for row in rows:
            writer.writerow([row.entity_type, row.entity_id, row.image_id, row.object_name, row.issue])

def report_maker() -> int:
    session = SessionLocal()
    try:
        image_issues = audit_images(session)
        eye_issues = audit_cropped_eyes(session)
        all_issues = [*image_issues, *eye_issues]

        write_csv(REPORT_DIR / "audit_issues.csv", all_issues)

        print("Audit finished.")
        print(f"Images with issues: {len(image_issues)}")
        print(f"Cropped eyes with issues: {len(eye_issues)}")
        print(f"Total issues: {len(all_issues)}")
        print(f"Report: {REPORT_DIR / 'audit_issues.csv'}")
        return 0
    finally:
        session.close()

if __name__ == "__main__":
    pass
