import json
from pathlib import Path

IMAGE_DIR = Path("data/processed/clean")
OUTPUT_PATH = Path("label_studio_tasks.json")

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}

tasks = []

for image_path in sorted(IMAGE_DIR.iterdir()):
    if image_path.suffix.lower() not in ALLOWED_EXTENSIONS:
        continue

    tasks.append({
        "data": {
            "image": f"/data/local-files/?d=clean/{image_path.name}"
        }
    })

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(tasks, f, ensure_ascii=False, indent=2)

print(f"Created {len(tasks)} tasks in {OUTPUT_PATH}")