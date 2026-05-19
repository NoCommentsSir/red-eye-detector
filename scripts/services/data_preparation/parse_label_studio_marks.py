import csv
import json
from pathlib import Path
from urllib.parse import unquote

EXPORT_JSON_PATH = Path("files/marking_for_synth.json")
OUTPUT_CSV_PATH = Path("files/parsed_pupil_annotations.csv")

def extract_image_path(task: dict) -> str:
    image_url = task.get("data", {}).get("image")

    if not image_url:
        return ""

    image_url = unquote(image_url)
    if "?d=" in image_url:
        return image_url.split("?d=", 1)[1]

    return image_url

def parse_task(task: dict) -> dict | None:
    annotations = task.get("annotations", [])

    if not annotations:
        return None

    annotation = annotations[0]

    if annotation.get("was_cancelled"):
        return None

    results = annotation.get("result", [])

    image_path = extract_image_path(task)
    image_name = Path(image_path).name

    parsed = {
        "task_id": task.get("id"),
        "image_path": image_path,
        "image_name": image_name,

        "original_width": None,
        "original_height": None,

        "pupil_center_x": None,
        "pupil_center_y": None,

        "bbox_x_min": None,
        "bbox_y_min": None,
        "bbox_x_max": None,
        "bbox_y_max": None,
        "bbox_width": None,
        "bbox_height": None,

        "synthetic_quality": None,
    }

    for result in results:
        from_name = result.get("from_name")
        value = result.get("value", {})

        original_width = result.get("original_width")
        original_height = result.get("original_height")

        if original_width is not None:
            parsed["original_width"] = original_width

        if original_height is not None:
            parsed["original_height"] = original_height

        if from_name == "pupil_center":
            if original_width is None or original_height is None:
                continue

            x_percent = value.get("x")
            y_percent = value.get("y")

            if x_percent is None or y_percent is None:
                continue

            parsed["pupil_center_x"] = round(x_percent / 100 * original_width)
            parsed["pupil_center_y"] = round(y_percent / 100 * original_height)

        elif from_name == "pupil_bbox":
            if original_width is None or original_height is None:
                continue

            x_percent = value.get("x")
            y_percent = value.get("y")
            width_percent = value.get("width")
            height_percent = value.get("height")

            if None in [x_percent, y_percent, width_percent, height_percent]:
                continue

            x_min = x_percent / 100 * original_width
            y_min = y_percent / 100 * original_height
            bbox_width = width_percent / 100 * original_width
            bbox_height = height_percent / 100 * original_height

            parsed["bbox_x_min"] = round(x_min)
            parsed["bbox_y_min"] = round(y_min)
            parsed["bbox_width"] = round(bbox_width)
            parsed["bbox_height"] = round(bbox_height)
            parsed["bbox_x_max"] = round(x_min + bbox_width)
            parsed["bbox_y_max"] = round(y_min + bbox_height)

        elif from_name == "synthetic_quality":
            choices = value.get("choices", [])
            parsed["synthetic_quality"] = choices[0] if choices else None

    return parsed


def is_complete_annotation(row: dict) -> bool:
    required_fields = [
        "image_path",
        "image_name",
        "original_width",
        "original_height",
        "pupil_center_x",
        "pupil_center_y",
        "bbox_x_min",
        "bbox_y_min",
        "bbox_x_max",
        "bbox_y_max",
        "bbox_width",
        "bbox_height",
    ]

    return all(row.get(field) is not None for field in required_fields)


def generate_dataset_for_pupils():
    with open(EXPORT_JSON_PATH, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    rows = []

    for task in tasks:
        row = parse_task(task)

        if row is None:
            continue

        if not is_complete_annotation(row):
            print(f"Skip incomplete annotation: task_id={row.get('task_id')}, image={row.get('image_name')}")
            continue

        rows.append(row)

    OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "task_id",
        "image_path",
        "image_name",
        "original_width",
        "original_height",
        "pupil_center_x",
        "pupil_center_y",
        "bbox_x_min",
        "bbox_y_min",
        "bbox_x_max",
        "bbox_y_max",
        "bbox_width",
        "bbox_height",
        "synthetic_quality",
    ]

    with open(OUTPUT_CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Parsed annotations: {len(rows)}")
    print(f"Saved to: {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    generate_dataset_for_pupils()