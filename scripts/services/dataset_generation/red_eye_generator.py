import random
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFilter

WORK_DIR = Path("data/processed")
CLEAN_DIR = WORK_DIR / "clean"
CORRUPTED_DIR = WORK_DIR / "corrupted"
MASKS_DIR = WORK_DIR / "masks"

for directory in (CLEAN_DIR, CORRUPTED_DIR, MASKS_DIR):
    directory.mkdir(parents=True, exist_ok=True)


def clamp(value: float, min_value: int, max_value: int) -> int:
    return int(max(min_value, min(round(value), max_value)))


def build_output_name(image_path: str, variant_idx: int, suffix: str) -> str:
    source_name = Path(image_path).name
    stem = Path(source_name).stem
    return f"{stem}_red_v{variant_idx}{suffix}"

def generate_red_pupil(
    image_path: str,
    pupil_center: list[int | float],
    pupil_bbox: list[int | float],
    variant_idx: int = 1,
) -> tuple[Path, Path]:
    px, py = pupil_center
    pbx, pby, pbw, pbh = pupil_bbox

    image_path = Path(image_path)
    if len(image_path.parts) > 1:
        full_image_path = WORK_DIR / image_path
    else:
        full_image_path = CLEAN_DIR / image_path

    if not full_image_path.exists():
        raise FileNotFoundError(f"Image not found: {full_image_path}")

    image = Image.open(full_image_path).convert("RGB")
    width, height = image.size

    center_shift_x = random.uniform(-pbw * 0.06, pbw * 0.06)
    center_shift_y = random.uniform(-pbh * 0.06, pbh * 0.06)

    cx = px + center_shift_x
    cy = py + center_shift_y

    rx = pbw * 0.5
    ry = pbh * 0.5

    x1 = clamp(cx - rx, 0, width - 1)
    y1 = clamp(cy - ry, 0, height - 1)
    x2 = clamp(cx + rx, 0, width - 1)
    y2 = clamp(cy + ry, 0, height - 1)

    if x2 <= x1 or y2 <= y1:
        raise ValueError(
            f"Invalid ellipse bbox for {image_path}: "
            f"x1={x1}, y1={y1}, x2={x2}, y2={y2}"
        )

    red_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
    red_draw = ImageDraw.Draw(red_layer)

    red_color = (
        random.randint(170, 255),  # R
        random.randint(15, 75),    # G
        random.randint(10, 55),    # B
        random.randint(140, 220),  # Alpha
    )

    red_draw.ellipse((x1, y1, x2, y2), fill=red_color)

    # Мягкие края
    blur_radius = random.uniform(0.6, 1.5)
    red_layer = red_layer.filter(ImageFilter.GaussianBlur(radius=blur_radius))

    # Добавляем небольшую неоднородность внутри красной области
    noise = np.random.normal(loc=1.0, scale=0.06, size=(height, width, 1))
    red_array = np.array(red_layer).astype(np.float32)

    red_array[..., :3] *= noise
    red_array = np.clip(red_array, 0, 255).astype(np.uint8)

    red_layer = Image.fromarray(red_array, mode="RGBA")

    corrupted = Image.alpha_composite(image.convert("RGBA"), red_layer).convert("RGB")

    mask = Image.new("L", image.size, 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.ellipse((x1, y1, x2, y2), fill=255)

    corrupted_name = build_output_name(str(image_path), variant_idx, ".png")
    mask_name = build_output_name(str(image_path), variant_idx, "_mask.png")

    corrupted_path = CORRUPTED_DIR / corrupted_name
    mask_path = MASKS_DIR / mask_name

    corrupted.save(corrupted_path)
    mask.save(mask_path)

    return corrupted_path, mask_path


def create_empty_mask_for_clean(image_path: str) -> Path:
    image_path_obj = Path(image_path)

    if len(image_path_obj.parts) > 1:
        full_image_path = WORK_DIR / image_path_obj
    else:
        full_image_path = CLEAN_DIR / image_path_obj

    if not full_image_path.exists():
        raise FileNotFoundError(f"Image not found: {full_image_path}")

    image = Image.open(full_image_path).convert("RGB")
    mask = Image.new("L", image.size, 0)

    stem = Path(image_path).stem
    mask_path = MASKS_DIR / f"{stem}_clean_mask.png"
    mask.save(mask_path)

    return mask_path


def create_red_pupils_dataset(
    annotations_csv_path: str,
    variants_per_image: int = 3,
    only_good_quality: bool = True,
) -> None:
    df = pd.read_csv(annotations_csv_path)

    metadata = []

    for i, row in df.iterrows():
        image_path = row["image_path"]

        pupil_center = [
            row["pupil_center_x"],
            row["pupil_center_y"],
        ]

        pupil_bbox = [
            row["bbox_x_min"],
            row["bbox_y_min"],
            row["bbox_width"],
            row["bbox_height"],
        ]

        # Сохраняем clean-negative mask для оригинального изображения
        try:
            clean_mask_path = create_empty_mask_for_clean(image_path)

            metadata.append(
                {
                    "source_image": image_path,
                    "image_path": str(WORK_DIR / image_path),
                    "mask_path": str(clean_mask_path),
                    "type": "clean",
                    "has_red_eye": 0,
                    "variant": 0,
                    "pupil_center_x": row["pupil_center_x"],
                    "pupil_center_y": row["pupil_center_y"],
                    "bbox_x_min": row["bbox_x_min"],
                    "bbox_y_min": row["bbox_y_min"],
                    "bbox_width": row["bbox_width"],
                    "bbox_height": row["bbox_height"],
                }
            )

        except Exception as error:
            print(f"[WARN] Failed to create clean mask for {image_path}: {error}")
            continue

        # Генерируем synthetic red-eye версии
        for variant_idx in range(1, variants_per_image + 1):
            try:
                corrupted_path, mask_path = generate_red_pupil(
                    image_path=image_path,
                    pupil_center=pupil_center,
                    pupil_bbox=pupil_bbox,
                    variant_idx=variant_idx,
                )

                metadata.append(
                    {
                        "source_image": image_path,
                        "image_path": str(corrupted_path),
                        "mask_path": str(mask_path),
                        "type": "synthetic_red",
                        "has_red_eye": 1,
                        "variant": variant_idx,
                        "pupil_center_x": row["pupil_center_x"],
                        "pupil_center_y": row["pupil_center_y"],
                        "bbox_x_min": row["bbox_x_min"],
                        "bbox_y_min": row["bbox_y_min"],
                        "bbox_width": row["bbox_width"],
                        "bbox_height": row["bbox_height"],
                    }
                )

            except Exception as error:
                print(f"[WARN] Failed to generate red pupil for {image_path}: {error}")

    metadata_path = WORK_DIR / "red_pupils_metadata.csv"
    pd.DataFrame(metadata).to_csv(metadata_path, index=False)

    print(f"Annotations used: {len(df)}")
    print(f"Generated records: {len(metadata)}")
    print(f"Metadata saved to: {metadata_path}")
    print(f"Corrupted images dir: {CORRUPTED_DIR}")
    print(f"Masks dir: {MASKS_DIR}")


if __name__ == "__main__":
    create_red_pupils_dataset(
        annotations_csv_path="files/parsed_pupil_annotations.csv",
        variants_per_image=3,
        only_good_quality=True,
    )