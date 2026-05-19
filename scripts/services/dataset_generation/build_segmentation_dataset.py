import random, os, shutil
from pathlib import Path

import pandas as pd
from PIL import Image

RANDOM_SEED = 42

WORK_DIR = Path("data/processed")
CLEAN_DIR = WORK_DIR / "clean"
CORRUPTED_DIR = WORK_DIR / "corrupted"
MASKS_DIR = WORK_DIR / "masks"

OUTPUT_DIR = Path("data/segmentation")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
for i in ['train', 'valid', 'test']:
    dir = OUTPUT_DIR / i
    dir.mkdir(parents=True, exist_ok=True)
    im_dir = dir / 'images'
    im_dir.mkdir(parents=True, exist_ok=True)
    msk_dir = dir / 'masks'
    msk_dir.mkdir(parents=True, exist_ok=True)

TRAIN_RATIO = 0.7
VAL_RATIO = 0.15
TEST_RATIO = 0.15

def get_minio_path(path:Path) -> str:
    if len(path.stem.split('_red_v')) > 1:
        return path.stem.split('_red_v')[0]
    
    if len(path.stem.split('_clean_mask')) > 1:
        return path.stem.split('_clean_mask')[0]
    
    return path.stem

def split_source_ids(source_ids: list[str]) -> dict[str, str]:
    random.seed(RANDOM_SEED)

    source_ids = sorted(set(source_ids))
    random.shuffle(source_ids)

    total = len(source_ids)
    train_end = int(total * TRAIN_RATIO)
    val_end = train_end + int(total * VAL_RATIO)

    split_map = {}

    for source_id in source_ids[:train_end]:
        split_map[source_id] = "train"

    for source_id in source_ids[train_end:val_end]:
        split_map[source_id] = "valid"

    for source_id in source_ids[val_end:]:
        split_map[source_id] = "test"

    return split_map

def copy_pair(
    image_path: Path,
    mask_path: Path,
    split: str,
    output_image_name: str,
    output_mask_name: str,
) -> tuple[Path, Path]:
    output_image_path = OUTPUT_DIR / split / "images" / output_image_name
    output_mask_path = OUTPUT_DIR / split / "masks" / output_mask_name

    shutil.copy2(image_path, output_image_path)
    shutil.copy2(mask_path, output_mask_path)
    return output_image_path, output_mask_path

def get_image():
    clean_arr = os.listdir(CLEAN_DIR)
    clean_paths = [get_minio_path(MASKS_DIR / i) for i in clean_arr]
    corrupt_arr = os.listdir(CORRUPTED_DIR)
    splited_images = split_source_ids(clean_paths)
    metadata = []
    
    for i in range(len(clean_arr)):
        name, ext = os.path.splitext(clean_arr[i])
        image_path = CLEAN_DIR / clean_arr[i]
        mask_name = name + '_clean_mask' + ext
        mask_path = MASKS_DIR / mask_name
        minio_path = get_minio_path(image_path)
        split = splited_images[minio_path]
        out_image_path, out_mask_path = copy_pair(image_path, mask_path, split, name + '_clean' + ext, mask_name)

        metadata.append(
            {
                "split": split,
                "source_id": minio_path,
                "image_path": str(out_image_path),
                "mask_path": str(out_mask_path),
                "type": "clean",
                "has_red_eye": 0,
            }
        )

    for i in range(len(clean_arr)):
        name, ext = os.path.splitext(clean_arr[i])
        image_path = CLEAN_DIR / clean_arr[i]
        mask_name = name + '_clean_mask' + ext
        mask_path = MASKS_DIR / mask_name
        minio_path = get_minio_path(image_path)
        split = splited_images[minio_path]
        out_image_path, out_mask_path = copy_pair(image_path, mask_path, split, name + '_clean' + ext, mask_name)

        metadata.append(
            {
                "split": split,
                "source_id": minio_path,
                "image_path": str(out_image_path),
                "mask_path": str(out_mask_path),
                "type": "clean",
                "has_red_eye": 0,
            }
        )

    for i in range(len(corrupt_arr)):
        name, ext = os.path.splitext(corrupt_arr[i])
        image_path = CORRUPTED_DIR / corrupt_arr[i]
        mask_name = name + '_mask' + ext
        mask_path = MASKS_DIR / mask_name
        minio_path = get_minio_path(image_path)
        split = splited_images[minio_path]
        out_image_path, out_mask_path = copy_pair(image_path, mask_path, split, name + ext, mask_name)

        metadata.append(
            {
                "split": split,
                "source_id": minio_path,
                "image_path": str(out_image_path),
                "mask_path": str(out_mask_path),
                "type": "clean",
                "has_red_eye": 0,
            }
        )

    split_counts = {}
    for item in metadata:
        split = item["split"]
        split_counts[split] = split_counts.get(split, 0) + 1
    
    print("\n=== Статистика загруженных данных ===")
    print(f"Обучающий набор (train): {split_counts.get('train', 0)}")
    print(f"Валидационный набор (valid): {split_counts.get('valid', 0)}")
    print(f"Тестовый набор (test): {split_counts.get('test', 0)}")
    print(f"Всего загружено: {len(metadata)}")
    print("=" * 35)

if __name__ == '__main__':
    get_image()