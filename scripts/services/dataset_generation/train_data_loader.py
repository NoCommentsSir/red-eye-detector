import numpy as np
import pandas as pd
import torch
import os
from scripts.connect.models import CroppedEye, EyesMarkingValidation
from scripts.connect.database import SessionLocal
from torch.utils.data import Dataset, DataLoader
from path import Path
from PIL import Image

reasons = {
    'side_eye': 6,
    'blur': 5,
    'reflection': 4,
    'hairs': 2,
    'closed': 1,
    'sun_glasses': 3
}
def pg_load_training_data():
    session = SessionLocal()
    try:
        data = session.query(
            CroppedEye.eye_id,
            CroppedEye.minio_key,
            CroppedEye.is_valid_eye,
            CroppedEye.rejecting_reason,
            EyesMarkingValidation.split
        ).join(EyesMarkingValidation, CroppedEye.eye_id == EyesMarkingValidation.eye_id).filter(
            CroppedEye.is_valid_eye is not None
        ).all()
        return data
    finally:
        session.close()

class EyeQualityDataset(Dataset):

    def __init__(self, df, data_path, split, transform=None):
        self.data = df[df["split"] == split].reset_index(drop=True)
        self.data_path = Path(data_path)
        self.transform = transform
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        row = self.data.iloc[idx]
        minio_key = row['minio_key'][9:]
        is_valid_eye = row['is_valid_eye']
        rejecting_reason = row['rejecting_reason']
        image_path = self.data_path / minio_key
        image = Image.open(image_path).convert("RGB")   
        if self.transform:
            image = self.transform(image)
            torch_image = image
        else:
            image_arr = np.array(image, dtype=np.float32)
            torch_image = torch.from_numpy(image_arr).permute(2, 0, 1).float() / 255.0
        reason = 0
        if rejecting_reason:
            reason = reasons[rejecting_reason] 
        return torch_image, is_valid_eye   

class SynthRedEyeDataset(Dataset):

    def __init__(self, images_dir, masks_dir, image_size, transform=None):
        self.images_dir = Path(images_dir)
        self.masks_dir = Path(masks_dir)
        self.image_size = image_size
        self.transform = transform

        self.image_paths = sorted(
            path for path in self.images_dir.iterdir()
            if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
        )
    
    def __len__(self):
        return len(self.image_paths)
    
    def __getitem__(self, idx):
        image_path = self.image_paths[idx]
        mask_name = f"{image_path.stem}_mask.png"
        mask_path = self.masks_dir / mask_name

        if not mask_path.exists():
            raise FileNotFoundError(
                f"Mask not found for image {image_path.name}. "
                f"Expected: {mask_path}"
            )
        
        image = Image.open(image_path).convert("RGB")   
        mask = Image.open(mask_path).convert("L")  

        image_arr = np.array(image, dtype=np.float32) / 255.0
        mask_arr = np.array(mask, dtype=np.float32) / 255.0
        mask_arr = (mask_arr > 0.5).astype(np.float32)

        if self.transform is not None:
            transformed = self.transform(image=image_arr, mask=mask_arr)
            image_arr = transformed["image"]
            mask_arr = transformed["mask"]

        torch_image = torch.from_numpy(image_arr).permute(2, 0, 1).float()
        torch_mask = torch.from_numpy(mask_arr).unsqueeze(0).float()
        return torch_image, torch_mask    

if __name__ == "__main__":
    train_dataset = SynthRedEyeDataset(
        images_dir="data/segmentation/train/images",
        masks_dir="data/segmentation/train/masks",
        image_size=(128, 96),
    )

    train_loader = DataLoader(
        train_dataset,
        batch_size=16,
        shuffle=True,
        num_workers=2,
    )

    images, masks = next(iter(train_loader))

    print(images.shape)  # [16, 3, 128, 128]
    print(masks.shape)   # [16, 1, 128, 128]
    print(images.min(), images.max())
    print(masks.min(), masks.max())
