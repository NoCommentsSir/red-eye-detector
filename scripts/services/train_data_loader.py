import numpy as np
import pandas as pd
import torch
from scripts.connect.models import CroppedEye, EyesMarkingValidation
from scripts.connect.database import SessionLocal
from torch.utils.data import Dataset, DataLoader
from path import Path
from PIL import Image

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
        return torch_image, is_valid_eye    

if __name__ == "__main__":
    training_data = pg_load_training_data()
    df = pd.DataFrame(training_data, columns=['eye_id', 'minio_key', 'is_valid_eye', 'rejecting_reason', 'split'])
    data = EyeQualityDataset(df, "data/eyes_to_review", "train")
    dataloader = DataLoader(data, batch_size=32, shuffle=True)
    for images, labels in dataloader:
        print(images.size(), labels.size())
        break
