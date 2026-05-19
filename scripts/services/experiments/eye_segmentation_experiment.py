import torch
import torch.nn as nn
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import mlflow, os
from pathlib import Path
from sklearn.metrics import classification_report
from scripts.services.dataset_generation.train_data_loader import SynthRedEyeDataset
from torch.utils.data import DataLoader
from torchvision import transforms
from scripts.models.RedEyeSegmentator import UNet, DiceLoss, fit_model, evaluate

WORK_DIR = Path("data/processed")
CLEAN_DIR = WORK_DIR / "clean"
CORRUPTED_DIR = WORK_DIR / "corrupted"
MASKS_DIR = WORK_DIR / "masks"

def plot_loss_curve(history, output_path="files/loss_curve.png"):
    # Убедиться, что директория существует
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    plt.figure(figsize=(8, 5))
    plt.plot(history["train_loss"], label="train_loss")
    plt.plot(history["valid_loss"], label="valid_loss")
    plt.xlabel("Epoch")
    plt.ylabel("Loss")
    plt.title("Train / Validation Loss")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Loss curve saved to {output_path}")

if __name__ == "__main__":
    model = UNet(1)

    arr = []
    train_dir = os.listdir("data/segmentation/train/images")
    for i in range(len(train_dir)):
        name, ext = os.path.splitext(train_dir[i])
        image_path = 'data/segmentation/train/images' + '/' + train_dir[i]
        mask_path = 'data/segmentation/train/masks' + '/' + name + '_mask' + ext
        split = 'train'
        flg = 1 if 'red' in name else 0
        arr.append([image_path, mask_path, split, flg])
    
    valid_dir = os.listdir("data/segmentation/valid/images")
    for i in range(len(valid_dir)):
        name, ext = os.path.splitext(valid_dir[i])
        image_path = 'data/segmentation/valid/images' + '/' + valid_dir[i]
        mask_path = 'data/segmentation/valid/masks' + '/' + name + '_mask' + ext
        split = 'valid'
        flg = 1 if 'red' in name else 0
        arr.append([image_path, mask_path, split, flg])

    test_dir = os.listdir("data/segmentation/test/images")
    for i in range(len(test_dir)):
        name, ext = os.path.splitext(test_dir[i])
        image_path = 'data/segmentation/test/images' + '/' + test_dir[i]
        mask_path = 'data/segmentation/test/masks' + '/' + name + '_mask' + ext
        split = 'test'
        flg = 1 if 'red' in name else 0
        arr.append([image_path, mask_path, split, flg])
    
    df = pd.DataFrame(arr, columns=['image_path', 'mask_path', 'split', 'has_red_pupil'])
    params = {
            'lr': 3e-4, 
            'eps': 1e-8, 
            'num_epochs': 25,
            'betas': (0.9, 0.999),
            'batch_size': 32,
            'model_name': 'UNet1',
            'dataset_version': 'v1.0',
            'image_size': (128, 96),
            'optimizer': 'Adam',
            'loss': 'DiceLoss + BCEWithLogitsLoss',
            'is_loss_weighted': False
        }
    
    count_clean = len(os.listdir(CLEAN_DIR))
    params['count_clean'] = count_clean
    count_corrupted = len(os.listdir(CORRUPTED_DIR))
    params['count_corrupted'] = count_corrupted
    #w_clean = (count_clean + count_corrupted) / (2 * count_clean + count_corrupted)
    #params['w_clean'] = w_clean
    #w_corrupted = (count_clean + count_corrupted) / (2 * count_corrupted)
    #params['w_corrupted'] = w_corrupted
    #w = np.array([w_clean, w_corrupted]).astype(np.float32)

    train_data = SynthRedEyeDataset("data/segmentation/train/images", "data/segmentation/train/masks", (128, 96),transform=None)
    train_data_loader = DataLoader(train_data, batch_size=params['batch_size'], shuffle=True)
    valid_data = SynthRedEyeDataset("data/segmentation/valid/images", "data/segmentation/valid/masks", (128, 96),transform=None)
    valid_data_loader = DataLoader(valid_data, batch_size=params['batch_size'], shuffle=False)
    test_data = SynthRedEyeDataset("data/segmentation/test/images", "data/segmentation/test/masks", (128, 96),transform=None)
    test_data_loader = DataLoader(test_data, batch_size=params['batch_size'], shuffle=False)
    
    train_ds = mlflow.data.from_pandas(
        df[df["split"] == "train"].reset_index(drop=True),
        source="data/segmentation/metadata.csv",
        name="red_eye_segmentation_train",
    )

    val_ds = mlflow.data.from_pandas(
        df[df["split"] == "val"].reset_index(drop=True),
        source="data/segmentation/metadata.csv",
        name="red_eye_segmentation_valid",
    )

    test_ds = mlflow.data.from_pandas(
        df[df["split"] == "test"].reset_index(drop=True),
        source="data/segmentation/metadata.csv",
        name="red_eye_segmentation_test",
    )
    params['num_train'] = len(train_data)
    params['num_valid'] = len(valid_data)
    params['num_test'] = len(test_data)

    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    print(f"Connecting to MLflow at: {mlflow_uri}")
    
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("red_eye_segmentation")
    with mlflow.start_run():
        mlflow.log_params(params)
        # mlflow.log_param("use_augmentations", True)
        # mlflow.log_param("augmentations", "flip,rotation,color_jitter,gaussian_blur")
        # mlflow.log_param("rotation_degrees", 7)
        # mlflow.log_param("brightness", 0.2)
        # mlflow.log_param("contrast", 0.2)
        # mlflow.log_param("blur_p", 0.15)
        mlflow.log_inputs(
            datasets=[train_ds, val_ds, test_ds],
            contexts=["train", "valid", "test"],
            tags_list=[{"id": "eye_train"}, {"id":"eye_valid"}, {"id":"eye_test"}]
        )
        criterion1 = nn.BCEWithLogitsLoss()
        criterion2 = DiceLoss(smooth=1e-6)
        out = fit_model(model, train_data_loader, criterion1, criterion2, valid_data_loader, params)
        
        loss_curve_path = "files/loss_curve_segm.png"
        plot_loss_curve(out, loss_curve_path)
        mlflow.log_artifact(loss_curve_path)

        test_metrics = evaluate(model, test_data_loader, criterion1, criterion2)
        mlflow.log_metric("test_loss", test_metrics["loss"])
        mlflow.log_metric("test_dice", test_metrics["dice"])
        mlflow.log_metric("test_iou", test_metrics["iou"])
        mlflow.log_metric("test_precision", test_metrics["precision"])
        mlflow.log_metric("test_recall", test_metrics["recall"])
        mlflow.pytorch.log_model(model, "model")
