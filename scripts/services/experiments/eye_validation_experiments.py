import torch
import torch.nn as nn
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import mlflow
import os
from pathlib import Path
from sklearn.metrics import classification_report
from scripts.services.dataset_generation.train_data_loader import EyeQualityDataset, pg_load_training_data
from torch.utils.data import DataLoader
from scripts.models.EyeCroppValidator import LeNet, fit_model, evaluate
from torchvision.models import resnet18, ResNet18_Weights

def imshow(img):
    """Display a tensor image"""
    # Convert tensor to numpy
    if isinstance(img, torch.Tensor):
        img = img.detach().cpu().numpy()
    
    # Handle batches - take only first image
    if img.ndim == 4:  # (batch, channels, height, width)
        img = img[0]
    
    # Move channels to last dimension for display
    if img.ndim == 3 and img.shape[0] == 1:  # (1, height, width)
        img = img[0]
    elif img.ndim == 3 and img.shape[0] == 3:  # (3, height, width)
        img = np.transpose(img, (1, 2, 0))
    
    plt.imshow(np.squeeze(img))
    plt.show()

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
    model = resnet18(weights=ResNet18_Weights.DEFAULT)
    for param in model.parameters():
        param.requires_grad = False
    model.fc = nn.Linear(model.fc.in_features, 2)

    params = {
            'lr': 1e-3, 
            'eps': 1e-8, 
            'num_epochs': 100,
            'betas': (0.9, 0.999),
            'batch_size': 32,
            'model_name': 'ResNet',
            'dataset_version': 'v1.0',
            'image_size': (128, 96),
            'optimizer': 'Adam',
            'loss': 'CrossEntropyLoss',
            'is_loss_weighted': True
        }
    
    training_data = pg_load_training_data()
    df = pd.DataFrame(training_data, columns=['eye_id', 'minio_key', 'is_valid_eye', 'rejecting_reason', 'split'])
    count_valid = len(df[(df['is_valid_eye'] == 1) & (df['split'] == 'train')])
    params['count_valid'] = count_valid
    count_invalid = len(df[(df['is_valid_eye'] == 0) & (df['split'] == 'train')])
    params['count_valid'] = count_invalid
    w_val = len(df) / (2 * count_valid)
    params['w_val'] = w_val
    w_inval = len(df) / (2 * count_invalid)
    params['w_inval'] = w_inval
    w = np.array([w_inval, w_val]).astype(np.float32)

    train_data = EyeQualityDataset(df, "data/eyes_to_review", "train", transform=None)
    train_data_loader = DataLoader(train_data, batch_size=params['batch_size'], shuffle=True)
    valid_data = EyeQualityDataset(df, "data/eyes_to_review", "valid", transform=None)
    valid_data_loader = DataLoader(valid_data, batch_size=params['batch_size'], shuffle=False)
    test_data = EyeQualityDataset(df, "data/eyes_to_review", "test", transform=None)
    test_data_loader = DataLoader(test_data, batch_size=params['batch_size'], shuffle=False)
    
    train_mlflow_ds = mlflow.data.from_pandas(
        df[df['split'] == 'train'],
        source="postgres_marking_data",
        name="eye_quality_train",
    )
    valid_mlflow_ds = mlflow.data.from_pandas(
        df[df['split'] == 'valid'],
        source="postgres_marking_data",
        name="eye_quality_valid",
    )
    test_mlflow_ds = mlflow.data.from_pandas(
        df[df['split'] == 'test'],
        source="postgres_minio_manifest",
        name="eye_quality_test",
    )
    params['num_train'] = len(train_data)
    params['num_valid'] = len(valid_data)
    params['num_test'] = len(test_data)

    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    print(f"Connecting to MLflow at: {mlflow_uri}")
    
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("eye_quality_classification")
    with mlflow.start_run():
        mlflow.log_params(params)
        # mlflow.log_param("use_augmentations", True)
        # mlflow.log_param("augmentations", "flip,rotation,color_jitter,gaussian_blur")
        # mlflow.log_param("rotation_degrees", 7)
        # mlflow.log_param("brightness", 0.2)
        # mlflow.log_param("contrast", 0.2)
        # mlflow.log_param("blur_p", 0.15)
        mlflow.log_inputs(
            datasets=[train_mlflow_ds, valid_mlflow_ds, test_mlflow_ds],
            contexts=["train", "valid", "test"],
            tags_list=[{"id": "eye_train"}, {"id":"eye_valid"}, {"id":"eye_test"}]
        )
        criterion = nn.CrossEntropyLoss(weight=torch.from_numpy(w))
        out = fit_model(model, train_data_loader, criterion, valid_data_loader, params)
        
        loss_curve_path = "files/loss_curve.png"
        plot_loss_curve(out, loss_curve_path)
        mlflow.log_artifact(loss_curve_path)

        test_metrics = evaluate(model, test_data_loader, criterion)
        mlflow.log_metric("test_loss", test_metrics["loss"])
        mlflow.log_metric("test_accuracy", test_metrics["accuracy"])
        mlflow.log_metric("test_macro_f1", test_metrics["macro_f1"])
        mlflow.log_metric("test_weighted_f1", test_metrics["weighted_f1"])
        
        report = classification_report(
            test_metrics["labels"],
            test_metrics["preds"],
            target_names=["invalid_eye", "valid_eye"],
        )

        print(report)

        report_path = "files/classification_report.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report)
        
        mlflow.log_artifact(report_path)
        mlflow.pytorch.log_model(model, "model")

    
    # training_data = pg_load_training_data()
    # df = pd.DataFrame(training_data, columns=['eye_id', 'minio_key', 'is_valid_eye', 'rejecting_reason', 'split'])
    # count_valid = len(df[(df['is_valid_eye'] == 1) & (df['split'] == 'train')])
    # params['count_valid'] = count_valid
    # w_val = len(df) / (7 * count_valid)
    # params['w_val'] = w_val
    # count_closed = len(df[(df['rejecting_reason'] == 'closed') & (df['split'] == 'train')])
    # params['count_closed'] = count_closed
    # w_cl = len(df) / (7 * count_closed)
    # params['w_closed'] = w_cl
    # count_hairs = len(df[(df['rejecting_reason'] == 'hairs') & (df['split'] == 'train')])
    # params['count_hairs'] = count_hairs
    # w_h = len(df) / (7 * count_hairs)
    # params['w_hairs'] = w_h
    # count_glasses = len(df[(df['rejecting_reason'] == 'sun_glasses') & (df['split'] == 'train')])
    # params['count_glasses'] = count_glasses
    # w_g = len(df) / (7 * count_glasses)
    # params['w_sun_glasses'] = w_g
    # count_reflection = len(df[(df['rejecting_reason'] == 'reflection') & (df['split'] == 'train')])
    # params['count_reflection'] = count_reflection
    # w_r = len(df) / (7 * count_reflection)
    # params['w_reflection'] = w_r
    # count_blur = len(df[(df['rejecting_reason'] == 'blur') & (df['split'] == 'train')])
    # params['count_blur'] = count_blur
    # w_b = len(df) / (7 * count_blur)
    # params['w_blur'] = w_b
    # count_side = len(df[(df['rejecting_reason'] == 'side_eye') & (df['split'] == 'train')])
    # params['count_side'] = count_side
    # w_s = len(df) / (7 * count_side)
    # params['w_side_eye'] = w_s
    # w = np.array([w_val, w_cl, w_h, w_g, w_r, w_b, w_s]).astype(np.float32)

    # train_data = EyeQualityDataset(df, "data/eyes_to_review", "train", transform=None)
    # train_data_loader = DataLoader(train_data, batch_size=params['batch_size'], shuffle=True)
    # valid_data = EyeQualityDataset(df, "data/eyes_to_review", "valid", transform=None)
    # valid_data_loader = DataLoader(valid_data, batch_size=params['batch_size'], shuffle=False)
    # test_data = EyeQualityDataset(df, "data/eyes_to_review", "test", transform=None)
    # test_data_loader = DataLoader(test_data, batch_size=params['batch_size'], shuffle=False)
    
    # train_mlflow_ds = mlflow.data.from_pandas(
    #     df[df['split'] == 'train'],
    #     source="postgres_marking_data",
    #     name="eye_quality_train",
    # )
    # valid_mlflow_ds = mlflow.data.from_pandas(
    #     df[df['split'] == 'valid'],
    #     source="postgres_marking_data",
    #     name="eye_quality_valid",
    # )
    # test_mlflow_ds = mlflow.data.from_pandas(
    #     df[df['split'] == 'test'],
    #     source="postgres_minio_manifest",
    #     name="eye_quality_test",
    # )
    # params['num_train'] = len(train_data)
    # params['num_valid'] = len(valid_data)
    # params['num_test'] = len(test_data)

    # mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    # print(f"Connecting to MLflow at: {mlflow_uri}")
    
    # mlflow.set_tracking_uri(mlflow_uri)
    # mlflow.set_experiment("eye_quality_classification")
    # with mlflow.start_run():
    #     mlflow.log_params(params)
    #     # mlflow.log_param("use_augmentations", True)
    #     # mlflow.log_param("augmentations", "flip,rotation,color_jitter,gaussian_blur")
    #     # mlflow.log_param("rotation_degrees", 7)
    #     # mlflow.log_param("brightness", 0.2)
    #     # mlflow.log_param("contrast", 0.2)
    #     # mlflow.log_param("blur_p", 0.15)
    #     mlflow.log_inputs(
    #         datasets=[train_mlflow_ds, valid_mlflow_ds, test_mlflow_ds],
    #         contexts=["train", "valid", "test"],
    #         tags_list=[{"id": "eye_train"}, {"id":"eye_valid"}, {"id":"eye_test"}]
    #     )
    #     criterion = nn.CrossEntropyLoss(weight=torch.from_numpy(w))
    #     out = fit_model(model, train_data_loader, criterion, valid_data_loader, params)
        
    #     loss_curve_path = "files/loss_curve.png"
    #     plot_loss_curve(out, loss_curve_path)
    #     mlflow.log_artifact(loss_curve_path)

    #     test_metrics = evaluate(model, test_data_loader, criterion)
    #     mlflow.log_metric("test_loss", test_metrics["loss"])
    #     mlflow.log_metric("test_accuracy", test_metrics["accuracy"])
    #     mlflow.log_metric("test_macro_f1", test_metrics["macro_f1"])
    #     mlflow.log_metric("test_weighted_f1", test_metrics["weighted_f1"])
        
    #     report = classification_report(
    #         test_metrics["labels"],
    #         test_metrics["preds"],
    #         target_names=["valid_eye", "closed_eye", "hairs", "glasses", "reflection", "blur", "side_eye"],
    #     )

    #     print(report)

    #     report_path = "files/classification_report.txt"
    #     with open(report_path, "w", encoding="utf-8") as f:
    #         f.write(report)
        
    #     mlflow.log_artifact(report_path)
    #     mlflow.pytorch.log_model(model, "model")
                        