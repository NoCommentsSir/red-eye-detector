import torch
import torch.optim as optim
import torch.nn as nn
import torch.nn.functional as F
import mlflow
from sklearn.metrics import f1_score, accuracy_score

def evaluate(model, dataloader, criterion1, criterion2):
    model.eval()

    total_loss = 0.0
    total_metrics = {
        "dice": 0.0,
        "iou": 0.0,
        "precision": 0.0,
        "recall": 0.0,
    }

    with torch.no_grad():
        for images, masks in dataloader:
            outputs = model(images)
            loss = criterion1(outputs, masks) + criterion2(outputs, masks)
            precision, recall, dice, iou = segmentation_metrics_from_logits(outputs, masks)
            total_loss += loss.item()
            total_metrics['precision'] += precision
            total_metrics['recall'] += recall
            total_metrics['dice'] += dice
            total_metrics['iou'] += iou

    total_loss /= len(dataloader)
    total_metrics['precision'] /= len(dataloader)
    total_metrics['recall'] /= len(dataloader)
    total_metrics['dice'] /= len(dataloader)
    total_metrics['iou'] /= len(dataloader)
    total_metrics['loss'] = total_loss
    return total_metrics

class DiceLoss(nn.Module):

    def __init__(self, smooth):
        super().__init__()
        self.smooth = smooth

    def __call__(self, pred, target):
        sp = F.sigmoid(pred)
        sp = sp.view(sp.size(0), -1)
        target = target.view(target.size(0), -1)
        part1 = 2 * (sp * target).sum(dim=1)
        part2 = sp.sum(dim=1) + target.sum(dim=1)
        dice = (part1 + self.smooth) / (part2 + self.smooth)
        return 1.0  - dice.mean()

@torch.no_grad()
def segmentation_metrics_from_logits(logits, targets, threshold: float = 0.5, eps: float = 1e-7):
    probs = F.sigmoid(logits)
    preds = (probs > threshold).float()
    preds = preds.view(preds.size(0), -1)
    targets = targets.view(targets.size(0), -1)

    tp = (preds * targets).sum(dim=1)
    fp = (preds * (1 - targets)).sum(dim=1)
    fn = ((1 - preds) * targets).sum(dim=1)

    precision = (tp + eps) / (tp + fp + eps)
    recall = (tp + eps) / (tp + fn + eps)
    dice = (2 * tp + eps) / (2 * tp + fp + fn + eps)
    iou = (tp + eps) / (tp + fp + fn + eps)

    return precision.mean().item(), recall.mean().item(), dice.mean().item(), iou.mean().item()

class UNet(nn.Module):
    
    def __init__(self, n_classes):
        super(UNet, self).__init__()
        # 128 x 96
        self.e11 = nn.Conv2d(3, 32, kernel_size=3, padding=1)   # -> [32, 128, 96]
        self.e12 = nn.Conv2d(32, 64, kernel_size=3, padding=1)  # -> [64, 128, 96]
        self.pool1 = nn.MaxPool2d(kernel_size=2, stride=2)      # -> [64, 64, 48]
        self.e21 = nn.Conv2d(64, 128, kernel_size=3, padding=1) # -> [128, 64, 48]
        self.e22 = nn.Conv2d(128, 128, kernel_size=3, padding=1)# -> [128, 64, 48]
        self.pool2 = nn.MaxPool2d(kernel_size=2, stride=2)      # -> [128, 32, 24]
        self.e31 = nn.Conv2d(128, 256, kernel_size=3, padding=1)# -> [256, 32, 24]
        self.e32 = nn.Conv2d(256, 256, kernel_size=3, padding=1)# -> [256, 32, 24]
        self.pool3 = nn.MaxPool2d(kernel_size=2, stride=2)      # -> [256, 16, 12]
        self.e41 = nn.Conv2d(256, 512, kernel_size=3, padding=1)# -> [512, 16, 12]
        self.e42 = nn.Conv2d(512, 512, kernel_size=3, padding=1)# -> [512, 16, 12]
        self.pool4 = nn.MaxPool2d(kernel_size=2, stride=2)      # -> [512, 8, 6]
        self.e51 = nn.Conv2d(512, 1024, kernel_size=3, padding=1)# -> [1024, 8, 6]
        self.e52 = nn.Conv2d(1024, 1024, kernel_size=3, padding=1)# -> [1024, 8, 6]

        self.upconv1 = nn.ConvTranspose2d(1024, 512, kernel_size=2, stride=2) # -> [512, 16, 12]
        self.d11 = nn.Conv2d(1024, 512, kernel_size=3, padding=1)            # -> [512, 16, 12]
        self.d12 = nn.Conv2d(512, 512, kernel_size=3, padding=1)             # -> [512, 16, 12]
        self.upconv2 = nn.ConvTranspose2d(512, 256, kernel_size=2, stride=2)  # -> [256, 32, 24]
        self.d21 = nn.Conv2d(512, 256, kernel_size=3, padding=1)             # -> [256, 32, 24]
        self.d22 = nn.Conv2d(256, 256, kernel_size=3, padding=1)             # -> [256, 32, 24]
        self.upconv3 = nn.ConvTranspose2d(256, 128, kernel_size=2, stride=2)  # -> [128, 64, 48]
        self.d31 = nn.Conv2d(256, 128, kernel_size=3, padding=1)             # -> [128, 64, 48]
        self.d32 = nn.Conv2d(128, 128, kernel_size=3, padding=1)             # -> [128, 64, 48]
        self.upconv4 = nn.ConvTranspose2d(128, 64, kernel_size=2, stride=2)   # -> [64, 128, 96]
        self.d41 = nn.Conv2d(128, 64, kernel_size=3, padding=1)              # -> [64, 128, 96]
        self.d42 = nn.Conv2d(64, 64, kernel_size=3, padding=1)               # -> [64, 128, 96]

        self.out = nn.Conv2d(64, n_classes, kernel_size=1)                   # -> [n_classes, 128, 96]

    def forward(self, input):
        xe11 = F.relu(self.e11(input))  
        xe12 = F.relu(self.e12(xe11))   
        xp1 = self.pool1(xe12)   
        xe21 = F.relu(self.e21(xp1))  
        xe22 = F.relu(self.e22(xe21)) 
        xp2 = self.pool2(xe22)  
        xe31 = F.relu(self.e31(xp2))  
        xe32 = F.relu(self.e32(xe31))   
        xp3 = self.pool3(xe32)   
        xe41 = F.relu(self.e41(xp3))  
        xe42 = F.relu(self.e42(xe41)) 
        xp4 = self.pool4(xe42)  
        xe51 = F.relu(self.e51(xp4))  
        xe52 = F.relu(self.e52(xe51)) 

        xu1 = self.upconv1(xe52)
        xu11 = torch.cat([xu1, xe42], dim=1)
        xd11 = F.relu(self.d11(xu11)) 
        xd12 = F.relu(self.d12(xd11)) 
        xu2 = self.upconv2(xd12)
        xu22 = torch.cat([xu2, xe32], dim=1)
        xd21 = F.relu(self.d21(xu22)) 
        xd22 = F.relu(self.d22(xd21)) 
        xu3 = self.upconv3(xd22)
        xu33 = torch.cat([xu3, xe22], dim=1)
        xd31 = F.relu(self.d31(xu33)) 
        xd32 = F.relu(self.d32(xd31)) 
        xu4 = self.upconv4(xd32)
        xu44 = torch.cat([xu4, xe12], dim=1)
        xd41 = F.relu(self.d41(xu44)) 
        xd42 = F.relu(self.d42(xd41)) 
        output = self.out(xd42)
        return output
    
def fit_model(model:UNet, trainloader, criterion1, criterion2, validloader, params):
    optimizer = optim.Adam(model.parameters(), params['lr'], params['betas'], params['eps'])
    epoch_metrics = {
        "train_loss": [],
        "valid_loss": [],
        "valid_dice": [],
        "valid_iou": [],
        "valid_precision": [],
        "valid_recall": [],
    }

    for _ in range(params['num_epochs']):
        model.train()
        train_loss = 0.0
        for i, data in enumerate(trainloader, 0):
            inputs, masks = data
            optimizer.zero_grad()
            output = model(inputs)
            batch_loss = criterion1(output, masks) + criterion2(output, masks)
            batch_loss.backward()
            optimizer.step()
            train_loss += batch_loss.item()
        train_loss = train_loss / len(trainloader)
        valid_metrics = evaluate(model, validloader, criterion1, criterion2)
        print(
            f"_ {_ + 1}/{params['num_epochs']} | "
            f"train_loss={train_loss:.4f} | "
        )
        epoch_metrics["train_loss"].append(train_loss)
        epoch_metrics["valid_loss"].append(valid_metrics["loss"])
        epoch_metrics["valid_dice"].append(valid_metrics["dice"])
        epoch_metrics["valid_iou"].append(valid_metrics["iou"])
        epoch_metrics["valid_precision"].append(valid_metrics["precision"])
        epoch_metrics["valid_recall"].append(valid_metrics["recall"])
        mlflow.log_metric("train_loss", train_loss, step=_ + 1)
        mlflow.log_metric("valid_loss", valid_metrics["loss"], step=_ + 1)
        mlflow.log_metric("valid_dice", valid_metrics["dice"], step=_ + 1)
        mlflow.log_metric("valid_iou", valid_metrics["iou"], step=_ + 1)
        mlflow.log_metric("valid_precision", valid_metrics["precision"], step=_ + 1)
        mlflow.log_metric("valid_recall", valid_metrics["recall"], step=_ + 1)
    print('Finished Training')
    return epoch_metrics

if __name__ == "__main__":
    pass