import torch
import torch.optim as optim
import torch.nn as nn
import torch.nn.functional as F
import mlflow
from sklearn.metrics import f1_score, accuracy_score

def evaluate(model, dataloader, criterion, scheduler=None):
    model.eval()

    total_loss = 0.0
    all_labels = []
    all_preds = []

    with torch.no_grad():
        for images, labels in dataloader:

            outputs = model(images)
            loss = criterion(outputs, labels)

            total_loss += loss.item()

            _, preds = torch.max(outputs, 1)

            all_labels.extend(labels.cpu().numpy())
            all_preds.extend(preds.cpu().numpy())

    avg_loss = total_loss / len(dataloader)
    if scheduler:
        scheduler.step(avg_loss)
    accuracy = accuracy_score(all_labels, all_preds)
    macro_f1 = f1_score(all_labels, all_preds, average="macro")
    weighted_f1 = f1_score(all_labels, all_preds, average="weighted")

    return {
        "loss": avg_loss,
        "accuracy": accuracy,
        "macro_f1": macro_f1,
        "weighted_f1": weighted_f1,
        "labels": all_labels,
        "preds": all_preds,
    }

class LeNet(nn.Module):
    
    def __init__(self):
        super(LeNet, self).__init__()
        self.conv1 = nn.Conv2d(3, 16, 3, padding=1)
        self.bn1 = nn.BatchNorm2d(16)
        self.conv2 = nn.Conv2d(16, 32, 5, padding=2)
        self.bn2 = nn.BatchNorm2d(32)
        self.dense1 = nn.Linear(32 * 32 * 24, 512)
        self.dense2 = nn.Linear(512, 64)
        self.dense3 = nn.Linear(64, 2)

    def forward(self, input):
        c1 = F.relu(self.conv1(input))
        b1 = self.bn1(c1)
        s1 = F.max_pool2d(b1, 2)
        c2 = F.relu(self.conv2(s1))
        b2 = self.bn2(c2)
        s2 = F.max_pool2d(b2, 2)
        f = torch.flatten(s2, 1)
        l1 = F.relu(self.dense1(f))
        l2 = F.relu(self.dense2(l1))
        output = self.dense3(l2)
        return output
    
def fit_model(model:LeNet, trainloader, criterion, validloader, params):
    optimizer = optim.Adam(model.parameters(), params['lr'], params['betas'], params['eps'])
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=3, min_lr=1e-6
    )
    epoch_metrics = {
        "train_loss": [],
        "valid_loss": [],
        "valid_accuracy": [],
        "valid_macro_f1": [],
    }

    for _ in range(params['num_epochs']):
        model.train()
        train_loss = 0.0
        for i, data in enumerate(trainloader, 0):
            input, label = data
            optimizer.zero_grad()
            output = model(input)
            batch_loss = criterion(output, label)
            batch_loss.backward()
            optimizer.step()
            train_loss += batch_loss.item()
        train_loss = train_loss / len(trainloader)
        valid_metrics = evaluate(model, validloader, criterion, scheduler)
        print(f"Step: {optimizer.param_groups[0]['lr']}")
        print(f"Epoch {_+1}, Loss: {train_loss}")
        epoch_metrics["train_loss"].append(train_loss)
        epoch_metrics["valid_loss"].append(valid_metrics["loss"])
        epoch_metrics["valid_accuracy"].append(valid_metrics["accuracy"])
        epoch_metrics["valid_macro_f1"].append(valid_metrics["macro_f1"])
        mlflow.log_metric("train_loss", train_loss, step=_ + 1)
        mlflow.log_metric("valid_loss", valid_metrics["loss"], step= _ + 1)
        mlflow.log_metric("valid_accuracy", valid_metrics["accuracy"], step= _ + 1)
        mlflow.log_metric("valid_macro_f1", valid_metrics["macro_f1"], step= _ + 1)
    print('Finished Training')
    return epoch_metrics

if __name__ == "__main__":
    pass