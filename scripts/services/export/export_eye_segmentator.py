import mlflow
import onnx, onnxruntime
import torch

MLFLOW_HOST = "http://localhost:5000"
RUN_ID = "037243b78d0949eb86b8861487f25bb2"
MODEL_PATH = "model"
OUTPUT_PATH = "models/onnx/red_eye_segmentator.onnx"
INPUT_SHAPE = (1, 3, 96, 128)

def main():
    mlflow.set_tracking_uri(MLFLOW_HOST)
    model_uri = f'runs:/{RUN_ID}/{MODEL_PATH}'
    model = mlflow.pytorch.load_model(model_uri)
    model.eval()
    dummy_input = torch.randn(INPUT_SHAPE)

    torch.onnx.export(model, 
                  dummy_input, 
                  OUTPUT_PATH, 
                  export_params=True, 
                  opset_version=11,
                  do_constant_folding=True, 
                  input_names=['input'],
                  output_names=['output']) 
    
    onnx_model = onnx.load(OUTPUT_PATH)
    onnx.checker.check_model(onnx_model)

    print("Модель успешно экспортирована в формат ONNX!")

if __name__ == '__main__':
    main()

