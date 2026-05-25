import numpy as np
import onnxruntime as ort


def check_model(path: str, input_shape: tuple[int, int, int, int]):
    print(f"Checking ONNX model: {path}")

    session = ort.InferenceSession(path, providers=["CPUExecutionProvider"])

    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name

    x = np.random.randn(*input_shape).astype(np.float32)

    output = session.run([output_name], {input_name: x})[0]

    print("Input name:", input_name)
    print("Output name:", output_name)
    print("Input shape:", x.shape)
    print("Output shape:", output.shape)
    print("Output dtype:", output.dtype)
    print("-" * 50)


def main():
    check_model(
        "models/onnx/eye_validator.onnx",
        (1, 3, 96, 128),
    )

    check_model(
        "models/onnx/red_eye_segmentator.onnx",
        (1, 3, 96, 128),
    )


if __name__ == "__main__":
    main()