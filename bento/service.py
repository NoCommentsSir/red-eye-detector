import bentoml
import onnxruntime
import numpy as np
from PIL import Image
import json

INPUT_NAME = 'input'
OUTPUT_NAME = 'output'

def image_preprocessing(image:Image.Image):
    rgb_image = image.convert('RGB')
    im_arr = np.array(rgb_image, dtype=np.float32) / 255.0
    im_arr = im_arr.transpose(2, 0, 1)
    im_arr = im_arr[np.newaxis, :]
    return im_arr

@bentoml.service(
    resources={"cpu": "2"},
    traffic={"timeout": 10},
)
class RedEyeService:

    def __init__(self) -> None:
        self.validation_session = onnxruntime.InferenceSession('models/onnx/eye_validator.onnx')
        self.segmentation_session = onnxruntime.InferenceSession('models/onnx/red_eye_segmentator.onnx')
        
        self.validation_input_name = self.validation_session.get_inputs()[0].name
        self.validation_output_name = self.validation_session.get_outputs()[0].name

        self.segmentation_input_name = self.validation_session.get_inputs()[0].name
        self.segmentation_output_name = self.validation_session.get_outputs()[0].name

    @bentoml.api
    def health(self) -> dict:
        resp = {
            'status': 'ok'
        }
        return resp
    
    @bentoml.api
    def eye_validation(self, image:Image.Image) -> dict:
        im_arr = image_preprocessing(image)
        ans = self.validation_session.run([self.validation_output_name], {self.validation_input_name: im_arr})[0]
        probs = np.squeeze(ans)
        class_id = int(np.argmax(probs))
        score = float(probs[class_id])
        output = {
            'is_valid': class_id,
            'score': score
        }
        return output
    
    @bentoml.api
    def red_eye_segmentation(self, image:Image.Image) -> Image.Image:
        im_arr = image_preprocessing(image)
        ans = self.segmentation_session.run([self.segmentation_output_name], {self.segmentation_input_name: im_arr})[0]
        mask = np.squeeze(ans)
        if mask.min() < 0 or mask.max() > 1:
            mask = 1 / (1 + np.exp(-mask))

        binary_mask = mask >= 0.2
        mask_img = (binary_mask.astype(np.uint8) * 255)
        return Image.fromarray(mask_img)
    
if __name__ == '__main__':
    image = Image.open('data/processed/clean/00a2b94727fc900f76435e2553431d38e11385cf3a391497bf6e7d5bfcffa189_left.png')
    x = image_preprocessing(image)
    