import mediapipe as mp
import numpy as np
from PIL import Image

def find_eye_centers(image: Image.Image):
    image_np = np.array(image.convert("RGB"))
    image_rows, image_cols, _ = image_np.shape

    mp_face_detection = mp.solutions.face_detection

    with mp_face_detection.FaceDetection(
        model_selection=1,
        min_detection_confidence=0.5,
    ) as face_detection:
        results = face_detection.process(image_np)

    if not results.detections:
        return None

    detection = results.detections[0]

    left_eye = mp_face_detection.get_key_point(
        detection,
        mp_face_detection.FaceKeyPoint.LEFT_EYE,
    )

    right_eye = mp_face_detection.get_key_point(
        detection,
        mp_face_detection.FaceKeyPoint.RIGHT_EYE,
    )

    return {
        "left": (
            int(left_eye.x * image_cols),
            int(left_eye.y * image_rows),
        ),
        "right": (
            int(right_eye.x * image_cols),
            int(right_eye.y * image_rows),
        ),
    }

if __name__ == '__main__':
    image_path = 'data/celeba/img_align_celeba/img_align_celeba/000002.jpg'
    image = Image.open(image_path)
    print(find_eye_centers(image))
