const API_URL = "http://localhost:8000";

const fileInput = document.getElementById("fileInput");
const uploadBtn = document.getElementById("uploadBtn");

const message = document.getElementById("message");
const statusBox = document.getElementById("statusBox");

const taskIdEl = document.getElementById("taskId");
const imageIdEl = document.getElementById("imageId");
const taskStatusEl = document.getElementById("taskStatus");

const previewImage = document.getElementById("previewImage");
const resultImage = document.getElementById("resultImage");

let pollingInterval = null;
let selectedFile = null;

fileInput.addEventListener("change", () => {
  selectedFile = fileInput.files[0];

  resetState();

  if (!selectedFile) {
    return;
  }

  const previewUrl = URL.createObjectURL(selectedFile);
  previewImage.src = previewUrl;
  previewImage.classList.remove("hidden");
});

uploadBtn.addEventListener("click", async () => {
  if (!selectedFile) {
    showMessage("Выбери изображение", true);
    return;
  }

  await uploadImage(selectedFile);
});

function resetState() {
  hideMessage();

  taskIdEl.textContent = "-";
  imageIdEl.textContent = "-";
  taskStatusEl.textContent = "-";

  statusBox.classList.add("hidden");
  resultImage.classList.add("hidden");
  resultImage.src = "";

  if (pollingInterval) {
    clearInterval(pollingInterval);
    pollingInterval = null;
  }
}

async function uploadImage(file) {
  try {
    uploadBtn.disabled = true;
    showMessage("Загружаю изображение...");

    const formData = new FormData();
    formData.append("file", file);

    const response = await fetch(`${API_URL}/api/images/upload`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Ошибка загрузки: ${response.status}. ${text}`);
    }

    const data = await response.json();

    const taskId = data.task_id;
    const imageId = data.image_id;
    const status = data.status;

    taskIdEl.textContent = taskId;
    imageIdEl.textContent = imageId;
    taskStatusEl.textContent = status;
    statusBox.classList.remove("hidden");

    showMessage("Файл загружен. Жду обработки...");

    startPolling(taskId, imageId);
  } catch (error) {
    showMessage(error.message, true);
  } finally {
    uploadBtn.disabled = false;
  }
}

function startPolling(taskId, imageId) {
  if (pollingInterval) {
    clearInterval(pollingInterval);
  }

  pollingInterval = setInterval(async () => {
    try {
      const response = await fetch(`${API_URL}/api/tasks/${taskId}`);

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Ошибка статуса: ${response.status}. ${text}`);
      }

      const data = await response.json();

      taskStatusEl.textContent = data.status;

      if (data.status === "processed") {
        clearInterval(pollingInterval);
        pollingInterval = null;

        showMessage("Обработка завершена");

        const resultUrl = `${API_URL}/api/images/${imageId}/result?t=${Date.now()}`;
        resultImage.src = resultUrl;
        resultImage.classList.remove("hidden");
      }

      if (data.status === "failed") {
        clearInterval(pollingInterval);
        pollingInterval = null;

        showMessage("Обработка завершилась с ошибкой", true);
      }
    } catch (error) {
      clearInterval(pollingInterval);
      pollingInterval = null;

      showMessage(error.message, true);
    }
  }, 2000);
}

function showMessage(text, isError = false) {
  message.textContent = text;
  message.classList.remove("hidden");

  if (isError) {
    message.classList.add("error");
  } else {
    message.classList.remove("error");
  }
}

function hideMessage() {
  message.textContent = "";
  message.classList.add("hidden");
  message.classList.remove("error");
}