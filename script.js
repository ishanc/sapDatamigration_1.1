document.addEventListener("DOMContentLoaded", () => {
    // File Upload Logic
    const uploadButton = document.getElementById("upload-button");
    const fileInput = document.getElementById("file-input");
    const uploadStatus = document.getElementById("upload-status");

    uploadButton.addEventListener("click", () => {
        const files = fileInput.files;
        if (files.length === 0) {
            uploadStatus.textContent = "Please select files to upload.";
            return;
        }

        uploadStatus.textContent = "Uploading files...";

        // Simulate file upload
        setTimeout(() => {
            uploadStatus.textContent = "Files uploaded successfully.";
        }, 1000);
    });

    // Chat Interface Logic
    const sendButton = document.getElementById("send-button");
    const userMessage = document.getElementById("user-message");
    const chatHistory = document.getElementById("chat-history");

    sendButton.addEventListener("click", () => {
        const message = userMessage.value.trim();
        if (message === "") {
            return;
        }

        // Add user message to chat history
        const userMessageElement = document.createElement("div");
        userMessageElement.textContent = `You: ${message}`;
        chatHistory.appendChild(userMessageElement);

        // Simulate LLM response
        const botMessageElement = document.createElement("div");
        botMessageElement.textContent = "Bot: Processing your request...";
        chatHistory.appendChild(botMessageElement);

        // Clear input field
        userMessage.value = "";

        // Simulate delay for bot response
        setTimeout(() => {
            botMessageElement.textContent = "Bot: Here is the response to your query.";
        }, 2000);
    });
});