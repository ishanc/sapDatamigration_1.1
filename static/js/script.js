document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const fileList = document.getElementById('selected-files');
    const uploadButton = document.getElementById('upload-button');
    const progressSection = document.getElementById('progress-section');
    const progressBar = document.getElementById('upload-progress');
    const progressStatus = document.getElementById('progress-status');
    const resultSection = document.getElementById('result-section');
    const processSummary = document.getElementById('process-summary');
    const downloadButton = document.getElementById('download-button');
    const toast = document.getElementById('toast');
    const resetButton = document.getElementById('reset-button');

    let selectedFiles = [];

    // Drag and drop handlers
    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        const files = Array.from(e.dataTransfer.files).filter(file => file.name.toLowerCase().endsWith('.csv'));
        handleFiles(files);
    });

    // Click to upload
    dropZone.addEventListener('click', () => {
        fileInput.click();
    });

    fileInput.addEventListener('change', (e) => {
        const files = Array.from(e.target.files).filter(file => file.name.toLowerCase().endsWith('.csv'));
        handleFiles(files);
    });

    function handleFiles(files) {
        selectedFiles = [...files];
        updateFileList();
    }

    function updateFileList() {
        fileList.innerHTML = '';
        selectedFiles.forEach(file => {
            const li = document.createElement('li');
            li.innerHTML = `
                ${file.name}
                <button onclick="removeFile('${file.name}')" class="remove-file">×</button>
            `;
            fileList.appendChild(li);
        });
        uploadButton.style.display = selectedFiles.length ? 'flex' : 'none';
    }

    window.removeFile = (fileName) => {
        selectedFiles = selectedFiles.filter(file => file.name !== fileName);
        updateFileList();
    };

    uploadButton.addEventListener('click', async () => {
        if (!selectedFiles.length) return;

        const formData = new FormData();
        selectedFiles.forEach(file => {
            formData.append('files[]', file);
        });

        progressSection.style.display = 'block';
        uploadButton.disabled = true;
        progressBar.style.width = '0%';
        progressStatus.textContent = 'Uploading files...';

        try {
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                throw new Error('Upload failed');
            }

            const responseData = await response.json();
            
            if (responseData.error) {
                throw new Error(responseData.error);
            }
            
            const result = responseData.result;
            
            progressBar.style.width = '100%';
            progressStatus.textContent = 'Processing complete!';
            
            // Show results
            resultSection.style.display = 'block';
            processSummary.innerHTML = `
                <p>Successfully processed ${result.records} records.</p>
                <p>Output columns: ${result.columns.join(', ')}</p>
            `;
            
            // Setup download button with the correct URL
            downloadButton.onclick = () => {
                window.location.href = result.download_url;
            };
            downloadButton.style.display = 'block';

            showToast('Files processed successfully!', 'success');
        } catch (error) {
            console.error('Upload error:', error);
            progressStatus.textContent = 'Upload failed';
            showToast(error.message || 'Error processing files. Please try again.', 'error');
            resultSection.style.display = 'none';
        } finally {
            uploadButton.disabled = false;
        }
    });

    function showToast(message, type = 'success') {
        toast.textContent = message;
        toast.className = `toast ${type}`;
        toast.style.display = 'block';
        
        setTimeout(() => {
            toast.style.display = 'none';
        }, 3000);
    }

    function resetForm() {
        // Reset selected files
        selectedFiles = [];
        updateFileList();
        
        // Reset file input
        fileInput.value = '';
        
        // Hide sections and reset progress
        progressSection.style.display = 'none';
        resultSection.style.display = 'none';
        progressBar.style.width = '0%';
        progressStatus.textContent = '';
        
        // Enable upload button
        uploadButton.disabled = false;
        
        showToast('Form reset successfully', 'success');
    }
    
    resetButton.addEventListener('click', resetForm);

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