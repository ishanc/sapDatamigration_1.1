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

    // === State Transformation Toggle ===
    console.group('Toggle Initialization');
    console.log('Starting toggle initialization...');
    
    const toggleCheckbox = document.querySelector('#toggle-state-rule');
    const toggleStatus = document.querySelector('#toggle-status');
    
    console.log('Toggle elements:', {
        toggleCheckbox: toggleCheckbox,
        toggleStatus: toggleStatus,
        toggleCheckboxExists: !!toggleCheckbox,
        toggleStatusExists: !!toggleStatus
    });

    if (!toggleCheckbox || !toggleStatus) {
        console.error('Toggle elements not found. DOM structure might be incorrect.');
        console.groupEnd();
        return;
    }

    let selectedFiles = [];

    // === File Upload Handlers ===
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

    // === File Processing ===
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

    // === State Transformation Functions ===
    async function checkStateRule() {
        console.group('checkStateRule');
        console.log('Starting state rule check...');
        toggleCheckbox.disabled = true;
        toggleStatus.textContent = 'Checking...';
        
        try {
            console.log('Sending request to check state rule status...');
            const response = await fetch('/state_rule_status', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({})
            });
            
            console.log('Response received:', response);
            const data = await response.json();
            console.log('State rule status data:', data);
            
            if (data.success) {
                toggleCheckbox.checked = data.active;
                toggleStatus.textContent = data.active ? 'Rule is ON' : 'Rule is OFF';
                console.log('Successfully updated toggle state:', {
                    checked: toggleCheckbox.checked,
                    status: toggleStatus.textContent
                });
            } else {
                console.error('Error in response:', data.error);
                toggleStatus.textContent = 'Error checking rule';
                showToast(data.error || 'Failed to check rule status', 'error');
            }
        } catch (e) {
            console.error('Error checking state rule:', e);
            toggleStatus.textContent = 'Error checking rule';
            showToast('Network or server error', 'error');
        } finally {
            toggleCheckbox.disabled = false;
            console.groupEnd();
        }
    }

    async function toggleStateRule(e) {
        console.group('toggleStateRule');
        console.log('Toggle state change event:', e);
        console.log('Current checkbox state:', e.target.checked);
        
        const action = e.target.checked ? 'add' : 'delete';
        console.log('Action to perform:', action);
        
        toggleCheckbox.disabled = true;
        toggleStatus.textContent = 'Processing...';
        
        try {
            console.log('Sending toggle request...');
            const response = await fetch('/toggle_state_transformation', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action })
            });
            
            console.log('Toggle response received:', response);
            const data = await response.json();
            console.log('Toggle response data:', data);
            
            if (data.success) {
                toggleStatus.textContent = e.target.checked ? 'Rule is ON' : 'Rule is OFF';
                showToast(data.message, 'success');
                console.log('Successfully toggled state');
            } else {
                console.error('Error in toggle response:', data.error);
                e.target.checked = !e.target.checked; // Revert the toggle
                toggleStatus.textContent = e.target.checked ? 'Rule is ON' : 'Rule is OFF';
                showToast(data.error || 'Failed to toggle rule', 'error');
            }
        } catch (e) {
            console.error('Error during toggle:', e);
            e.target.checked = !e.target.checked; // Revert the toggle
            toggleStatus.textContent = 'Error';
            showToast('Network or server error', 'error');
        } finally {
            toggleCheckbox.disabled = false;
            console.groupEnd();
        }
    }

    // Initialize toggle functionality
    console.log('Adding event listener to toggle...');
    toggleCheckbox.addEventListener('change', toggleStateRule);
    
    // Initial check of rule status
    console.log('Performing initial state check...');
    checkStateRule();
    
    console.groupEnd();
});