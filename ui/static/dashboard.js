// ===============================================
// DASHBOARD JAVASCRIPT - SQL Workload RCA
// Enterprise Edition v4.0
// ===============================================

let selectedFiles = [];
let uploadInProgress = false;

// ===============================================
// INITIALIZATION
// ===============================================
async function initializeDashboard() {
    console.log('Initializing Dashboard...');
    
    // Setup file upload handlers
    setupFileUpload();
    
    // Check for existing data
    await checkExistingData();
}

// ===============================================
// FILE UPLOAD SETUP
// ===============================================
function setupFileUpload() {
    const uploadZone = document.getElementById('uploadZone');
    const fileInput = document.getElementById('fileInput');
    
    // Click to browse
    uploadZone.addEventListener('click', (e) => {
        if (e.target.closest('.btn-browse')) return; // Let button handle its own click
        if (!uploadInProgress && selectedFiles.length === 0) {
            fileInput.click();
        }
    });
    
    // Drag and drop
    uploadZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        if (!uploadInProgress) {
            uploadZone.classList.add('dragover');
        }
    });
    
    uploadZone.addEventListener('dragleave', (e) => {
        e.preventDefault();
        uploadZone.classList.remove('dragover');
    });
    
    uploadZone.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadZone.classList.remove('dragover');
        
        if (!uploadInProgress) {
            const files = Array.from(e.dataTransfer.files);
            handleFileSelection(files);
        }
    });
    
    // File input change
    fileInput.addEventListener('change', (e) => {
        if (!uploadInProgress) {
            const files = Array.from(e.target.files);
            handleFileSelection(files);
        }
        e.target.value = ''; // Reset for re-selection
    });
}

// ===============================================
// FILE SELECTION HANDLING
// ===============================================
function handleFileSelection(files) {
    // Filter valid files (HTML with AWR or ASH in name)
    const validFiles = files.filter(file => {
        const name = file.name.toLowerCase();
        return name.endsWith('.html') && (name.includes('awr') || name.includes('ash'));
    });
    
    if (validFiles.length === 0) {
        showUploadStatus('Please select valid AWR or ASH HTML files', 'error');
        return;
    }
    
    // Add to selected files (avoid duplicates)
    validFiles.forEach(file => {
        if (!selectedFiles.find(f => f.name === file.name)) {
            selectedFiles.push(file);
        }
    });
    
    updateFileListUI();
}

function updateFileListUI() {
    const panel = document.getElementById('selectedFilesPanel');
    const list = document.getElementById('filesList');
    const countEl = document.getElementById('fileCount');
    const actions = document.getElementById('uploadActions');
    const zoneContent = document.getElementById('uploadZoneContent');
    const zoneReady = document.getElementById('uploadZoneReady');
    
    if (selectedFiles.length === 0) {
        panel.style.display = 'none';
        actions.style.display = 'none';
        zoneContent.style.display = 'flex';
        zoneReady.style.display = 'none';
        return;
    }
    
    // Update zone state
    zoneContent.style.display = 'none';
    zoneReady.style.display = 'flex';
    
    // Update file count
    countEl.textContent = `${selectedFiles.length} file${selectedFiles.length > 1 ? 's' : ''}`;
    
    // Build file list
    list.innerHTML = selectedFiles.map((file, index) => `
        <li class="file-item">
            <div class="file-info">
                <span class="file-icon">📄</span>
                <span class="file-name">${file.name}</span>
                <span class="file-size">${formatFileSize(file.size)}</span>
            </div>
            <button class="btn-remove" onclick="removeFile(${index})" title="Remove file">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="18" y1="6" x2="6" y2="18"/>
                    <line x1="6" y1="6" x2="18" y2="18"/>
                </svg>
            </button>
        </li>
    `).join('');
    
    panel.style.display = 'block';
    actions.style.display = 'flex';
}

function removeFile(index) {
    selectedFiles.splice(index, 1);
    updateFileListUI();
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

// ===============================================
// FILE UPLOAD
// ===============================================
async function uploadFiles() {
    if (selectedFiles.length === 0 || uploadInProgress) return;
    
    uploadInProgress = true;
    
    const uploadBtn = document.getElementById('uploadBtn');
    
    // Update button state
    uploadBtn.disabled = true;
    uploadBtn.innerHTML = `
        <svg class="spinner" width="20" height="20" viewBox="0 0 24 24">
            <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2" fill="none" stroke-dasharray="60" stroke-dashoffset="20"/>
        </svg>
        Uploading & Parsing...
    `;
    
    // Show status bar
    showUploadProgress('Uploading files...', 20);
    
    const formData = new FormData();
    selectedFiles.forEach(file => formData.append('files', file));
    
    try {
        showUploadProgress('Parsing reports...', 50);
        
        const response = await fetch('/api/upload', {
            method: 'POST',
            credentials: 'include',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showUploadProgress('Processing complete!', 100);
            
            // Store new upload data in sessionStorage for newresults page
            sessionStorage.setItem('newUploadData', JSON.stringify(result));
            
            // Redirect to NEW RESULTS page (not existing results)
            setTimeout(() => {
                window.location.href = '/newresults';
            }, 800);
        } else {
            showUploadStatus(result.detail || result.message || 'Upload failed', 'error');
            resetUploadButton();
        }
    } catch (error) {
        console.error('Upload error:', error);
        showUploadStatus('Network error during upload', 'error');
        resetUploadButton();
    }
}

function resetUploadButton() {
    uploadInProgress = false;
    const uploadBtn = document.getElementById('uploadBtn');
    uploadBtn.disabled = false;
    uploadBtn.innerHTML = `
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
            <polyline points="17,8 12,3 7,8"/>
            <line x1="12" y1="3" x2="12" y2="15"/>
        </svg>
        Upload & Parse Files
    `;
}

function showUploadProgress(message, percent) {
    const statusBar = document.getElementById('uploadStatusBar');
    const progress = document.getElementById('statusProgress');
    const messageEl = document.getElementById('statusMessage');
    
    statusBar.style.display = 'flex';
    progress.style.width = `${percent}%`;
    messageEl.textContent = message;
}

function showUploadStatus(message, type) {
    const statusBar = document.getElementById('uploadStatusBar');
    const messageEl = document.getElementById('statusMessage');
    
    statusBar.style.display = 'flex';
    statusBar.className = `upload-status-bar ${type}`;
    messageEl.textContent = message;
    
    if (type !== 'info') {
        setTimeout(() => {
            statusBar.style.display = 'none';
            statusBar.className = 'upload-status-bar';
        }, 5000);
    }
}

// ===============================================
// CHECK EXISTING DATA
// ===============================================
async function checkExistingData() {
    try {
        const response = await fetch('/api/results', {
            method: 'GET',
            credentials: 'include',
            cache: 'no-store'
        });
        
        if (!response.ok) return;
        
        const result = await response.json();
        
        if (result.has_data) {
            showExistingDataBanner(result);
        }
    } catch (error) {
        console.log('No existing data found');
    }
}

function showExistingDataBanner(result) {
    const banner = document.getElementById('statusBanner');
    const viewResultsLink = document.getElementById('viewResultsLink');
    const quickStats = document.getElementById('quickStatsSection');
    
    // Show banner
    banner.style.display = 'block';
    viewResultsLink.style.display = 'flex';
    
    // Get accurate CSV count from multiple sources
    let csvCount = 0;
    
    // Priority 1: Direct csv_file_list
    if (result.csv_file_list && Array.isArray(result.csv_file_list)) {
        csvCount = result.csv_file_list.length;
    }
    // Priority 2: csv_validation object
    else if (result.csv_validation?.total_csv_files) {
        csvCount = result.csv_validation.total_csv_files;
    }
    // Priority 3: csv_count field
    else if (result.csv_count) {
        csvCount = result.csv_count;
    }
    // Priority 4: Count from parsing_results
    else if (result.parsing_results && Array.isArray(result.parsing_results)) {
        csvCount = result.parsing_results.filter(f => 
            f.file && f.file.endsWith('.csv')
        ).length;
    }
    
    // Update description
    document.getElementById('statusDescription').textContent = 
        `${csvCount} CSV files available for analysis.`;
    
    // Show quick stats - count non-CSV reports processed
    const reportsProcessed = result.parsing_results?.filter(p => 
        p.type !== 'CSV' && !p.file?.endsWith('.csv')
    ).length || result.parsing_results?.length || 0;
    
    document.getElementById('csvFileCount').textContent = csvCount;
    document.getElementById('htmlFileCount').textContent = reportsProcessed;
    document.getElementById('analysisStatus').textContent = result.has_rca_result ? 'Analyzed' : 'Ready';
    quickStats.style.display = 'block';
}

function startNewUpload() {
    // Hide banner
    document.getElementById('statusBanner').style.display = 'none';
    
    // Reset files
    selectedFiles = [];
    updateFileListUI();
    
    // Scroll to upload section
    document.getElementById('uploadSection').scrollIntoView({ behavior: 'smooth' });
}
