// -------------------------------
// FILE UPLOAD FUNCTIONALITY
// -------------------------------

// Global copy utility
function copyToClipboard(text, btnElement) {
  navigator.clipboard
    .writeText(text)
    .then(() => {
      const original = btnElement.innerHTML;
      btnElement.innerHTML = "✓ Copied";
      btnElement.classList.add("copied");
      setTimeout(() => {
        btnElement.innerHTML = original;
        btnElement.classList.remove("copied");
      }, 2000);
    })
    .catch(() => {
      // Fallback for older browsers
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      const original = btnElement.innerHTML;
      btnElement.innerHTML = "✓ Copied";
      btnElement.classList.add("copied");
      setTimeout(() => {
        btnElement.innerHTML = original;
        btnElement.classList.remove("copied");
      }, 2000);
    });
}

// Wrap any pre/code block with a copy button
function addCopyButtons() {
  document
    .querySelectorAll(
      ".fix-card pre, .sql-code, .code-block, .recommendation-section pre",
    )
    .forEach((el) => {
      if (el.parentElement.classList.contains("copy-wrapper")) return;
      const wrapper = document.createElement("div");
      wrapper.className = "copy-wrapper";
      el.parentNode.insertBefore(wrapper, el);
      wrapper.appendChild(el);
      const btn = document.createElement("button");
      btn.className = "btn-copy";
      btn.innerHTML = "📋 Copy";
      btn.onclick = () => copyToClipboard(el.textContent, btn);
      wrapper.appendChild(btn);
    });
}

let selectedFiles = [];
let uploadInProgress = false;
let uploadFlowStarted = false;
let sessionUploadCompleted = false;
let detectedTimeWindow = null; // Auto-detected time window from AWR report

function initializeFileUpload() {
  const uploadArea = document.getElementById("uploadArea");
  const fileInput = document.getElementById("fileInput");
  const browseLink =
    document.querySelector(".duz-browse") ||
    document.querySelector(".browse-link");

  // Click to browse
  browseLink.addEventListener("click", () => {
    if (!uploadFlowStarted && !sessionUploadCompleted) {
      fileInput.click();
    }
  });

  uploadArea.addEventListener("click", () => {
    if (!uploadFlowStarted && !sessionUploadCompleted) {
      fileInput.click();
    }
  });

  // Drag and drop
  uploadArea.addEventListener("dragover", (e) => {
    e.preventDefault();
    if (!uploadFlowStarted && !sessionUploadCompleted) {
      uploadArea.classList.add("dragover");
    }
  });

  uploadArea.addEventListener("dragleave", () => {
    if (!uploadFlowStarted && !sessionUploadCompleted) {
      uploadArea.classList.remove("dragover");
    }
  });

  uploadArea.addEventListener("drop", (e) => {
    e.preventDefault();
    uploadArea.classList.remove("dragover");

    if (!uploadFlowStarted && !sessionUploadCompleted) {
      const files = Array.from(e.dataTransfer.files);
      handleFileSelection(files);
    }
  });

  fileInput.addEventListener("change", (e) => {
    if (!uploadFlowStarted && !sessionUploadCompleted) {
      const files = Array.from(e.target.files);
      handleFileSelection(files);
    }
    // Clear the input to allow same file selection if needed
    e.target.value = "";
  });
}

function handleFileSelection(files) {
  // Prevent file selection if session upload already completed
  if (sessionUploadCompleted) {
    showUploadStatus(
      "Upload already completed in this session. Please refresh the page.",
      "error",
    );
    return;
  }

  const validFiles = files.filter(
    (file) =>
      file.name.toLowerCase().endsWith(".html") &&
      (file.name.toLowerCase().includes("awr") ||
        file.name.toLowerCase().includes("ash")),
  );

  if (validFiles.length === 0) {
    showUploadStatus("Please select valid AWR or ASH HTML files", "error");
    return;
  }

  // Mark that upload flow has started
  uploadFlowStarted = true;

  selectedFiles = validFiles;
  displaySelectedFiles();

  document.getElementById("fileList").style.display = "block";
  document.getElementById("uploadBtn").style.display = "block";

  // Update UI to show upload is in progress
  updateUploadAreaState();
}

function displaySelectedFiles() {
  const filesList = document.getElementById("selectedFiles");
  filesList.innerHTML = "";

  selectedFiles.forEach((file, index) => {
    const li = document.createElement("li");
    li.className = "file-item";
    li.innerHTML = `
            <div class="file-info">
              <span class="file-icon">📄</span>
              <span class="file-name">${file.name}</span>
              <span class="file-size">${(file.size / 1024).toFixed(1)} KB</span>
            </div>
            <button class="btn-remove" onclick="removeFile(${index})" title="Remove">✕</button>
        `;
    filesList.appendChild(li);
  });
}

function removeFile(index) {
  if (sessionUploadCompleted) {
    showUploadStatus(
      "Cannot modify files - upload already completed in this session.",
      "error",
    );
    return;
  }

  selectedFiles.splice(index, 1);
  displaySelectedFiles();

  if (selectedFiles.length === 0) {
    document.getElementById("fileList").style.display = "none";
    document.getElementById("uploadBtn").style.display = "none";
  }
}

function updateUploadAreaState() {
  const uploadArea = document.getElementById("uploadArea");

  if (uploadFlowStarted) {
    // Update UI to show files are selected and ready for upload
    uploadArea.innerHTML = `
            <div class="duz-visual">
              <div class="duz-circle" style="background:linear-gradient(135deg,rgba(16,185,129,0.15),rgba(52,211,153,0.15));border-color:rgba(16,185,129,0.2);">
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#34d399" stroke-width="1.5"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22,4 12,14.01 9,11.01"/></svg>
              </div>
              <h3 style="color:#34d399;">Files Selected & Ready</h3>
              <p>Click "Upload & Parse Files" below to proceed</p>
            </div>
        `;
    uploadArea.style.cursor = "default";
    uploadArea.style.opacity = "0.8";
  }
}

async function uploadFiles() {
  if (sessionUploadCompleted) {
    showUploadStatus(
      "Upload already completed in this session. Please refresh the page.",
      "error",
    );
    return;
  }

  if (selectedFiles.length === 0) {
    showUploadStatus("No files selected", "error");
    return;
  }

  if (uploadInProgress) {
    showUploadStatus("Upload already in progress...", "info");
    return;
  }

  uploadInProgress = true;

  // Disable upload button during upload
  const uploadBtn = document.getElementById("uploadBtn");
  const originalText = uploadBtn.textContent;
  uploadBtn.disabled = true;
  uploadBtn.textContent = "Uploading...";

  const formData = new FormData();
  selectedFiles.forEach((file) => {
    formData.append("files", file);
  });

  showUploadStatus("Uploading and parsing files...", "info");

  try {
    const response = await fetch("/api/upload", {
      method: "POST",
      credentials: "same-origin", // Include cookies in request
      body: formData,
    });

    const result = await response.json();

    if (response.ok) {
      showUploadStatus("Files uploaded and parsed successfully!", "success");
      displayParsingResults(result);

      // Mark session as having completed upload
      sessionUploadCompleted = true;

      // Hide upload section after successful upload
      document.getElementById("uploadSection").style.display = "none";

      // Do NOT reset upload flow - keep session completed state
    } else {
      showUploadStatus(result.detail || "Upload failed", "error");
      // Re-enable upload on error
      uploadBtn.disabled = false;
      uploadBtn.textContent = originalText;
      uploadInProgress = false;
    }
  } catch (error) {
    showUploadStatus("Network error during upload", "error");
    // Re-enable upload on error
    uploadBtn.disabled = false;
    uploadBtn.textContent = originalText;
    uploadInProgress = false;
  }
}

function resetUploadFlow() {
  uploadInProgress = false;
  uploadFlowStarted = false;
  selectedFiles = [];

  // Only reset upload area if session not completed
  if (!sessionUploadCompleted) {
    // Reset upload area to original state
    const uploadArea = document.getElementById("uploadArea");
    uploadArea.innerHTML = `
            <div class="duz-visual">
              <div class="duz-circle">
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17,8 12,3 7,8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
              </div>
              <h3>Drag & drop files here</h3>
              <p>or <span class="duz-browse">browse your computer</span></p>
              <div class="duz-formats">
                <span class="duz-format-chip">.html</span>
                <span class="duz-format-chip">AWR Reports</span>
                <span class="duz-format-chip">ASH Reports</span>
              </div>
            </div>
            <input type="file" id="fileInput" multiple accept=".html" style="display:none" />
        `;
    uploadArea.style.cursor = "pointer";
    uploadArea.style.opacity = "1";

    // Re-attach browse link event
    const browseLink =
      document.querySelector(".duz-browse") ||
      document.querySelector(".browse-link");
    browseLink.addEventListener("click", () => {
      if (!uploadFlowStarted && !sessionUploadCompleted) {
        document.getElementById("fileInput").click();
      }
    });
  } else {
    // Show session completed state
    const uploadArea = document.getElementById("uploadArea");
    uploadArea.innerHTML = `
            <div class="duz-visual">
              <div class="duz-circle" style="background:linear-gradient(135deg,rgba(16,185,129,0.15),rgba(52,211,153,0.15));border-color:rgba(16,185,129,0.2);">
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#34d399" stroke-width="1.5"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22,4 12,14.01 9,11.01"/></svg>
              </div>
              <h3 style="color:#34d399;">Upload Completed</h3>
              <p>Refresh page to upload new files</p>
            </div>
        `;
    uploadArea.style.cursor = "default";
    uploadArea.style.opacity = "0.6";
  }
}

function showUploadStatus(message, type) {
  const statusDiv = document.getElementById("uploadStatus");
  statusDiv.textContent = message;
  statusDiv.className = `upload-status ${type}`;
  statusDiv.style.display = "block";

  if (type !== "info") {
    setTimeout(() => {
      statusDiv.style.display = "none";
    }, 5000);
  }
}

function displayParsingResults(result) {
  const parsingStatus = document.getElementById("parsingStatus");
  const highLoadSection = document.getElementById("highLoadSection");
  const highLoadResults = document.getElementById("highLoadResults");

  // Display the comprehensive summary if available
  if (result.summary) {
    console.log("📋 UPLOAD SUMMARY:\n" + result.summary);
  }

  // Show parsing status with detailed results
  let statusHTML = `
        <div class="parsing-success">
            <h3 style="color: #10b981; margin-bottom: 1rem;">✅ Multi-File Upload & Parsing Complete</h3>
            <div class="parsing-details">
                <p><strong>Files Processed:</strong></p>
                <ul>`;

  result.parsing_results.forEach((file) => {
    const statusIcon = file.status === "success" ? "✅" : "❌";
    const statusColor = file.status === "success" ? "#10b981" : "#ef4444";
    const csvInfo = file.csv_files > 0 ? ` → ${file.csv_files} CSV files` : "";
    statusHTML += `<li style="color: ${statusColor}">${statusIcon} ${file.file} (${file.type})${csvInfo}</li>`;
  });

  statusHTML += `
                </ul>
                <p><strong>Total CSV Files in System:</strong> ${result.total_csv_files ?? result.parsed_csv_count ?? result.csv_count ?? 0}</p>
                <p><strong>New CSV Files Generated:</strong> ${result.new_csv_files_generated ?? result.new_generated_csv ?? 0}</p>`;

  // Show summary in a collapsible section
  if (result.summary) {
    statusHTML += `
                <details style="margin-top: 1rem; padding: 0.5rem; background: #f8f9fa; border-radius: 4px; color: #000000;">
                    <summary style="cursor: pointer; font-weight: bold; color: #000000;">📋 Detailed Summary</summary>
                    <pre style="margin-top: 0.5rem; font-family: monospace; white-space: pre-wrap; color: #000000;">${result.summary}</pre>
                </details>`;
  }

  statusHTML += `
            </div>
        </div>
    `;

  parsingStatus.innerHTML = statusHTML;

  // Show high load detection results (AWR only)
  if (result.high_load_periods && result.high_load_periods.length > 0) {
    let highLoadHTML = "";

    // Filter to show only AWR-based high load detection, exclude ASH
    const awrHighLoadPeriods = result.high_load_periods.filter((period) => {
      return (
        period.details && !period.details.toLowerCase().includes("ash analysis")
      );
    });

    awrHighLoadPeriods.forEach((period, index) => {
      const severityColor = "#ef4444"; // Always red for detected high load

      // Extract metrics from period if available
      const metrics = period.metrics || {};
      const detailsStr = period.details || "";

      highLoadHTML += `
                <div class="high-load-period" style="border-left: 4px solid ${severityColor}; padding: 1rem; margin: 0.5rem 0; background: #f8f9fa; color: #1f2937;">
                    <div style="font-weight: bold; color: ${severityColor};">
                        🔺 High Load Detected: ${period.period}
                    </div>
                    <div style="margin-top: 0.5rem; font-size: 0.9rem; color: #374151;">
                        ${formatHighLoadMetricsForDashboard(metrics, detailsStr)}
                    </div>
                </div>
            `;

      // Use the first AWR period's time window for RCA
      if (index === 0 && period.period) {
        setDetectedTimeWindow(period.period);
      }
    });

    if (highLoadHTML) {
      highLoadResults.innerHTML = highLoadHTML;
      highLoadSection.style.display = "block";
    } else {
      // No AWR high load detected - still try to get time window from any period
      if (
        result.high_load_periods.length > 0 &&
        result.high_load_periods[0].period
      ) {
        setDetectedTimeWindow(result.high_load_periods[0].period);
      }
      highLoadResults.innerHTML = `
                <div style="padding: 1rem; background: #f0f9ff; border: 1px solid #10b981; border-radius: 8px; color: #1f2937;">
                    🟢 High Load Detected: No High Load Detected
                </div>
            `;
      highLoadSection.style.display = "block";
    }
  } else {
    highLoadResults.innerHTML = `
            <div style="padding: 1rem; background: #f0f9ff; border: 1px solid #10b981; border-radius: 8px; color: #1f2937;">
                🟢 High Load Detected: No High Load Detected
            </div>
        `;
    highLoadSection.style.display = "block";
  }

  parsingSection.style.display = "block";

  // Load available time range after parsing
  loadAvailableTimeRange();
}

/**
 * Format High Load metrics for dashboard display.
 * Shows 3 metrics: elapsed time, executions, CPU%
 */
function formatHighLoadMetricsForDashboard(metrics, detailsStr) {
  // Try to get values from metrics object first, then parse from details string
  let elapsedTime = metrics.total_elapsed_time_s || 0;
  let executions = metrics.total_executions || 0;
  let cpuPercent = metrics.cpu_percentage || 0;

  // Parse from details string if metrics not available
  if (detailsStr) {
    const elapsedMatch = detailsStr.match(/Total elapsed time:\s*([\d.]+)s/i);
    if (elapsedMatch && elapsedTime === 0)
      elapsedTime = parseFloat(elapsedMatch[1]);

    const execMatch = detailsStr.match(/Total executions:\s*([\d,]+)/i);
    if (execMatch && executions === 0)
      executions = parseInt(execMatch[1].replace(/,/g, ""));

    const cpuMatch = detailsStr.match(/(?:Max )?CPU usage:\s*([\d.]+)%/i);
    if (cpuMatch && cpuPercent === 0) cpuPercent = parseFloat(cpuMatch[1]);
  }

  // CRITICAL: Cap CPU at 100%
  cpuPercent = Math.min(100.0, cpuPercent);

  // Format for display - show 3 metrics
  return `
        <span style="font-weight: 500;">AWR Analysis:</span><br>
        <span style="margin-left: 1rem;">• Total elapsed time: ${elapsedTime.toFixed(1)}s</span><br>
        <span style="margin-left: 1rem;">• Total executions: ${executions.toLocaleString()}</span><br>
        <span style="margin-left: 1rem;">• CPU Usage: ${cpuPercent.toFixed(1)}%</span>
    `;
}

// -------------------------------
// LOAD AVAILABLE TIME RANGE
// -------------------------------
async function loadAvailableTimeRange() {
  // Time window is now automatically detected from AWR high load periods
  // Just show the time window section if we have a detected time window
  if (detectedTimeWindow) {
    document.getElementById("timeWindowSection").style.display = "block";
  }
}

/**
 * Parse and store the detected time window from AWR report
 * Format expected: "9:30 AM - 10:30 AM" or similar
 */
function setDetectedTimeWindow(periodStr) {
  detectedTimeWindow = parseTimeWindow(periodStr);

  // Update the UI display
  const displayEl = document.getElementById("detectedTimeWindowDisplay");
  if (displayEl) {
    displayEl.textContent = periodStr;
  }

  // Enable the RCA button if we have a valid time window
  const rcaBtn = document.getElementById("runRcaBtn");
  if (rcaBtn && detectedTimeWindow) {
    rcaBtn.disabled = false;
  }

  // Show the time window section
  const timeWindowSection = document.getElementById("timeWindowSection");
  if (timeWindowSection) {
    timeWindowSection.style.display = "block";
  }
}

/**
 * Parse time window string like "9:30 AM - 10:30 AM" into structured data
 */
function parseTimeWindow(periodStr) {
  if (!periodStr) return null;

  // Match pattern like "9:30 AM - 10:30 AM" or "09:30 AM - 10:30 AM"
  const match = periodStr.match(
    /(\d{1,2}):(\d{2})\s*(AM|PM)\s*[-–]\s*(\d{1,2}):(\d{2})\s*(AM|PM)/i,
  );

  if (!match) {
    console.warn("Could not parse time window:", periodStr);
    return null;
  }

  const [, startHour, startMinute, startAmPm, endHour, endMinute, endAmPm] =
    match;

  // Convert to 24-hour format
  let startHour24 = parseInt(startHour);
  if (startAmPm.toUpperCase() === "PM" && startHour24 !== 12) startHour24 += 12;
  if (startAmPm.toUpperCase() === "AM" && startHour24 === 12) startHour24 = 0;

  let endHour24 = parseInt(endHour);
  if (endAmPm.toUpperCase() === "PM" && endHour24 !== 12) endHour24 += 12;
  if (endAmPm.toUpperCase() === "AM" && endHour24 === 12) endHour24 = 0;

  return {
    start_hour: startHour24,
    start_minute: parseInt(startMinute),
    end_hour: endHour24,
    end_minute: parseInt(endMinute),
    start_formatted: `${startHour}:${startMinute.padStart(2, "0")} ${startAmPm.toUpperCase()}`,
    end_formatted: `${endHour}:${endMinute.padStart(2, "0")} ${endAmPm.toUpperCase()}`,
    time_window: periodStr,
  };
}

function initializeTimePickers(timeRange) {
  console.log("Initializing time pickers...");

  // Temporarily make time window section visible to ensure DOM elements are accessible
  const timeWindowSection = document.getElementById("timeWindowSection");
  const wasHidden =
    timeWindowSection && timeWindowSection.style.display === "none";
  if (wasHidden) {
    timeWindowSection.style.display = "block";
  }

  // Initialize time picker dropdowns
  populateHourSelectors();
  populateMinuteSelectors();

  // Restore original visibility state
  if (wasHidden) {
    timeWindowSection.style.display = "none";
  }

  // Display available time range - REMOVED per user request
  // displayTimeRangeInfo(timeRange);

  // Set default values if available
  if (timeRange) {
    setDefaultTimeValues(timeRange);
  }

  // Add event listeners for validation
  const timeInputs = [
    "startHour",
    "startMinute",
    "startAmPm",
    "endHour",
    "endMinute",
    "endAmPm",
  ];
  timeInputs.forEach((inputId) => {
    const element = document.getElementById(inputId);
    if (element) {
      element.addEventListener("change", validateTimeSelection);
    }
  });

  console.log("Time pickers initialized successfully");
}

// Test function for debugging time pickers - can be called from browser console
function testTimePickers() {
  console.log("=== TESTING TIME PICKERS ===");

  const timeWindowSection = document.getElementById("timeWindowSection");
  console.log("Time window section:", timeWindowSection);
  console.log(
    "Time window section display:",
    timeWindowSection ? timeWindowSection.style.display : "not found",
  );

  // Make section visible for testing
  if (timeWindowSection) {
    timeWindowSection.style.display = "block";
  }

  const elements = {
    startHour: document.getElementById("startHour"),
    startMinute: document.getElementById("startMinute"),
    startAmPm: document.getElementById("startAmPm"),
    endHour: document.getElementById("endHour"),
    endMinute: document.getElementById("endMinute"),
    endAmPm: document.getElementById("endAmPm"),
  };

  console.log("Time picker elements:", elements);

  // Test population
  populateHourSelectors();
  populateMinuteSelectors();

  // Check results
  Object.keys(elements).forEach((key) => {
    const elem = elements[key];
    if (elem) {
      console.log(`${key}: ${elem.children.length} options`);
    } else {
      console.log(`${key}: element not found!`);
    }
  });

  console.log("=== TEST COMPLETE ===");
}

function populateHourSelectors() {
  console.log("Populating hour selectors...");
  const startHour = document.getElementById("startHour");
  const endHour = document.getElementById("endHour");

  console.log("startHour element:", startHour);
  console.log("endHour element:", endHour);

  if (!startHour || !endHour) {
    console.error("Hour selector elements not found!");
    return;
  }

  // Clear and populate hours (1-12)
  [startHour, endHour].forEach((select, index) => {
    console.log(`Populating ${index === 0 ? "start" : "end"} hour selector`);
    select.innerHTML = '<option value="">--</option>';
    for (let i = 1; i <= 12; i++) {
      const option = document.createElement("option");
      option.value = i.toString().padStart(2, "0");
      option.textContent = i.toString();
      select.appendChild(option);
    }
    console.log(
      `${index === 0 ? "Start" : "End"} hour selector populated with ${select.children.length} options`,
    );
  });
}

function populateMinuteSelectors() {
  console.log("Populating minute selectors...");
  const startMinute = document.getElementById("startMinute");
  const endMinute = document.getElementById("endMinute");

  console.log("startMinute element:", startMinute);
  console.log("endMinute element:", endMinute);

  if (!startMinute || !endMinute) {
    console.error("Minute selector elements not found!");
    return;
  }

  // Clear and populate minutes (00-59)
  [startMinute, endMinute].forEach((select, index) => {
    console.log(`Populating ${index === 0 ? "start" : "end"} minute selector`);
    select.innerHTML = '<option value="">--</option>';
    for (let i = 0; i < 60; i += 5) {
      // 5-minute intervals
      const option = document.createElement("option");
      const value = i.toString().padStart(2, "0");
      option.value = value;
      option.textContent = value;
      select.appendChild(option);
    }
    console.log(
      `${index === 0 ? "Start" : "End"} minute selector populated with ${select.children.length} options`,
    );
  });
}

function displayTimeRangeInfo(timeRange) {
  const infoDiv = document.getElementById("timeRangeInfo");
  if (timeRange && (timeRange.awr || timeRange.ash)) {
    let info = "<strong>Available time ranges:</strong><br>";
    if (timeRange.awr) {
      const start = new Date(timeRange.awr.begin).toLocaleString();
      const end = new Date(timeRange.awr.end).toLocaleString();
      info += `📊 AWR: ${start} - ${end}<br>`;
    }
    if (timeRange.ash) {
      const start = new Date(timeRange.ash.begin).toLocaleString();
      const end = new Date(timeRange.ash.end).toLocaleString();
      info += `🔥 ASH: ${start} - ${end}`;
    }
    infoDiv.innerHTML = info;
  } else {
    infoDiv.innerHTML =
      "<small>Time range information will be available after parsing</small>";
  }
}

function setDefaultTimeValues(timeRange) {
  // Set reasonable defaults based on available data
  let defaultStart, defaultEnd;

  if (timeRange.awr) {
    defaultStart = new Date(timeRange.awr.begin);
    defaultEnd = new Date(timeRange.awr.end);
  } else if (timeRange.ash) {
    defaultStart = new Date(timeRange.ash.begin);
    defaultEnd = new Date(timeRange.ash.end);
  }

  if (defaultStart && defaultEnd) {
    // Set start time
    const startHour12 = defaultStart.getHours() % 12 || 12;
    const startAmPm = defaultStart.getHours() >= 12 ? "PM" : "AM";
    const startMinute = Math.floor(defaultStart.getMinutes() / 5) * 5;

    document.getElementById("startHour").value = startHour12
      .toString()
      .padStart(2, "0");
    document.getElementById("startMinute").value = startMinute
      .toString()
      .padStart(2, "0");
    document.getElementById("startAmPm").value = startAmPm;

    // Set end time
    const endHour12 = defaultEnd.getHours() % 12 || 12;
    const endAmPm = defaultEnd.getHours() >= 12 ? "PM" : "AM";
    const endMinute = Math.floor(defaultEnd.getMinutes() / 5) * 5;

    document.getElementById("endHour").value = endHour12
      .toString()
      .padStart(2, "0");
    document.getElementById("endMinute").value = endMinute
      .toString()
      .padStart(2, "0");
    document.getElementById("endAmPm").value = endAmPm;
  }
}

function validateTimeSelection() {
  const startHour = document.getElementById("startHour").value;
  const startMinute = document.getElementById("startMinute").value;
  const startAmPm = document.getElementById("startAmPm").value;

  const endHour = document.getElementById("endHour").value;
  const endMinute = document.getElementById("endMinute").value;
  const endAmPm = document.getElementById("endAmPm").value;

  const validationDiv = document.getElementById("timeValidation");
  const rcaBtn = document.getElementById("runRcaBtn");

  // Check if all fields are filled
  if (
    !startHour ||
    !startMinute ||
    !startAmPm ||
    !endHour ||
    !endMinute ||
    !endAmPm
  ) {
    rcaBtn.disabled = true;
    validationDiv.style.display = "none";
    return;
  }

  // Convert to Date objects for comparison
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const startTime = new Date(today);
  let startHour24 = parseInt(startHour);
  if (startAmPm === "PM" && startHour24 !== 12) startHour24 += 12;
  if (startAmPm === "AM" && startHour24 === 12) startHour24 = 0;
  startTime.setHours(startHour24, parseInt(startMinute));

  const endTime = new Date(today);
  let endHour24 = parseInt(endHour);
  if (endAmPm === "PM" && endHour24 !== 12) endHour24 += 12;
  if (endAmPm === "AM" && endHour24 === 12) endHour24 = 0;
  endTime.setHours(endHour24, parseInt(endMinute));

  // Validation checks
  if (endTime <= startTime) {
    showValidationError("End time must be after start time");
    rcaBtn.disabled = true;
    return;
  }

  const durationMinutes = (endTime - startTime) / (1000 * 60);
  if (durationMinutes < 5) {
    showValidationError("Analysis window must be at least 5 minutes");
    rcaBtn.disabled = true;
    return;
  }

  if (durationMinutes > 720) {
    // 12 hours
    showValidationError("Analysis window should not exceed 12 hours");
    rcaBtn.disabled = true;
    return;
  }

  // All validations passed
  const durationText = formatDuration(durationMinutes);
  showValidationSuccess(`Analysis window: ${durationText}`);
  rcaBtn.disabled = false;
}

function showValidationError(message) {
  const validationDiv = document.getElementById("timeValidation");
  validationDiv.textContent = "⚠️ " + message;
  validationDiv.className = "time-validation error";
  validationDiv.style.display = "block";
}

function showValidationSuccess(message) {
  const validationDiv = document.getElementById("timeValidation");
  validationDiv.textContent = "✓ " + message;
  validationDiv.className = "time-validation success";
  validationDiv.style.display = "block";
}

function formatDuration(minutes) {
  if (minutes < 60) {
    return `${minutes} minutes`;
  } else {
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    return remainingMinutes > 0
      ? `${hours}h ${remainingMinutes}m`
      : `${hours} hour${hours > 1 ? "s" : ""}`;
  }
}

function getSelectedTimeRange() {
  const startHour = parseInt(document.getElementById("startHour").value);
  const startMinute = parseInt(document.getElementById("startMinute").value);
  const startAmPm = document.getElementById("startAmPm").value;

  const endHour = parseInt(document.getElementById("endHour").value);
  const endMinute = parseInt(document.getElementById("endMinute").value);
  const endAmPm = document.getElementById("endAmPm").value;

  // Convert to 24-hour format
  let startHour24 = startHour;
  if (startAmPm === "PM" && startHour !== 12) startHour24 += 12;
  if (startAmPm === "AM" && startHour === 12) startHour24 = 0;

  let endHour24 = endHour;
  if (endAmPm === "PM" && endHour !== 12) endHour24 += 12;
  if (endAmPm === "AM" && endHour === 12) endHour24 = 0;

  return {
    start_hour: startHour24,
    start_minute: startMinute,
    end_hour: endHour24,
    end_minute: endMinute,
    start_time_formatted: `${startHour.toString().padStart(2, "0")}:${startMinute.toString().padStart(2, "0")} ${startAmPm}`,
    end_time_formatted: `${endHour.toString().padStart(2, "0")}:${endMinute.toString().padStart(2, "0")} ${endAmPm}`,
  };
}

// -------------------------------
// RCA EXECUTION (Auto-detected time window)
// -------------------------------
async function runRCA() {
  console.log("runRCA function called");

  // Use the auto-detected time window from AWR report
  if (!detectedTimeWindow) {
    alert(
      "No analysis time window detected. Please ensure AWR data is loaded.",
    );
    return;
  }

  console.log("Using detected time window:", detectedTimeWindow);

  // Show loading state
  const rcaBtn = document.getElementById("runRcaBtn");
  const originalText = rcaBtn.textContent;
  rcaBtn.textContent = "🔄 Running RCA Analysis...";
  rcaBtn.disabled = true;

  console.log("Making API call to /api/run_rca");

  try {
    const response = await fetch("/api/run_rca", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        start_hour: detectedTimeWindow.start_hour,
        start_minute: detectedTimeWindow.start_minute,
        end_hour: detectedTimeWindow.end_hour,
        end_minute: detectedTimeWindow.end_minute,
        time_window: detectedTimeWindow.time_window,
      }),
    });

    console.log("API response status:", response.status);
    const result = await response.json();
    console.log("API result:", result);

    if (response.ok) {
      displayRCAResults(result);
      // Keep time window section visible but scroll to results
      setTimeout(() => {
        const summaryElement = document.getElementById("summarySection");
        if (summaryElement && summaryElement.style.display !== "none") {
          summaryElement.scrollIntoView({ behavior: "smooth" });
        } else {
          const resultsElement = document.getElementById("resultsSection");
          if (resultsElement) {
            resultsElement.scrollIntoView({ behavior: "smooth" });
          }
        }
      }, 100); // Small delay to ensure elements are rendered
    } else {
      alert(result.detail || "RCA analysis failed");
    }
  } catch (error) {
    console.error("RCA API error:", error);
    // Try to extract actual error message
    let errorMsg = "Network error during RCA analysis";
    if (error && error.message) {
      errorMsg += ": " + error.message;
    }
    alert(errorMsg);
  } finally {
    rcaBtn.textContent = originalText;
    rcaBtn.disabled = false;
  }
}

function displayRCAResults(result) {
  console.log("displayRCAResults called with:", result);

  // Get all the DOM elements we need
  const summarySection = document.getElementById("summarySection");
  const resultsSection = document.getElementById("resultsSection");
  const summaryTableBody = document.getElementById("summary-table-body");
  const detailedTableBody = document.getElementById("detailed-table-body");

  console.log("DOM elements found:", {
    summarySection: !!summarySection,
    resultsSection: !!resultsSection,
    summaryTableBody: !!summaryTableBody,
    detailedTableBody: !!detailedTableBody,
  });

  // Clear previous results
  if (summaryTableBody) summaryTableBody.innerHTML = "";
  if (detailedTableBody) detailedTableBody.innerHTML = "";

  // Display DBA Expert Analysis if available
  if (result.dba_expert_analysis) {
    displayDBAExpertAnalysis(result.dba_expert_analysis);
  }

  // Handle the actual API response format
  const queries = result.top_sql || result.top_queries || [];
  console.log("SQL queries found:", queries.length, queries);

  if (queries.length > 0) {
    // Populate both tables with the same data
    queries.forEach((query, index) => {
      console.log(`Processing query ${index}:`, query);

      const riskClass = getRiskClass(query.risk);

      // Handle both possible response formats
      const sqlId = query.sql_id || `SQL_${index + 1}`;
      const elapsedTime = query.elapsed || query.elapsed_time || 0;
      const cpuTime = query.cpu || query.cpu_time || 0;
      const executions = query.executions || 0;
      const elapsedPerExec = query.elapsed_per_exec || 0;
      const risk = query.risk || query.risk_score || "N/A";

      console.log(
        `Processed data - ID: ${sqlId}, Elapsed: ${elapsedTime}, CPU: ${cpuTime}, Executions: ${executions}, AvgPerExec: ${elapsedPerExec}, Risk: ${risk}`,
      );

      // Create row for summary table (no Fix column) - ONLY if element exists
      if (summaryTableBody) {
        const summaryRow = document.createElement("tr");
        summaryRow.innerHTML = `
                    <td><span class="sql-id-copy" onclick="copyToClipboard('${sqlId}', this)" title="Click to copy">${sqlId} <span style="font-size:0.65rem;opacity:0.5;">📋</span></span></td>
                    <td>${parseFloat(elapsedTime).toFixed(2)}</td>
                    <td>${parseFloat(cpuTime).toFixed(2)}</td>
                    <td>${executions}</td>
                    <td>${parseFloat(elapsedPerExec).toFixed(3)}</td>
                    <td>
                        <span class="risk-badge ${riskClass}">
                            ${risk}
                        </span>
                    </td>
                `;
        summaryTableBody.appendChild(summaryRow);
        console.log("Added row to summary table");
      }

      // Create row for detailed table (with Fix button) - ONLY if element exists
      if (detailedTableBody) {
        const detailedRow = document.createElement("tr");
        detailedRow.innerHTML = `
                    <td>${sqlId}</td>
                    <td>${parseFloat(elapsedTime).toFixed(2)}</td>
                    <td>${parseFloat(cpuTime).toFixed(2)}</td>
                    <td>${executions}</td>
                    <td>${parseFloat(elapsedPerExec).toFixed(3)}</td>
                    <td>
                        <span class="risk-badge ${riskClass}">
                            ${risk}
                        </span>
                    </td>
                    <td>
                        <button class="fix-btn" onclick="showFixRecommendations('${sqlId}', ${JSON.stringify(
                          {
                            sql_id: sqlId,
                            elapsed_time: elapsedTime,
                            cpu_time: cpuTime,
                            executions: executions,
                            elapsed_per_exec: elapsedPerExec,
                            risk_score: risk,
                          },
                        ).replace(/"/g, "&quot;")})">
                            Fix
                        </button>
                    </td>
                `;
        detailedTableBody.appendChild(detailedRow);
        console.log("Added row to detailed table");
      }
    });

    // Show summary section only
    if (summarySection) {
      summarySection.style.display = "block";
      console.log("Summary section made visible");
    }
    // resultsSection (Problematic Queries table) intentionally not displayed

    console.log("Both sections should now be visible");
    console.log(
      "Summary section display:",
      summarySection ? summarySection.style.display : "not found",
    );
    console.log(
      "Results section display:",
      resultsSection ? resultsSection.style.display : "not found",
    );

    // After 2-second delay, scroll to summary section
    setTimeout(() => {
      if (summarySection && summarySection.offsetHeight > 0) {
        summarySection.scrollIntoView({
          behavior: "smooth",
          block: "start",
        });
        console.log("Scrolled to summary section");
      }
      // resultsSection scroll removed as section is no longer displayed
    }, 2000);
  } else {
    // Only show "No high-load SQL found" in the detailed table
    if (detailedTableBody) {
      detailedTableBody.innerHTML = `
                <tr>
                    <td colspan="7" style="text-align: center; padding: 2rem;">
                        No problematic queries identified. Database workload within normal parameters.
                    </td>
                </tr>
            `;
    }
    if (summarySection) summarySection.style.display = "none";
    // resultsSection (Problematic Queries table) intentionally not displayed
    console.log("No queries found, showing empty message");
  }
}

// -------------------------------
// DBA EXPERT ANALYSIS DISPLAY
// -------------------------------
function getRiskClass(riskScore) {
  if (typeof riskScore === "string") {
    return riskScore.toLowerCase();
  }
  if (riskScore >= 80) return "high";
  if (riskScore >= 50) return "medium";
  return "low";
}

function _legacy_displayDBAExpertAnalysis(dbaAnalysis) {
  console.log("Displaying DBA Expert Analysis:", dbaAnalysis);

  // Create or get DBA Analysis section
  let dbaSection = document.getElementById("dbaExpertSection");
  if (!dbaSection) {
    dbaSection = document.createElement("section");
    dbaSection.id = "dbaExpertSection";
    dbaSection.className = "dba-expert-section";

    // Insert before results section
    const resultsSection = document.getElementById("resultsSection");
    resultsSection.parentNode.insertBefore(dbaSection, resultsSection);
  }

  const findings = dbaAnalysis.problematic_sql_findings || [];
  const workloadSummary = dbaAnalysis.workload_summary || {};
  const conclusion = dbaAnalysis.dba_conclusion || "";

  let html = `
        <h2>🧠 AI-Driven DBA Expert Analysis</h2>
        <div class="dba-workload-summary">
            <div class="workload-stat">
                <span class="stat-label">Pattern:</span>
                <span class="stat-value">${workloadSummary.pattern || "Unknown"}</span>
            </div>
            <div class="workload-stat">
                <span class="stat-label">Total Elapsed:</span>
                <span class="stat-value">${workloadSummary.total_elapsed_s || 0}s</span>
            </div>
            <div class="workload-stat">
                <span class="stat-label">SQL Analyzed:</span>
                <span class="stat-value">${workloadSummary.sql_analyzed || 0}</span>
            </div>
            <div class="workload-stat">
                <span class="stat-label">Problematic Found:</span>
                <span class="stat-value highlight">${workloadSummary.problematic_found || 0}</span>
            </div>
        </div>
    `;

  if (findings.length > 0) {
    html += '<div class="dba-findings">';

    findings.forEach((finding, index) => {
      const severityClass = (finding.severity || "").toLowerCase();

      // Determine PRIMARY root cause (ONE only)
      const techParams = finding.technical_parameters || {};
      const cpuPct =
        parseFloat(techParams.cpu_percentage || techParams.cpu_pct) || 0;
      const ioPct =
        parseFloat(techParams.io_percentage || techParams.io_pct) || 0;
      const dbTimePct =
        parseFloat(
          techParams.contribution_to_db_time_pct || techParams.db_time_pct,
        ) || 0;

      let primaryCause = "";
      let primaryIcon = "";
      let primaryColor = "";
      let explanation = "";

      if (cpuPct >= 80 && ioPct <= 10) {
        primaryCause = "CPU-Bound SQL";
        primaryIcon = "🔥";
        primaryColor = "#ef4444";
        explanation = `This SQL is CPU-bound, consuming ~${dbTimePct}% of DB Time during the analysis window, with high CPU utilization (${cpuPct}%) per execution and minimal IO wait.`;
      } else if (ioPct >= 40) {
        primaryCause = "IO-Bound SQL";
        primaryIcon = "💾";
        primaryColor = "#f59e0b";
        explanation = `This SQL is IO-bound due to excessive physical reads. High DB Time (${dbTimePct}%) is driven by IO waits (${ioPct}%) caused by missing or inefficient indexes.`;
      } else if (ioPct >= 20 && cpuPct < 50) {
        primaryCause = "IO-Bound SQL (Missing Index)";
        primaryIcon = "📑";
        primaryColor = "#f59e0b";
        explanation = `This SQL is IO-bound with ${ioPct}% IO wait time. High physical reads suggest full table scans due to missing or inefficient indexes.`;
      } else if (cpuPct >= 50) {
        primaryCause = "CPU-Intensive SQL";
        primaryIcon = "🔴";
        primaryColor = "#ef4444";
        explanation = `This SQL is CPU-intensive with ${cpuPct}% CPU utilization, indicating compute-heavy operations like sorting, hashing, or complex joins.`;
      } else {
        primaryCause = "Mixed Workload";
        primaryIcon = "⚡";
        primaryColor = "#8b5cf6";
        explanation = `This SQL contributes to both CPU and IO load. Neither CPU (${cpuPct}%) nor IO (${ioPct}%) is dominant, indicating mixed execution pressure.`;
      }

      html += `
                <div class="dba-finding-card">
                    <!-- Finding Header -->
                    <div class="finding-header" style="margin-bottom: 16px;">
                        <h3>🎯 Finding #${index + 1}: ${finding.sql_id}</h3>
                        <span class="severity-badge ${severityClass}">${finding.severity || "MEDIUM"} PRIORITY</span>
                    </div>
                    
                    <!-- ROOT CAUSE ANALYSIS - IMMEDIATELY AFTER SQL ID -->
                    <div class="root-cause-section" style="border: 2px solid ${primaryColor}; border-radius: 12px; margin: 12px 0; background: linear-gradient(135deg, ${primaryColor}15 0%, ${primaryColor}08 100%); overflow: hidden;">
                        <div style="background: ${primaryColor}22; padding: 12px 16px; display: flex; align-items: center; gap: 10px;">
                            <span style="font-size: 1.3rem;">🎯</span>
                            <span style="font-size: 1.1rem; font-weight: 700; color: ${primaryColor};">Root Cause Analysis</span>
                        </div>
                        <div style="padding: 16px;">
                            <!-- Primary Cause Badge -->
                            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
                                <div style="background: ${primaryColor}22; border: 2px solid ${primaryColor}; border-radius: 12px; padding: 12px 20px; display: flex; align-items: center; gap: 10px;">
                                    <span style="font-size: 1.5rem;">${primaryIcon}</span>
                                    <div>
                                        <div style="font-size: 0.7rem; color: #94a3b8; text-transform: uppercase;">Primary Cause</div>
                                        <div style="font-size: 1.1rem; font-weight: 700; color: ${primaryColor};">${primaryCause}</div>
                                    </div>
                                </div>
                            </div>
                            <!-- Explanation -->
                            <div style="background: rgba(0,0,0,0.2); border-radius: 8px; padding: 12px; margin-bottom: 12px;">
                                <div style="color: #94a3b8; font-size: 0.8rem; margin-bottom: 6px;">📋 Explanation</div>
                                <p style="color: #e2e8f0; margin: 0; line-height: 1.5;">${explanation}</p>
                            </div>
                            <!-- Evidence Grid -->
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 8px;">
                                ${dbTimePct ? `<div style="text-align: center; padding: 8px; background: rgba(239,68,68,0.1); border-radius: 6px;"><div style="font-size: 1.1rem; font-weight: 700; color: #ef4444;">${dbTimePct}%</div><div style="font-size: 0.7rem; color: #94a3b8;">DB Time</div></div>` : ""}
                                ${cpuPct ? `<div style="text-align: center; padding: 8px; background: rgba(239,68,68,0.1); border-radius: 6px;"><div style="font-size: 1.1rem; font-weight: 700; color: #f59e0b;">${cpuPct}%</div><div style="font-size: 0.7rem; color: #94a3b8;">CPU</div></div>` : ""}
                                ${ioPct ? `<div style="text-align: center; padding: 8px; background: rgba(245,158,11,0.1); border-radius: 6px;"><div style="font-size: 1.1rem; font-weight: 700; color: #f59e0b;">${ioPct}%</div><div style="font-size: 0.7rem; color: #94a3b8;">IO Wait</div></div>` : ""}
                            </div>
                            <!-- Problem Summary Details -->
                            ${
                              finding.problem_summary
                                ? `
                            <div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.1);">
                                <div style="color: #94a3b8; font-size: 0.8rem; margin-bottom: 6px;">📝 Details</div>
                                <div style="color: #cbd5e1; line-height: 1.5;">${formatMarkdown(finding.problem_summary)}</div>
                            </div>
                            `
                                : ""
                            }
                        </div>
                    </div>
                    
                    <!-- DBA Assessment -->
                    ${
                      finding.execution_pattern?.dba_assessment
                        ? `
                        <div class="dba-assessment" style="border: 2px solid #6366f1; border-radius: 8px; padding: 12px; margin: 12px 0; background: rgba(99, 102, 241, 0.01); box-sizing: border-box;">
                            <strong>📝 DBA Assessment:</strong>
                            <p style="margin: 8px 0 0 0; color: #cbd5e1;">${finding.execution_pattern.dba_assessment}</p>
                        </div>
                    `
                        : ""
                    }
                    
                    <!-- NOTE: DBA Interpretation with multiple causes hidden per DBA requirements -->
                    <!-- Single root cause is shown in Root Cause Analysis section above -->
                    
                    <!-- 🛠️ Final Recommendations -->
                    <div class="dba-section recommendations">
                        <h4>🛠️ Final DBA Recommendations</h4>
                        ${formatDBARecommendations(finding.recommendations || {})}
                    </div>
                </div>
            `;
    });

    html += "</div>";
  } else {
    html += `
            <div class="no-findings">
                <p>✅ No high-risk SQL identified. Workload appears normal.</p>
            </div>
        `;
  }

  // Add conclusion
  if (conclusion) {
    html += `
            <div class="dba-conclusion">
                <h3>🎯 Final DBA Conclusion</h3>
                <div class="conclusion-content">
                    ${formatMarkdown(conclusion)}
                </div>
            </div>
        `;
  }

  dbaSection.innerHTML = html;
  dbaSection.style.display = "block";

  // Add copy buttons to all code blocks
  setTimeout(() => addCopyButtons(), 100);
}

function formatTechnicalParams(params) {
  return `
        <div class="param-grid">
            <div class="param-item">
                <span class="param-label">Total Elapsed Time:</span>
                <span class="param-value">${params.total_elapsed_time_s || 0}s</span>
            </div>
            <div class="param-item">
                <span class="param-label">CPU Time:</span>
                <span class="param-value">${params.cpu_time_s || 0}s</span>
            </div>
            <div class="param-item">
                <span class="param-label">Execution Count:</span>
                <span class="param-value">${params.executions || 0}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Avg Elapsed/Exec:</span>
                <span class="param-value">${params.avg_elapsed_per_exec_s || 0}s</span>
            </div>
            <div class="param-item">
                <span class="param-label">DB Time Contribution:</span>
                <span class="param-value">${params.contribution_to_db_time_pct || 0}%</span>
            </div>
            <div class="param-item">
                <span class="param-label">CPU %:</span>
                <span class="param-value">${params.cpu_percentage || 0}%</span>
            </div>
            <div class="param-item">
                <span class="param-label">I/O %:</span>
                <span class="param-value">${params.io_percentage || 0}%</span>
            </div>
        </div>
    `;
}

function formatDBARecommendations(recs) {
  let html = "";

  // Priority
  if (recs.priority_description) {
    html += `<div class="priority-box">${recs.priority_description}</div>`;
  }

  // What to do next
  if (recs.what_dba_should_do_next) {
    html += `
            <div class="recommendation-section">
                ${formatMarkdown(recs.what_dba_should_do_next)}
            </div>
        `;
  }

  // Action plan
  if (recs.dba_action_plan) {
    html += `
            <div class="recommendation-section">
                ${formatMarkdown(recs.dba_action_plan)}
            </div>
        `;
  }

  // Expected improvement
  if (recs.expected_improvement) {
    html += `<div class="improvement-note">💡 ${recs.expected_improvement}</div>`;
  }

  return html || "<p>No specific recommendations available</p>";
}

function formatMarkdown(text) {
  if (!text) return "";

  // Clean the text first
  text = text.trim();

  // First handle basic markdown formatting
  let html = text
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.*?)\*/g, "<em>$1</em>")
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/```sql\n([\s\S]*?)```/g, '<pre class="sql-code">$1</pre>')
    .replace(/```([\s\S]*?)```/g, '<pre class="code-block">$1</pre>');

  // Split into lines and process each line
  const lines = html.split("\n");
  let result = [];
  let inBulletSection = false;

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i].trim();

    // Skip completely empty lines when not in bullet mode
    if (!line && !inBulletSection) {
      continue;
    }

    // Check for bullet point
    if (line.startsWith("•")) {
      // Start bullet section if not already started
      if (!inBulletSection) {
        result.push('<ul class="dba-bullet-list">');
        inBulletSection = true;
      }
      // Add bullet item (remove • and trim)
      const bulletText = line.substring(1).trim();
      result.push(`<li>${bulletText}</li>`);
    } else if (line.match(/^\d+\.\s/)) {
      // Handle numbered lists too
      if (inBulletSection) {
        result.push("</ul>");
        inBulletSection = false;
      }
      result.push(
        `<p class="numbered-item" style="margin: 1px 0 2px 0 !important; padding: 0 !important; line-height: 1.3 !important;">${line}</p>`,
      );
    } else {
      // Close bullet section if we were in one and this is not empty
      if (inBulletSection && line) {
        result.push("</ul>");
        inBulletSection = false;
      }

      // Process regular lines
      if (line) {
        // Handle headers with ultra-tight spacing
        if (line.startsWith("######")) {
          result.push(
            `<h6 style="margin: 1px 0 0 0 !important; padding: 0 !important; line-height: 1.2 !important;">${line.substring(6).trim()}</h6>`,
          );
        } else if (line.startsWith("#####")) {
          result.push(
            `<h5 style="margin: 1px 0 0 0 !important; padding: 0 !important; line-height: 1.2 !important;">${line.substring(5).trim()}</h5>`,
          );
        } else if (line.startsWith("####")) {
          result.push(
            `<h4 style="margin: 1px 0 0 0 !important; padding: 0 !important; line-height: 1.2 !important;">${line.substring(4).trim()}</h4>`,
          );
        } else if (line.startsWith("###")) {
          result.push(
            `<h3 style="margin: 1px 0 0 0 !important; padding: 0 !important; line-height: 1.2 !important;">${line.substring(3).trim()}</h3>`,
          );
        } else if (line.startsWith("##")) {
          result.push(
            `<h2 style="margin: 1px 0 0 0 !important; padding: 0 !important; line-height: 1.2 !important;">${line.substring(2).trim()}</h2>`,
          );
        } else if (line.startsWith("#")) {
          result.push(
            `<h1 style="margin: 1px 0 0 0 !important; padding: 0 !important; line-height: 1.2 !important;">${line.substring(1).trim()}</h1>`,
          );
        } else {
          result.push(
            `<p style="margin: 0 0 1px 0 !important; padding: 0 !important; line-height: 1.3 !important;">${line}</p>`,
          );
        }
      } else if (inBulletSection) {
        // Allow empty lines to end bullet sections
        result.push("</ul>");
        inBulletSection = false;
      }
    }
  }

  // Close any remaining bullet section
  if (inBulletSection) {
    result.push("</ul>");
  }

  return result.join("\n");
}

function escapeHtml(unsafe) {
  return unsafe
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// Premium Dashboard Style Interpretation Blocks
function createPremiumInterpretationBlocks(text) {
  if (!text)
    return '<p style="margin: 0; padding: 8px; color: #9ca3af;">No interpretation available</p>';

  // Parse the markdown text to extract interpretation blocks
  const lines = text.split("\n");
  let html = "";

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();

    // Skip empty lines
    if (!line) continue;

    // Check if it's a heading (interpretation title)
    if (line.match(/^\*\*.*\*\*$/)) {
      let title = line.replace(/\*\*/g, "").trim();
      let icon = "";

      // Check if title already starts with an emoji
      const emojiRegex = /^(\p{Emoji_Presentation}|\p{Emoji}\uFE0F)\s*/u;
      const emojiMatch = title.match(emojiRegex);

      if (emojiMatch) {
        // Extract existing icon and remove it from title
        icon = emojiMatch[0].trim();
        title = title.replace(emojiRegex, "").trim();
      } else {
        // Generate icon if none exists
        icon = getInterpretationIcon(title);
      }

      // Find the next paragraph (description)
      let description = "";
      for (let j = i + 1; j < lines.length; j++) {
        const nextLine = lines[j].trim();
        if (!nextLine) continue;
        if (nextLine.match(/^\*\*.*\*\*$/)) break; // Next title found
        description = nextLine;
        i = j; // Skip to processed line
        break;
      }

      // Create simple content row without border
      html += `
                <div class="interpretation-row" style="margin: 0 0 16px 0; padding: 0;">
                    <div style="margin: 0 0 4px 0;">
                        <h5 style="margin: 0; padding: 0; font-size: 14px; font-weight: 600; color: #e2e8f0; line-height: 1.2; display: flex; align-items: center; gap: 8px;">
                            <span style="font-size: 16px;">${icon}</span>
                            <span>${title}</span>
                        </h5>
                    </div>
                    <div style="margin: 0; padding: 0 0 0 24px;">
                        <p style="margin: 0; padding: 0; font-size: 13px; line-height: 1.4; color: #cbd5e1; text-align: justify;">
                            ${description}
                        </p>
                    </div>
                </div>
            `;
    }
  }

  return (
    html ||
    '<p style="margin: 0; padding: 8px; color: #9ca3af;">No interpretation blocks found</p>'
  );
}

function getInterpretationIcon(title) {
  const titleLower = title.toLowerCase();
  if (titleLower.includes("cpu")) return "🔴";
  if (titleLower.includes("parallel") || titleLower.includes("compute"))
    return "🟡";
  if (titleLower.includes("stale") || titleLower.includes("statistics"))
    return "🟡";
  if (titleLower.includes("io") || titleLower.includes("i/o")) return "🟠";
  if (titleLower.includes("frequency") || titleLower.includes("fast"))
    return "🔵";
  if (titleLower.includes("slow") || titleLower.includes("batch")) return "🔴";
  if (titleLower.includes("select") || titleLower.includes("join")) return "⚠️";
  if (titleLower.includes("where") || titleLower.includes("distinct"))
    return "🔴";
  return "🔍";
}

// -------------------------------
// FIX RECOMMENDATIONS (Keep for backward compatibility)
// -------------------------------
async function showFixRecommendations(sqlId, queryData) {
  const fixPanel = document.getElementById("fixPanel");

  // Show loading state
  document.getElementById("recIndex").textContent =
    "Loading recommendations...";
  document.getElementById("recRewrite").textContent = "Loading...";
  document.getElementById("riskScore").textContent = "Loading...";
  document.getElementById("commands").textContent = "Loading...";

  fixPanel.style.display = "block";
  fixPanel.scrollIntoView({ behavior: "smooth" });

  try {
    const response = await fetch("/api/recommend", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        sql_id: sqlId,
        query_data: queryData,
      }),
    });

    const result = await response.json();

    if (response.ok) {
      displayFixRecommendations(result);
    } else {
      showFixError(result.detail || "Failed to get recommendations");
    }
  } catch (error) {
    showFixError("Network error while getting recommendations");
    console.error("Fix recommendations error:", error);
  }
}

function displayFixRecommendations(recommendations) {
  document.getElementById("recIndex").textContent =
    recommendations.recommended_indexes || "No index recommendations available";

  document.getElementById("recRewrite").textContent =
    recommendations.query_rewrite || "No query rewrite suggestions available";

  document.getElementById("riskScore").textContent =
    recommendations.risk_assessment || "Risk assessment not available";

  document.getElementById("commands").textContent =
    recommendations.exact_commands || "No specific commands available";

  // Add risk score styling
  const riskElement = document.getElementById("riskScore");
  if (recommendations.risk_level) {
    riskElement.className = `risk-score ${recommendations.risk_level.toLowerCase()}`;
  }

  // Add copy buttons to fix panel
  setTimeout(() => addCopyButtons(), 100);
}

function showFixError(message) {
  document.getElementById("recIndex").textContent =
    "Error loading recommendations";
  document.getElementById("recRewrite").textContent = message;
  document.getElementById("riskScore").textContent = "Error";
  document.getElementById("commands").textContent =
    "Unable to generate commands";
}

// -------------------------------
// LOAD EXISTING RESULTS
// -------------------------------
async function loadExistingResults() {
  try {
    // Check if there are existing parsed CSV files
    const response = await fetch("/api/results", {
      credentials: "same-origin",
    });
    if (response.ok) {
      const result = await response.json();
      if (result.has_data) {
        // Show option to view existing results or upload new files
        showExistingDataOptions(result);
      }
    }
  } catch (error) {
    console.log("No existing results found");
  }
}

function showExistingDataOptions(data) {
  // Show the styled existing data banner in the HTML
  const banner = document.getElementById("existingDataBanner");
  if (banner) {
    banner.style.display = "flex";
  }
  // Also show hero stats if available
  const heroStats = document.getElementById("dashHeroStats");
  if (heroStats) {
    heroStats.style.display = "flex";
  }

  // Populate stats cards with real data
  if (data) {
    const filesCount = document.getElementById("statFilesCount");
    const csvCount = document.getElementById("statCsvCount");
    const loadStatus = document.getElementById("statHighLoad");

    if (filesCount) {
      filesCount.textContent = data.html_count || 0;
    }
    if (csvCount) {
      csvCount.textContent = data.csv_count || 0;
    }
    if (loadStatus) {
      if (data.has_rca_result) {
        loadStatus.textContent = "Analyzed";
        loadStatus.style.color = "#22c55e";
      } else if (data.has_parsed_csv) {
        loadStatus.textContent = "Pending";
        loadStatus.style.color = "#f59e0b";
      } else {
        loadStatus.textContent = "--";
      }
    }
  }
}

async function viewExistingResults() {
  try {
    const response = await fetch("/api/results", {
      credentials: "same-origin",
    });
    const result = await response.json();

    if (response.ok && result.has_data) {
      // Hide upload section and show results directly
      document.getElementById("uploadSection").style.display = "none";

      if (result.parsing_results) {
        displayParsingResults(result);
      }

      if (result.analysis_results) {
        displayRCAResults(result.analysis_results);
      }
    }
  } catch (error) {
    alert("Error loading existing results");
  }
}

function startFreshUpload() {
  // Hide the existing data banner
  const banner = document.getElementById("existingDataBanner");
  if (banner) banner.style.display = "none";
  // Also remove legacy notice if present
  const notice = document.getElementById("existingDataNotice");
  if (notice) notice.remove();

  // Check if upload has already been completed in this session
  if (sessionUploadCompleted) {
    showSessionUploadCompleteMessage();
    return;
  }

  // Reset all sections
  document.getElementById("parsingSection").style.display = "none";
  document.getElementById("timeWindowSection").style.display = "none";
  document.getElementById("resultsSection").style.display = "none";
  document.getElementById("fixPanel").style.display = "none";

  // Clear file selection and reset upload flow completely
  resetUploadFlow();

  document.getElementById("fileList").style.display = "none";
  document.getElementById("uploadBtn").style.display = "none";
}

function showSessionUploadCompleteMessage() {
  const uploadSection = document.getElementById("uploadSection");

  // Add session complete notice
  const sessionCompleteNotice = document.createElement("div");
  sessionCompleteNotice.id = "sessionCompleteNotice";
  sessionCompleteNotice.style.cssText =
    "background: #f0fdf4; border: 1px solid #10b981; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; text-align: center;";
  sessionCompleteNotice.innerHTML = `
        <h3 style="color: #10b981; margin: 0 0 0.5rem 0;">✅ Files Already Uploaded</h3>
        <p style="color: #065f46; margin: 0 0 1rem 0;">You have already uploaded files in this session. To upload new files, please refresh the page.</p>
        <button onclick="location.reload()" style="background: #10b981; color: white; border: none; padding: 0.5rem 1rem; border-radius: 4px; cursor: pointer;">
            Refresh Page
        </button>
    `;

  // Clear existing content and show notice
  uploadSection.innerHTML = "";
  uploadSection.appendChild(sessionCompleteNotice);
  uploadSection.style.display = "block";
}

function populateTimestampOptions(timestamps) {
  const timeWindowSection = document.getElementById("timeWindowSection");

  // Create timestamp selector UI
  const timestampSelectorHTML = `
        <h2>Select Time Window</h2>
        <p>Choose from detected high-load periods or enter custom range</p>
        
        <div class="timestamp-options">
            <h3>Detected Time Periods:</h3>
            <div class="period-options">
                ${timestamps
                  .map(
                    (ts) => `
                    <div class="period-option" onclick="selectTimePeriod('${ts.start}', '${ts.end}')">
                        <div class="period-header">
                            <strong>${ts.label}</strong>
                            <span class="period-time">${ts.start} - ${ts.end}</span>
                        </div>
                        <div class="period-description">${ts.description}</div>
                    </div>
                `,
                  )
                  .join("")}
            </div>
        </div>
        
        <div class="custom-time-inputs">
            <h3>Custom Time Range:</h3>
            <div class="time-inputs">
                <input type="text" id="startTime" placeholder="Start: YYYY-MM-DD HH:MM:SS" class="time-input">
                <input type="text" id="endTime" placeholder="End: YYYY-MM-DD HH:MM:SS" class="time-input">
                <button onclick="runRCA()" class="rca-btn">Run RCA</button>
            </div>
        </div>
    `;

  timeWindowSection.innerHTML = timestampSelectorHTML;
}

function selectTimePeriod(start, end) {
  document.getElementById("startTime").value = start;
  document.getElementById("endTime").value = end;

  // Highlight selected period
  document
    .querySelectorAll(".period-option")
    .forEach((el) => el.classList.remove("selected"));
  event.target.closest(".period-option").classList.add("selected");
}

function showTimestampError(message) {
  const timeWindowSection = document.getElementById("timeWindowSection");
  timeWindowSection.innerHTML = `
        <h2>Select Time Window</h2>
        <div class="error-message" style="display: block;">
            ${message}. Using default time range.
        </div>
        <div class="time-inputs">
            <input type="text" id="startTime" placeholder="Start: YYYY-MM-DD HH:MM:SS" class="time-input" value="2025-12-16 10:00:00">
            <input type="text" id="endTime" placeholder="End: YYYY-MM-DD HH:MM:SS" class="time-input" value="2025-12-16 12:00:00">
            <button onclick="runRCA()" class="rca-btn">Run RCA</button>
        </div>
    `;
  timeWindowSection.style.display = "block";
}

// -------------------------------
// LOGOUT FUNCTION
// -------------------------------
async function logout() {
  try {
    const response = await fetch("/auth/logout", {
      method: "POST",
    });

    if (response.ok) {
      window.location.href = "/login";
    }
  } catch (error) {
    console.error("Logout failed:", error);
    // Force redirect anyway
    window.location.href = "/login";
  }
}

// -------------------------------
// COMMON RENDER LOGIC
// -------------------------------
function renderFromResult(data) {
  renderTopSQL(data.top_sql || []);
  window.lastAgentInsights = data.agent_insights || [];
}

// -------------------------------
// TOP SQL TABLE
// -------------------------------
function renderTopSQL(rows) {
  const tbody = document.getElementById("sql-table-body");
  tbody.innerHTML = "";

  rows.forEach((r, idx) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
            <td>${r.sql_id}</td>
            <td>${r.elapsed}</td>
            <td>${r.cpu}</td>
            <td class="risk ${r.risk.toLowerCase()}">${r.risk}</td>
            <td><button onclick="showFix(${idx})">Fix</button></td>
        `;
    tbody.appendChild(tr);
  });
}

// -------------------------------
// FIX PANEL
// -------------------------------
function showFix(index) {
  const insight = window.lastAgentInsights[index];
  if (!insight) return;

  document.getElementById("fixPanel").style.display = "block";
  document.getElementById("recIndex").innerText = insight.index || "N/A";
  document.getElementById("recRewrite").innerText = insight.rewrite || "N/A";
  document.getElementById("riskScore").innerText = insight.risk || "N/A";
  document.getElementById("commands").innerText = insight.commands || "N/A";
}

// -------------------------------
// AUTO LOAD ON PAGE OPEN
// -------------------------------
window.onload = function () {
  initializeFileUpload();
  // Don't auto-load last result anymore since we want fresh uploads

  // Force tight spacing on interpretation sections
  forceTightSpacing();
};

// Force tight spacing function
function forceTightSpacing() {
  console.log("Forcing tight spacing...");

  // Add aggressive CSS rules
  const style = document.createElement("style");
  style.innerHTML = `
        .interpretation h4, .interpretation h3, .interpretation h2, .interpretation h1 {
            margin-bottom: 0px !important;
            margin-top: 8px !important;
            padding-bottom: 0px !important;
        }
        .interpretation p {
            margin-top: 0px !important;
            margin-bottom: 6px !important;
            padding-top: 0px !important;
        }
        .interpretation .dba-bullet-list {
            margin-top: 4px !important;
            margin-bottom: 8px !important;
        }
        .interpretation .numbered-item {
            margin-top: 2px !important;
            margin-bottom: 4px !important;
        }
    `;
  document.head.appendChild(style);

  // Also apply inline styles to existing elements every 500ms for 5 seconds
  let attempts = 0;
  const interval = setInterval(() => {
    attempts++;
    console.log(`Applying spacing attempt ${attempts}`);

    document
      .querySelectorAll(
        ".interpretation h4, .interpretation h3, .interpretation h2, .interpretation h1",
      )
      .forEach((el) => {
        el.style.marginBottom = "0px";
        el.style.marginTop = "4px";
        el.style.paddingBottom = "0px";
      });

    document.querySelectorAll(".interpretation p").forEach((el) => {
      el.style.marginTop = "0px";
      el.style.marginBottom = "3px";
      el.style.paddingTop = "0px";
      el.style.lineHeight = "1.4";
    });

    if (attempts >= 10) {
      clearInterval(interval);
    }
  }, 500);
}
