// ===============================================
// NEW RESULTS PAGE JAVASCRIPT
// SQL Workload RCA - Enterprise Edition v4.1
// For newly uploaded data ONLY
// ===============================================

let currentAnalysisData = null;
let newUploadData = null;
let detectedTimeWindow = null; // Auto-detected time window from AWR report

// ===============================================
// INITIALIZATION
// ===============================================
async function initializeNewResultsPage() {
  console.log("Initializing New Results Page...");

  // Load new upload results (time window is auto-detected from AWR)
  await loadNewUploadResults();
}

// ===============================================
// LOAD NEW UPLOAD RESULTS
// ===============================================
async function loadNewUploadResults() {
  try {
    // First check sessionStorage for upload data
    const storedData = sessionStorage.getItem("newUploadData");

    if (storedData) {
      newUploadData = JSON.parse(storedData);
      displayNewParsingResults(newUploadData);

      // Clear sessionStorage after displaying
      sessionStorage.removeItem("newUploadData");
    } else {
      // Fallback to API call
      const response = await fetch("/api/results", {
        method: "GET",
        credentials: "include",
        cache: "no-store",
      });

      if (!response.ok) {
        showNoDataMessage();
        return;
      }

      const result = await response.json();

      if (result.has_data && result.has_parsed_csv) {
        displayNewParsingResults(result);
      } else {
        showNoDataMessage();
      }
    }
  } catch (error) {
    console.error("Error loading new results:", error);
    showNoDataMessage();
  }
}

function showNoDataMessage() {
  document.getElementById("newUploadBanner").style.display = "none";

  const section = document.getElementById("parsingResultsSection");
  section.innerHTML = `
        <div class="card-header">
            <div class="card-title">
                <span class="title-icon">⚠️</span>
                <h2>No New Data</h2>
            </div>
        </div>
        <div class="card-body" style="text-align: center; padding: 3rem;">
            <p style="color: #94a3b8; margin-bottom: 1.5rem;">No new upload data found. Please upload AWR/ASH reports first.</p>
            <a href="/dashboard" class="btn-primary">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                    <polyline points="17,8 12,3 7,8"/>
                    <line x1="12" y1="3" x2="12" y2="15"/>
                </svg>
                Go to Dashboard
            </a>
        </div>
    `;
}

// ===============================================
// DISPLAY NEW PARSING RESULTS
// ===============================================
function displayNewParsingResults(data) {
  const summary = document.getElementById("parsingSummary");

  // Get accurate CSV count from multiple sources
  let csvCount = 0;
  let csvFiles = [];

  // Priority 1: Direct file list from parsing_results
  if (data.parsing_results && Array.isArray(data.parsing_results)) {
    csvFiles = data.parsing_results.filter(
      (f) => f.file && f.file.endsWith(".csv"),
    );

    // If parsing_results contains CSV file entries directly
    if (csvFiles.length > 0) {
      csvCount = csvFiles.length;
    } else {
      // Count from csv_list in each parsing result
      data.parsing_results.forEach((pr) => {
        if (pr.csv_list && Array.isArray(pr.csv_list)) {
          csvFiles.push(...pr.csv_list);
        }
      });
      csvCount = csvFiles.length;
    }
  }

  // Priority 2: Fallback to various count fields
  if (csvCount === 0) {
    csvCount =
      data.total_csv_files ||
      data.new_csv_files_generated ||
      data.parsed_csv_count ||
      data.csv_count ||
      data.csv_validation?.total_csv_files ||
      0;
  }

  // Priority 3: Get file list from csv_validation
  if (csvFiles.length === 0 && data.csv_validation?.csv_file_list) {
    csvFiles = data.csv_validation.csv_file_list;
    csvCount = csvFiles.length;
  }

  // Priority 4: Get from new_csv_files
  if (csvFiles.length === 0 && data.new_csv_files) {
    csvFiles = data.new_csv_files;
    csvCount = csvFiles.length;
  }

  // Update banner
  const uploadedCount =
    data.uploaded_files?.length ||
    data.parsing_results?.filter((p) => p.type !== "CSV").length ||
    0;
  document.getElementById("uploadSummaryText").textContent =
    `${uploadedCount} report(s) parsed → ${csvCount} CSV files generated`;

  // Build parsing summary
  let html = `
        <div class="parsing-grid">
            <div class="parsing-stat">
                <span class="parsing-stat-value">${csvCount}</span>
                <span class="parsing-stat-label">CSV Files Generated</span>
            </div>
            <div class="parsing-stat">
                <span class="parsing-stat-value">${uploadedCount}</span>
                <span class="parsing-stat-label">Reports Processed</span>
            </div>
        </div>
    `;

  // Show CSV file list
  if (csvFiles.length > 0) {
    html += `<div class="parsing-files-list">`;
    csvFiles.forEach((file) => {
      const fileName =
        typeof file === "string" ? file : file.file || file.name || "Unknown";
      html += `
                <div class="parsing-file-item success">
                    <span class="file-status-icon">✓</span>
                    <span class="file-name">${fileName}</span>
                    <span class="file-type badge-csv">CSV</span>
                </div>
            `;
    });
    html += `</div>`;
  }

  summary.innerHTML = html;

  // Show high load detection
  if (data.high_load_periods && data.high_load_periods.length > 0) {
    displayHighLoadDetection(data.high_load_periods);
  }

  // Show time window section
  document.getElementById("timeWindowSection").style.display = "block";
}

// ===============================================
// HIGH LOAD DETECTION DISPLAY
// ===============================================
function displayHighLoadDetection(highLoadPeriods) {
  const section = document.getElementById("highLoadSection");
  const content = document.getElementById("highLoadContent");

  // Filter AWR-based high load only (exclude ASH analysis text)
  const awrPeriods = highLoadPeriods.filter(
    (p) => p.details && !p.details.toLowerCase().includes("ash analysis"),
  );

  if (awrPeriods.length === 0) {
    content.innerHTML = `
            <div class="high-load-status normal">
                <span class="status-icon">✓</span>
                <div class="status-content">
                    <strong>No High Load Detected</strong>
                    <p>System performance within normal parameters during this period</p>
                </div>
            </div>
        `;
    // Even with no high load, use the first period's time window if available
    if (highLoadPeriods.length > 0 && highLoadPeriods[0].period) {
      setDetectedTimeWindow(highLoadPeriods[0].period);
    }
  } else {
    let html = "";
    awrPeriods.forEach((period, index) => {
      // Extract metrics from the period if available
      const metrics = period.metrics || {};

      // Parse details string to extract individual metrics
      const detailsStr = period.details || "";

      html += `
                <div class="high-load-alert">
                    <div class="alert-header">
                        <span class="alert-icon">🔺</span>
                        <strong>High Load Detected: ${period.period}</strong>
                    </div>
                    <div class="alert-metrics">
                        ${formatHighLoadMetrics(metrics, detailsStr)}
                    </div>
                </div>
            `;

      // Use the first AWR period's time window for RCA
      if (index === 0 && period.period) {
        setDetectedTimeWindow(period.period);
      }
    });
    content.innerHTML = html;
  }

  section.style.display = "block";
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

/**
 * Format High Load metrics for display.
 * Shows 3 metrics: elapsed time, executions, CPU%
 */
function formatHighLoadMetrics(metrics, detailsStr) {
  // Try to get values from metrics object first, then parse from details string
  let elapsedTime = metrics.total_elapsed_time_s || 0;
  let executions = metrics.total_executions || 0;
  let cpuPercent = metrics.cpu_percentage || 0;

  // Parse from details string if metrics not available
  if (detailsStr) {
    // Extract elapsed time
    const elapsedMatch = detailsStr.match(/Total elapsed time:\s*([\d.]+)s/i);
    if (elapsedMatch && elapsedTime === 0)
      elapsedTime = parseFloat(elapsedMatch[1]);

    // Extract executions
    const execMatch = detailsStr.match(/Total executions:\s*([\d,]+)/i);
    if (execMatch && executions === 0)
      executions = parseInt(execMatch[1].replace(/,/g, ""));

    // Extract CPU usage
    const cpuMatch = detailsStr.match(/(?:Max )?CPU usage:\s*([\d.]+)%/i);
    if (cpuMatch && cpuPercent === 0) cpuPercent = parseFloat(cpuMatch[1]);
  }

  // CRITICAL: Cap CPU at 100%
  cpuPercent = Math.min(100.0, cpuPercent);

  // Format for display - show 3 metrics
  return `
        <div class="metrics-grid">
            <div class="metric-item">
                <span class="metric-label">Total elapsed time:</span>
                <span class="metric-value">${elapsedTime.toFixed(1)}s</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Total executions:</span>
                <span class="metric-value">${executions.toLocaleString()}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">CPU Usage:</span>
                <span class="metric-value ${cpuPercent > 80 ? "high" : ""}">${cpuPercent.toFixed(1)}%</span>
            </div>
        </div>
    `;
}

// ===============================================
// RUN RCA ANALYSIS (Auto-detected time window)
// ===============================================
async function runRCA() {
  // Use the auto-detected time window from AWR report
  if (!detectedTimeWindow) {
    alert(
      "No analysis time window detected. Please ensure AWR data is loaded.",
    );
    return;
  }

  const rcaBtn = document.getElementById("runRcaBtn");
  const originalContent = rcaBtn.innerHTML;

  rcaBtn.disabled = true;
  rcaBtn.innerHTML = `
        <svg class="spinner" width="20" height="20" viewBox="0 0 24 24">
            <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2" fill="none" stroke-dasharray="60" stroke-dashoffset="20"/>
        </svg>
        Running Analysis...
    `;

  try {
    const response = await fetch("/api/run_rca", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        start_hour: detectedTimeWindow.start_hour,
        start_minute: detectedTimeWindow.start_minute,
        end_hour: detectedTimeWindow.end_hour,
        end_minute: detectedTimeWindow.end_minute,
        time_window: detectedTimeWindow.time_window,
      }),
    });

    const result = await response.json();

    if (response.ok) {
      currentAnalysisData = result;
      displayRCAResults(result);

      // Scroll to results
      setTimeout(() => {
        document
          .getElementById("dbaExpertSection")
          ?.scrollIntoView({ behavior: "smooth" });
      }, 100);
    } else {
      alert(result.detail || "RCA analysis failed");
    }
  } catch (error) {
    console.error("RCA error:", error);
    alert("Network error during RCA analysis");
  } finally {
    rcaBtn.disabled = false;
    rcaBtn.innerHTML = originalContent;
  }
}

// ===============================================
// RCA RESULTS DISPLAY
// ===============================================
function displayRCAResults(result) {
  // Update overview bar
  updateOverviewBar(result);

  // NOTE: Workload-level RCA section removed per DBA requirements
  // RCA is now shown at SQL-level inside each finding (Problem Summary renamed to Root Cause Analysis)
  // This provides clearer, per-SQL root cause analysis instead of generic workload-level summary

  // Display DBA Expert Analysis (contains SQL-level RCA)
  if (result.dba_expert_analysis) {
    displayDBAExpertAnalysis(result.dba_expert_analysis);
  }
}

// ===============================================
// WORKLOAD-LEVEL RCA SECTION REMOVED
// ===============================================
// Per DBA requirements, workload-level RCA (MIXED_WORKLOAD, LOW CONFIDENCE, etc.)
// is removed. RCA is now shown at SQL-level inside each finding.
// The displayRootCauseSection function has been intentionally removed.

function updateOverviewBar(result) {
  const bar = document.getElementById("resultsOverviewBar");
  const dba = result.dba_expert_analysis || {};
  const summary = dba.workload_summary || {};

  document.getElementById("analysisWindowDisplay").textContent =
    result.analysis_window || "--";
  document.getElementById("sqlAnalyzedCount").textContent =
    summary.sql_analyzed || 0;
  document.getElementById("problematicCount").textContent =
    summary.problematic_found || 0;
  document.getElementById("totalElapsedDisplay").textContent =
    `${summary.total_elapsed_s || 0}s`;

  bar.style.display = "block";
}

function _legacy_displayDBAExpertAnalysis(dbaAnalysis) {
  const section = document.getElementById("dbaExpertSection");
  const content = document.getElementById("dbaExpertContent");
  const patternBadge = document.getElementById("patternBadge");

  const summary = dbaAnalysis.workload_summary || {};
  const findings = dbaAnalysis.problematic_sql_findings || [];

  patternBadge.textContent = summary.pattern || "Unknown Pattern";

  let html = "";

  // Workload Summary Grid
  html += `
        <div class="analysis-summary-grid">
            <div class="summary-item">
                <span class="item-label">Pattern</span>
                <span class="item-value">${summary.pattern || "N/A"}</span>
            </div>
            <div class="summary-item">
                <span class="item-label">Total Elapsed</span>
                <span class="item-value">${summary.total_elapsed_s || 0}s</span>
            </div>
            <div class="summary-item">
                <span class="item-label">SQL Analyzed</span>
                <span class="item-value">${summary.sql_analyzed || 0}</span>
            </div>
            <div class="summary-item highlight">
                <span class="item-label">Problematic Found</span>
                <span class="item-value">${summary.problematic_found || 0}</span>
            </div>
        </div>
    `;

  // SQL Findings
  if (findings.length > 0) {
    html += `<div class="sql-findings-list">`;

    findings.forEach((finding, index) => {
      const severityClass = (finding.severity || "medium").toLowerCase();
      const sqlId = finding.sql_id || `SQL_${index + 1}`;
      const fixPanelId = `fix-panel-${index}`;

      // 🔥 Check for Load Reduction Actions directly
      const lra = finding.load_reduction_actions;
      const hasLRA = lra && lra.actions && lra.actions.length > 0;

      html += `
                <div class="sql-finding-card" id="finding-${index}">
                    <div class="finding-header">
                        <div class="finding-title">
                            <span class="finding-number">Finding #${index + 1}</span>
                            <span class="finding-sql-id">${sqlId}</span>
                        </div>
                        <span class="severity-badge ${severityClass}">${finding.severity || "MEDIUM"} PRIORITY</span>
                    </div>
                    
                    <!-- 🎯 ROOT CAUSE ANALYSIS (SQL-Level) - IMMEDIATELY AFTER SQL ID -->
                    <div class="finding-section root-cause-analysis" style="background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, rgba(220, 38, 38, 0.05) 100%); border: 2px solid #ef4444; border-radius: 12px; margin: 16px 0;">
                        <div class="section-header" style="background: rgba(239, 68, 68, 0.15); padding: 12px 16px; border-radius: 10px 10px 0 0;">
                            <span class="section-icon" style="font-size: 1.3rem;">🎯</span>
                            <span class="section-title" style="font-size: 1.1rem; font-weight: 700; color: #ef4444;">Root Cause Analysis</span>
                        </div>
                        <div class="section-content" style="padding: 16px;">
                            ${formatRootCauseAnalysis(finding)}
                        </div>
                    </div>
                    
                    <!-- 🔥 LOAD REDUCTION ACTIONS - ACTIONABLE DBA TASKS -->
                    ${
                      hasLRA
                        ? `
                        <div class="load-reduction-primary" style="background: linear-gradient(135deg, rgba(34, 197, 94, 0.15) 0%, rgba(16, 185, 129, 0.15) 100%); border: 3px solid #22c55e; border-radius: 12px; margin: 16px 0; overflow: hidden;">
                            <div style="background: linear-gradient(135deg, rgba(34, 197, 94, 0.3) 0%, rgba(16, 185, 129, 0.3) 100%); padding: 16px 20px; display: flex; align-items: center; gap: 12px;">
                                <span style="font-size: 1.5rem;">⚡</span>
                                <span style="flex: 1; font-size: 1.1rem; font-weight: 700; color: #22c55e; text-transform: uppercase;">DBA Actions to Reduce Database Load</span>
                                <span style="background: linear-gradient(135deg, #22c55e 0%, #10b981 100%); color: white; padding: 6px 14px; border-radius: 20px; font-size: 0.9rem; font-weight: 600;">${lra.total_actions} Actions</span>
                            </div>
                            <div style="padding: 16px;">
                                ${lra.actions
                                  .map(
                                    (action) => `
                                    <div style="background: rgba(0,0,0,0.3); border-left: 4px solid ${action.category === "IO_DOMINANT" ? "#ef4444" : action.category === "MISSING_INDEX" ? "#f59e0b" : action.category === "HIGH_CPU" ? "#ef4444" : "#8b5cf6"}; padding: 16px; margin: 12px 0; border-radius: 0 8px 8px 0;">
                                        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 12px;">
                                            <h5 style="flex: 1; margin: 0; color: #f1f5f9; font-size: 1rem;">${action.title}</h5>
                                            <span style="background: #3b82f6; color: white; padding: 2px 10px; border-radius: 12px; font-size: 0.75rem;">Priority ${action.priority}</span>
                                        </div>
                                        <div style="margin-bottom: 12px;">
                                            <strong style="color: #94a3b8;">Why This Helps:</strong>
                                            <p style="color: #cbd5e1; margin: 4px 0;">${action.why_this_helps}</p>
                                        </div>
                                        <div>
                                            <strong style="color: #94a3b8;">📋 DBA-Executable Queries:</strong>
                                            ${action.sql_queries
                                              .map(
                                                (q) => `
                                                <div style="position: relative; margin: 8px 0;">
                                                    <pre style="background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 12px; overflow-x: auto; font-family: Consolas, Monaco, monospace; font-size: 0.8rem; color: #94a3b8; white-space: pre-wrap;"><code>${q.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</code></pre>
                                                    <button onclick="navigator.clipboard.writeText(this.previousElementSibling.textContent); this.textContent='✓ Copied!'; setTimeout(() => this.textContent='📋 Copy', 1500);" style="position: absolute; top: 8px; right: 8px; background: #3b82f6; color: white; border: none; padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 0.75rem;">📋 Copy</button>
                                                </div>
                                            `,
                                              )
                                              .join("")}
                                        </div>
                                        <div style="background: rgba(59, 130, 246, 0.1); border-radius: 8px; padding: 12px; margin-top: 12px;">
                                            <span>💡</span> <strong>Recommendation:</strong>
                                            <p style="margin: 4px 0; color: #cbd5e1;">${action.dba_action_text}</p>
                                        </div>
                                    </div>
                                `,
                                  )
                                  .join("")}
                            </div>
                        </div>
                    `
                        : ""
                    }
                    
                    <!-- DBA Assessment -->
                    ${
                      finding.execution_pattern?.dba_assessment
                        ? `
                        <div class="finding-section dba-assessment">
                            <div class="section-header">
                                <span class="section-icon">📝</span>
                                <span class="section-title">DBA Assessment</span>
                            </div>
                            <div class="section-content">
                                <p>${finding.execution_pattern.dba_assessment}</p>
                            </div>
                        </div>
                    `
                        : ""
                    }
                    
                    <!-- DBA Interpretation - Hidden per DBA requirements (single RCA shown above) -->
                    
                    <!-- Fix Button - Collapsed by Default -->
                    <div class="fix-button-container">
                        <button class="btn-fix" onclick="toggleFixPanel('${fixPanelId}')">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/>
                            </svg>
                            <span>Show Fix Recommendations</span>
                            <svg class="chevron" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="6,9 12,15 18,9"/>
                            </svg>
                        </button>
                    </div>
                    
                    <!-- Fix Panel - Hidden by Default -->
                    <div class="fix-panel collapsed" id="${fixPanelId}">
                        ${formatFixRecommendations(finding.recommendations || finding.dba_recommendations, sqlId, finding)}
                    </div>
                </div>
            `;
    });

    html += `</div>`;
  } else {
    html += `
            <div class="no-findings-message">
                <span class="message-icon">✅</span>
                <h3>No Problematic SQL Identified</h3>
                <p>Database workload appears normal for the selected time window.</p>
            </div>
        `;
  }

  // DBA Conclusion
  if (dbaAnalysis.dba_conclusion) {
    html += `
            <div class="dba-conclusion-section">
                <div class="section-header">
                    <span class="section-icon">🎯</span>
                    <span class="section-title">Final DBA Conclusion</span>
                </div>
                <div class="conclusion-content">
                    ${formatMarkdown(dbaAnalysis.dba_conclusion)}
                </div>
            </div>
        `;
  }

  content.innerHTML = html;
  section.style.display = "block";
}

// ===============================================
// FIX PANEL TOGGLE
// ===============================================
function toggleFixPanel(panelId) {
  const panel = document.getElementById(panelId);
  const btn = panel.previousElementSibling.querySelector(".btn-fix");

  if (panel.classList.contains("collapsed")) {
    panel.classList.remove("collapsed");
    panel.classList.add("expanded");
    btn.classList.add("active");
    btn.querySelector("span").textContent = "Hide Fix Recommendations";

    // Smooth scroll to panel
    setTimeout(() => {
      panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }, 100);
  } else {
    panel.classList.remove("expanded");
    panel.classList.add("collapsed");
    btn.classList.remove("active");
    btn.querySelector("span").textContent = "Show Fix Recommendations";
  }
}

// ===============================================
// FORMATTING FUNCTIONS
// ===============================================

// ===============================================
// SQL-LEVEL ROOT CAUSE ANALYSIS (Single Dominant Cause)
// ===============================================
function formatRootCauseAnalysis(finding) {
  // Determine the PRIMARY root cause (ONE only, decisive)
  const techParams = finding.technical_parameters || {};

  // DEBUG: Log what we're receiving from backend
  console.log("DEBUG formatRootCauseAnalysis - finding:", finding);
  console.log("DEBUG technical_parameters:", techParams);
  console.log("DEBUG cpu_percentage:", techParams.cpu_percentage);
  console.log("DEBUG io_percentage:", techParams.io_percentage);

  // Use correct backend key names with fallbacks for compatibility
  const cpuPct =
    parseFloat(techParams.cpu_percentage || techParams.cpu_pct) || 0;
  const ioPct = parseFloat(techParams.io_percentage || techParams.io_pct) || 0;
  const dbTimePct =
    parseFloat(
      techParams.contribution_to_db_time_pct || techParams.db_time_pct,
    ) || 0;
  const executions = techParams.executions || 0;
  const avgExecTime =
    techParams.avg_time || techParams.avg_elapsed_per_exec_s || 0;

  console.log(
    "DEBUG parsed values - cpuPct:",
    cpuPct,
    "ioPct:",
    ioPct,
    "dbTimePct:",
    dbTimePct,
  );

  // Determine PRIMARY cause (only ONE)
  let primaryCause = "";
  let primaryIcon = "";
  let primaryColor = "";
  let explanation = "";

  // Decision tree for single root cause (no multiple labels)
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
    explanation = `This SQL contributes to both CPU and IO load. Neither CPU (${cpuPct}%) nor IO (${ioPct}%) is dominant, indicating mixed execution pressure across ${executions} executions.`;
  }

  // Also extract from problem_summary if available
  const problemSummary = finding.problem_summary || "";

  return `
    <!-- Primary Root Cause (ONE ONLY) -->
    <div style="display: flex; align-items: flex-start; gap: 16px; margin-bottom: 16px;">
      <div style="background: ${primaryColor}22; border: 2px solid ${primaryColor}; border-radius: 12px; padding: 16px 20px; display: flex; align-items: center; gap: 12px; min-width: 200px;">
        <span style="font-size: 2rem;">${primaryIcon}</span>
        <div>
          <div style="font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px;">Primary Cause</div>
          <div style="font-size: 1.25rem; font-weight: 700; color: ${primaryColor};">${primaryCause}</div>
        </div>
      </div>
    </div>
    
    <!-- Explanation -->
    <div style="background: rgba(0,0,0,0.2); border-radius: 8px; padding: 16px; margin-bottom: 16px;">
      <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 8px;">📋 Explanation</div>
      <p style="color: #e2e8f0; margin: 0; line-height: 1.6;">${explanation}</p>
    </div>
    
    <!-- Evidence / Quick Stats -->
    <div style="background: rgba(0,0,0,0.15); border-radius: 8px; padding: 16px;">
      <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 12px;">📊 Evidence</div>
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px;">
        ${dbTimePct ? `<div style="text-align: center; padding: 8px; background: rgba(239,68,68,0.1); border-radius: 8px;"><div style="font-size: 1.25rem; font-weight: 700; color: #ef4444;">${dbTimePct}%</div><div style="font-size: 0.75rem; color: #94a3b8;">DB Time</div></div>` : ""}
        ${cpuPct ? `<div style="text-align: center; padding: 8px; background: rgba(239,68,68,0.1); border-radius: 8px;"><div style="font-size: 1.25rem; font-weight: 700; color: #f59e0b;">${cpuPct}%</div><div style="font-size: 0.75rem; color: #94a3b8;">CPU</div></div>` : ""}
        ${ioPct ? `<div style="text-align: center; padding: 8px; background: rgba(245,158,11,0.1); border-radius: 8px;"><div style="font-size: 1.25rem; font-weight: 700; color: #f59e0b;">${ioPct}%</div><div style="font-size: 0.75rem; color: #94a3b8;">IO Wait</div></div>` : ""}
        ${executions ? `<div style="text-align: center; padding: 8px; background: rgba(59,130,246,0.1); border-radius: 8px;"><div style="font-size: 1.25rem; font-weight: 700; color: #3b82f6;">${executions}</div><div style="font-size: 0.75rem; color: #94a3b8;">Executions</div></div>` : ""}
        ${avgExecTime ? `<div style="text-align: center; padding: 8px; background: rgba(139,92,246,0.1); border-radius: 8px;"><div style="font-size: 1.25rem; font-weight: 700; color: #8b5cf6;">${avgExecTime}s</div><div style="font-size: 0.75rem; color: #94a3b8;">Avg Exec</div></div>` : ""}
      </div>
    </div>
    
    <!-- Problem Summary (if available) -->
    ${
      problemSummary
        ? `
    <div style="margin-top: 16px; padding-top: 16px; border-top: 1px solid rgba(255,255,255,0.1);">
      <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 8px;">📝 Details</div>
      <div style="color: #cbd5e1; line-height: 1.6;">${formatMarkdown(problemSummary)}</div>
    </div>
    `
        : ""
    }
  `;
}

function formatProblemSummary(summary) {
  if (!summary) return "<p>No summary available</p>";
  return formatMarkdown(summary);
}

function formatDBAInterpretation(interpretation) {
  if (!interpretation) return "<p>No interpretation available</p>";

  const lines = interpretation.split("\n");
  let html = "";

  // Regex to strip leading emojis from title
  const emojiRegex =
    /^[\u{1F300}-\u{1F9FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}\u{1F600}-\u{1F64F}\u{1F680}-\u{1F6FF}\s]+/u;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    // Check for **Title** pattern
    const titleMatch = line.match(/^\*\*(.*?)\*\*$/);
    if (titleMatch) {
      let rawTitle = titleMatch[1].trim();

      // Strip any leading emojis from the title (they come from backend)
      let title = rawTitle.replace(emojiRegex, "").trim();

      // Get the appropriate icon based on cleaned title
      let icon = getInterpretationIcon(title || rawTitle);

      // Get description (next non-empty line)
      let description = "";
      for (let j = i + 1; j < lines.length; j++) {
        const nextLine = lines[j].trim();
        if (!nextLine) continue;
        if (nextLine.match(/^\*\*.*\*\*$/)) break;
        description = nextLine;
        i = j;
        break;
      }

      html += `
                <div class="interpretation-item">
                    <div class="interpretation-header">
                        <span class="interpretation-icon">${icon}</span>
                        <span class="interpretation-title">${title || rawTitle}</span>
                    </div>
                    <p class="interpretation-desc">${description}</p>
                </div>
            `;
    }
  }

  return html || formatMarkdown(interpretation);
}

function getInterpretationIcon(title) {
  const t = title.toLowerCase();
  if (t.includes("cpu")) return "🔴";
  if (t.includes("io") || t.includes("i/o")) return "🟠";
  if (t.includes("parallel")) return "🟡";
  if (t.includes("batch") || t.includes("slow")) return "🔴";
  if (t.includes("frequency")) return "🔵";
  return "🔍";
}

function formatFixRecommendations(recs, sqlId, finding) {
  // ⚡ FIRST: Check for new fix_recommendations structure (signal-based)
  const fixRecs = finding?.fix_recommendations;
  if (fixRecs && fixRecs.fix_sections && fixRecs.fix_sections.length > 0) {
    return renderFixRecommendationsSections(fixRecs, sqlId);
  }

  // Generate dynamic recommendations if none provided
  if (!recs || (typeof recs === "object" && Object.keys(recs).length === 0)) {
    return generateDynamicRecommendations(sqlId, finding);
  }

  // Handle string format recommendations (from some engines)
  if (typeof recs === "string") {
    return `<div style="padding: 16px;">${formatRecommendationContent(recs, sqlId)}</div>`;
  }

  let html = "";

  // Priority Description - styled like working section
  if (recs.priority_description) {
    html += `
            <div style="background: linear-gradient(135deg, rgba(245, 158, 11, 0.2), rgba(234, 88, 12, 0.1)); border-left: 4px solid #f59e0b; padding: 12px 16px; margin: 12px 0; border-radius: 0 8px 8px 0;">
                <span style="font-size: 1.2rem; margin-right: 8px;">⚠️</span>
                <span style="color: #fcd34d; font-weight: 500;">${recs.priority_description}</span>
            </div>
        `;
  }

  // What DBA Should Do Next - styled like DBA Actions section
  if (recs.what_dba_should_do_next) {
    html += `
            <div style="background: rgba(0,0,0,0.3); border-left: 4px solid #3b82f6; padding: 16px; margin: 12px 0; border-radius: 0 8px 8px 0;">
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 12px;">
                    <span style="font-size: 1.3rem;">🛠️</span>
                    <h4 style="flex: 1; margin: 0; color: #3b82f6; font-size: 1.1rem; font-weight: 600;">What DBA Should Do Next</h4>
                </div>
                <div style="color: #cbd5e1;">
                    ${formatRecommendationContent(recs.what_dba_should_do_next, sqlId)}
                </div>
            </div>
        `;
  }

  // DBA Action Plan - styled like working section
  if (recs.dba_action_plan) {
    html += `
            <div style="background: rgba(0,0,0,0.3); border-left: 4px solid #22c55e; padding: 16px; margin: 12px 0; border-radius: 0 8px 8px 0;">
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 12px;">
                    <span style="font-size: 1.3rem;">📋</span>
                    <h4 style="flex: 1; margin: 0; color: #22c55e; font-size: 1.1rem; font-weight: 600;">DBA Action Plan</h4>
                </div>
                <div style="color: #cbd5e1;">
                    ${formatActionPlan(recs.dba_action_plan)}
                </div>
            </div>
        `;
  } else {
    // Generate dynamic action plan if not present
    html += generateDynamicActionPlan(sqlId, finding);
  }

  // Expected Improvement - styled like recommendation box
  if (recs.expected_improvement) {
    html += `
            <div style="background: rgba(59, 130, 246, 0.1); border-radius: 8px; padding: 12px 16px; margin-top: 12px; display: flex; align-items: flex-start; gap: 10px;">
                <span style="font-size: 1.2rem;">💡</span>
                <div>
                    <strong style="color: #3b82f6;">Expected Improvement:</strong>
                    <p style="margin: 4px 0 0 0; color: #cbd5e1;">${recs.expected_improvement}</p>
                </div>
            </div>
        `;
  }

  // If still no content, generate dynamic
  if (!html.trim()) {
    return generateDynamicRecommendations(sqlId, finding);
  }

  return html;
}

// ⚡ NEW: Render Fix Recommendations with DBA Action Plan style - EXACT SAME FORMAT
function renderFixRecommendationsSections(fixRecs, _sqlId) {
  const detectedIssues = fixRecs.detected_issues || [];
  const sections = fixRecs.fix_sections || [];
  const totalSections = sections.length;

  // Use EXACT same inline styles as "DBA ACTIONS TO REDUCE DATABASE LOAD"
  let html = `
        <div style="background: linear-gradient(135deg, rgba(139, 92, 246, 0.15) 0%, rgba(99, 102, 241, 0.15) 100%); border: 3px solid #8b5cf6; border-radius: 12px; margin: 16px 0; overflow: hidden;">
            <div style="background: linear-gradient(135deg, rgba(139, 92, 246, 0.3) 0%, rgba(99, 102, 241, 0.3) 100%); padding: 16px 20px; display: flex; align-items: center; gap: 12px;">
                <span style="font-size: 1.5rem;">🔧</span>
                <span style="flex: 1; font-size: 1.1rem; font-weight: 700; color: #a78bfa; text-transform: uppercase;">Fix Recommendations</span>
                <span style="background: linear-gradient(135deg, #8b5cf6 0%, #6366f1 100%); color: white; padding: 6px 14px; border-radius: 20px; font-size: 0.9rem; font-weight: 600;">${totalSections} Action${totalSections > 1 ? "s" : ""}</span>
            </div>
            <div style="padding: 16px;">
                <div style="margin-bottom: 16px;">
                    <strong style="color: #a78bfa;">🎯 Detected Issues:</strong>
                    ${detectedIssues.map((issue) => `<span style="display: inline-block; background: rgba(139, 92, 246, 0.2); color: #a78bfa; padding: 4px 12px; border-radius: 20px; margin: 4px; font-size: 0.85rem;">${formatIssueName(issue)}</span>`).join("")}
                </div>
    `;

  // Render each fix section with EXACT same styling as load_reduction_actions
  for (const section of sections) {
    const categoryColor = getCategoryColor(section.category);
    const icon = section.section_icon || "🔧";

    html += `
                <div style="background: rgba(0,0,0,0.3); border-left: 4px solid ${categoryColor}; padding: 16px; margin: 12px 0; border-radius: 0 8px 8px 0;">
                    <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 12px;">
                        <span style="font-size: 1.3rem;">${icon}</span>
                        <h5 style="flex: 1; margin: 0; color: #f1f5f9; font-size: 1rem;">${section.section_title}</h5>
                        <span style="background: #3b82f6; color: white; padding: 2px 10px; border-radius: 12px; font-size: 0.75rem;">${section.priority_tag}</span>
                    </div>
                    <div style="margin-bottom: 12px;">
                        <strong style="color: #94a3b8;">Why This Helps:</strong>
                        <p style="color: #cbd5e1; margin: 4px 0;">${section.why_shown}</p>
                    </div>
                    <div>
                        <strong style="color: #94a3b8;">📋 DBA-Executable Queries:</strong>
        `;

    // Render each step's SQL with COPY button
    for (const step of section.steps) {
      const escapedSql = step.sql_code
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
      html += `
                        <div style="position: relative; margin: 8px 0;">
                            <div style="color: #60a5fa; font-size: 0.85rem; margin-bottom: 4px;">-- ▶ ${step.title}</div>
                            <pre style="background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 12px; padding-right: 70px; overflow-x: auto; font-family: Consolas, Monaco, monospace; font-size: 0.8rem; color: #94a3b8; white-space: pre-wrap; margin: 0;"><code>${escapedSql}</code></pre>
                            <button onclick="navigator.clipboard.writeText(this.previousElementSibling.textContent); this.textContent='✓ Copied!'; setTimeout(() => this.textContent='📋 Copy', 1500);" style="position: absolute; top: 28px; right: 8px; background: #3b82f6; color: white; border: none; padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 0.75rem;">📋 Copy</button>
                        </div>
            `;
    }

    html += `
                    </div>
                    <div style="background: rgba(59, 130, 246, 0.1); border-radius: 8px; padding: 12px; margin-top: 12px;">
                        <span>💡</span> <strong>Recommendation:</strong>
                        <p style="margin: 4px 0; color: #cbd5e1;">${section.expected_improvement}</p>
                    </div>
                </div>
        `;
  }

  html += `
            </div>
        </div>
    `;

  return html;
}

// Helper: Get category-specific color
function getCategoryColor(category) {
  const colorMap = {
    IO_REDUCTION: "#ef4444",
    SQL_ACCESS_ADVISOR: "#f59e0b",
    PARALLEL_EXECUTION: "#3b82f6",
    PLAN_STABILITY: "#8b5cf6",
    CPU_REDUCTION: "#f97316",
  };
  return colorMap[category] || "#64748b";
}

// Helper: Format issue name for display
function formatIssueName(issue) {
  const nameMap = {
    IO_DOMINANT: "🔴 IO Dominant",
    HIGH_CPU: "🔥 High CPU",
    BATCH_PATTERN: "📊 Batch Pattern",
    PLAN_INSTABILITY: "📌 Plan Instability",
    HIGH_IMPACT: "⚡ High Impact",
  };
  return nameMap[issue] || issue.replace(/_/g, " ");
}

// Helper: Escape HTML for safe display
function escapeHtml(text) {
  if (!text) return "";
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// Helper: Copy to clipboard
function copyToClipboard(button) {
  const codeBlock = button.previousElementSibling;
  const code = codeBlock.querySelector("code");
  const text = code.textContent;

  navigator.clipboard
    .writeText(text)
    .then(() => {
      const original = button.textContent;
      button.textContent = "✅ Copied!";
      button.classList.add("copied");
      setTimeout(() => {
        button.textContent = original;
        button.classList.remove("copied");
      }, 2000);
    })
    .catch((err) => {
      console.error("Copy failed:", err);
    });
}

// Generate dynamic recommendations based on SQL finding data
function generateDynamicRecommendations(sqlId, finding) {
  const severity = finding?.severity || "HIGH";
  const execPattern = finding?.execution_pattern || {};
  const interpretation = finding?.dba_interpretation || "";

  // Extract metrics from finding
  const totalElapsed = execPattern.total_elapsed || 0;
  const executions = execPattern.total_executions || 0;
  const avgElapsed = execPattern.avg_elapsed || 0;
  const cpuPct = execPattern.cpu_percent || 0;

  // Analyze interpretation text to determine issue type
  const interpLower = interpretation.toLowerCase();
  const hasIOIssue =
    interpLower.includes("i/o") ||
    interpLower.includes("io-heavy") ||
    interpLower.includes("disk read");
  const hasCPUIssue =
    interpLower.includes("cpu") || interpLower.includes("compute");
  const hasFullScan =
    interpLower.includes("full scan") || interpLower.includes("full table");
  const hasMissingIndex =
    interpLower.includes("missing index") || interpLower.includes("index");
  const hasStaleStats =
    interpLower.includes("stale") || interpLower.includes("statistics");
  const hasHighFreq =
    interpLower.includes("frequency") || interpLower.includes("high frequency");
  const hasBatchQuery =
    interpLower.includes("batch") ||
    interpLower.includes("report query") ||
    interpLower.includes("heavyweight");

  let html = "";

  // Priority Banner
  const priorityBanners = {
    CRITICAL:
      "🔴 <strong>CRITICAL</strong> - Production impacting, requires immediate action",
    HIGH: "🟠 <strong>HIGH</strong> - Major performance drain, address within 24 hours",
    MEDIUM:
      "🟡 <strong>MEDIUM</strong> - Notable impact, schedule tuning this week",
  };

  html += `
        <div class="fix-priority-banner">
            <span class="priority-icon">⚠️</span>
            ${priorityBanners[severity] || priorityBanners["HIGH"]}
        </div>
    `;

  html += `<div class="fix-section"><div class="fix-section-header"><span class="fix-icon">🛠️</span><h4>What DBA Should Do Next</h4></div><div class="fix-section-content">`;

  // 1. ALWAYS: First step - Capture execution plan for this SQL_ID
  html += `
        <h5 class="rec-subheader">🔍 Step 1: Capture Execution Plan for SQL_ID ${sqlId}</h5>
        <pre class="sql-code compact"><code>-- Get current execution plan with runtime statistics
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('${sqlId}', NULL, 'ALLSTATS LAST'));

-- If cursor not in cache, check AWR
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_AWR('${sqlId}'));</code></pre>
        <p class="action-item">✅ Look for: TABLE ACCESS FULL, NESTED LOOPS on large tables, high A-Rows vs E-Rows discrepancy</p>
    `;

  // 2. Based on I/O issues - Missing index / Full scan
  if (hasIOIssue || hasFullScan || hasMissingIndex) {
    html += `
            <h5 class="rec-subheader">🔴 Step 2: Address I/O-Heavy Full Table Scans</h5>
            <p><strong>Root Cause:</strong> High I/O indicates missing indexes forcing full table scans.</p>
            <pre class="sql-code compact"><code>-- Identify tables being scanned for this SQL
SELECT p.object_owner, p.object_name, p.operation, p.options
FROM v$sql_plan p
WHERE p.sql_id = '${sqlId}'
  AND p.operation = 'TABLE ACCESS' 
  AND p.options = 'FULL';

-- Check index candidates from execution plan
SELECT DISTINCT referenced_name AS table_name, referenced_column AS column_name
FROM dba_dependencies d
WHERE d.name IN (
  SELECT object_name FROM v$sql_plan WHERE sql_id = '${sqlId}'
);</code></pre>
            <p class="action-item">✅ <strong>Action:</strong> Create composite index on high-selectivity columns in WHERE/JOIN clauses:</p>
            <pre class="sql-code compact"><code>-- Example: Create index after identifying columns from plan
CREATE INDEX idx_${sqlId.substring(0, 8)}_opt ON schema.table_name(column1, column2)
TABLESPACE USERS ONLINE NOLOGGING;

-- Verify index usage
SELECT index_name, visibility, status FROM dba_indexes 
WHERE table_name = 'TABLE_NAME';</code></pre>
        `;
  }

  // 3. Based on CPU issues
  if (hasCPUIssue || cpuPct > 30) {
    html += `
            <h5 class="rec-subheader">🟠 Step ${hasIOIssue ? "3" : "2"}: Reduce CPU Overhead</h5>
            <p><strong>Root Cause:</strong> High CPU (${cpuPct.toFixed(1)}%) from complex operations or suboptimal join methods.</p>
            <pre class="sql-code compact"><code>-- Check if hash joins are causing CPU pressure
SELECT operation, options, cost, cpu_cost, io_cost
FROM v$sql_plan 
WHERE sql_id = '${sqlId}'
ORDER BY id;

-- Force more efficient join method if needed
SELECT /*+ USE_HASH(t1 t2) PARALLEL(t1, 4) */ ...
-- Or force nested loops for small result sets:
SELECT /*+ USE_NL(t1 t2) INDEX(t2 idx_name) */ ...</code></pre>
            <p class="action-item">✅ <strong>Action:</strong> Review join order and consider adding hints for optimal join method</p>
        `;
  }

  // 4. High frequency queries - Application issue
  if (hasHighFreq || (executions > 1000 && avgElapsed < 0.1)) {
    html += `
            <h5 class="rec-subheader">🔵 Step ${(hasIOIssue ? 3 : 2) + (hasCPUIssue ? 1 : 0)}: Address High Execution Frequency</h5>
            <p><strong>Root Cause:</strong> ${executions.toLocaleString()} executions indicate application-level inefficiency (loops, no caching).</p>
            <pre class="sql-code compact"><code>-- Verify execution pattern
SELECT sql_id, executions, elapsed_time/1e6 total_sec, 
       elapsed_time/NULLIF(executions,0)/1e6 avg_sec,
       rows_processed, buffer_gets
FROM v$sql 
WHERE sql_id = '${sqlId}';

-- Check if cursor sharing can help (if literals detected)
SELECT COUNT(*), sql_text 
FROM v$sql 
WHERE force_matching_signature = (
  SELECT force_matching_signature FROM v$sql WHERE sql_id = '${sqlId}'
) GROUP BY sql_text;</code></pre>
            <p class="action-item">✅ <strong>Action:</strong> Work with application team to implement:</p>
            <ul class="rec-list">
                <li>Result caching at application layer</li>
                <li>Batch processing instead of row-by-row</li>
                <li>Connection pooling optimization</li>
            </ul>
        `;
  }

  // 5. Stale statistics
  if (hasStaleStats || totalElapsed > 50) {
    html += `
            <h5 class="rec-subheader">📊 Verify & Refresh Optimizer Statistics</h5>
            <p><strong>Root Cause:</strong> Stale statistics cause optimizer to choose suboptimal plans.</p>
            <pre class="sql-code compact"><code>-- Check statistics freshness for tables in this SQL
SELECT table_name, last_analyzed, num_rows, stale_stats, 
       ROUND((SYSDATE - last_analyzed)) days_old
FROM dba_tab_statistics
WHERE table_name IN (
  SELECT DISTINCT object_name FROM v$sql_plan 
  WHERE sql_id = '${sqlId}' AND object_type = 'TABLE'
)
ORDER BY last_analyzed NULLS FIRST;

-- Gather fresh statistics
BEGIN
  DBMS_STATS.GATHER_TABLE_STATS(
    ownname          => 'SCHEMA_NAME',
    tabname          => 'TABLE_NAME',
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
    method_opt       => 'FOR ALL COLUMNS SIZE AUTO',
    cascade          => TRUE,
    degree           => 4
  );
END;
/</code></pre>
            <p class="action-item">✅ <strong>Action:</strong> Schedule statistics gathering job if tables change frequently</p>
        `;
  }

  // 6. For batch/report queries - SQL Tuning Advisor
  if (hasBatchQuery || severity === "CRITICAL" || totalElapsed > 100) {
    html += `
            <h5 class="rec-subheader">🤖 Run SQL Tuning Advisor (Recommended for Heavy Queries)</h5>
            <pre class="sql-code compact"><code>-- Create and execute tuning task
DECLARE
  l_task_name VARCHAR2(30) := 'TUNE_${sqlId}';
  l_sql_id    VARCHAR2(13) := '${sqlId}';
BEGIN
  -- Create tuning task
  l_task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(
    sql_id          => l_sql_id,
    scope           => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,
    time_limit      => 300,
    task_name       => l_task_name,
    description     => 'Tuning task for problematic SQL ${sqlId}'
  );
  
  -- Execute the task
  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name => l_task_name);
END;
/

-- View recommendations
SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK('TUNE_${sqlId}') AS recommendations FROM DUAL;</code></pre>
            <p class="action-item">✅ <strong>Action:</strong> Review and implement SQL Profile if recommended by advisor</p>
        `;
  }

  // 7. Plan stability - if high impact
  if (severity === "CRITICAL" || severity === "HIGH") {
    html += `
            <h5 class="rec-subheader">📌 Lock Good Execution Plan (SQL Plan Baseline)</h5>
            <p><strong>Why:</strong> Prevent plan regression after optimizer changes.</p>
            <pre class="sql-code compact"><code>-- Load current plan into SQL Plan Baseline
DECLARE
  l_plans_loaded PLS_INTEGER;
BEGIN
  l_plans_loaded := DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(
    sql_id          => '${sqlId}',
    plan_hash_value => NULL,  -- Load all plans
    enabled         => 'YES',
    fixed           => 'NO'
  );
  DBMS_OUTPUT.PUT_LINE('Plans loaded: ' || l_plans_loaded);
END;
/

-- Verify baseline created
SELECT sql_handle, plan_name, enabled, accepted, fixed
FROM dba_sql_plan_baselines
WHERE signature = (SELECT exact_matching_signature FROM v$sql WHERE sql_id = '${sqlId}' AND ROWNUM = 1);</code></pre>
        `;
  }

  html += `</div></div>`;

  // Generate DBA Action Plan
  html += generateDynamicActionPlan(sqlId, finding);

  // Expected Improvement based on analysis
  let improvementText = "";
  if (hasIOIssue && hasMissingIndex) {
    improvementText = "40-60% reduction in elapsed time after proper indexing";
  } else if (hasCPUIssue) {
    improvementText = "30-50% CPU reduction with optimized join methods";
  } else if (hasHighFreq) {
    improvementText = "50-70% load reduction with application-side caching";
  } else if (hasStaleStats) {
    improvementText = "20-40% improvement after statistics refresh";
  } else {
    improvementText =
      "30-50% performance improvement with recommended optimizations";
  }

  html += `
        <div class="fix-improvement">
            <span class="improvement-icon">💡</span>
            <strong>Expected Improvement:</strong> ${improvementText}
        </div>
    `;

  return html;
}

// Generate dynamic DBA Action Plan based on RCA findings
function generateDynamicActionPlan(sqlId, finding) {
  const interpretation = finding?.dba_interpretation || "";
  const execPattern = finding?.execution_pattern || {};
  const severity = finding?.severity || "HIGH";

  // Analyze what issues we found
  const interpLower = interpretation.toLowerCase();
  const hasIOIssue =
    interpLower.includes("i/o") || interpLower.includes("full scan");
  const hasCPUIssue = interpLower.includes("cpu");
  const hasStaleStats =
    interpLower.includes("stale") || interpLower.includes("statistics");
  const hasHighFreq =
    interpLower.includes("frequency") || execPattern.total_executions > 1000;

  // Build context-specific action items
  let immediateActions = [
    `Capture execution plan: <code class="inline-code">SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('${sqlId}', NULL, 'ALLSTATS LAST'))</code>`,
  ];

  if (hasIOIssue) {
    immediateActions.push("Identify full table scan operations in plan");
    immediateActions.push("Check for missing indexes on filter columns");
  }
  if (hasCPUIssue) {
    immediateActions.push("Analyze join methods and consider hints");
  }
  if (hasStaleStats) {
    immediateActions.push("Check table statistics age and stale_stats flag");
  }

  let shortTermActions = [];
  if (hasIOIssue) {
    shortTermActions.push("Create composite index on high-selectivity columns");
  }
  if (hasCPUIssue) {
    shortTermActions.push("Test SQL rewrites with different hints in DEV");
  }
  if (hasStaleStats) {
    shortTermActions.push("Gather fresh statistics on affected tables");
  }
  shortTermActions.push("Run SQL Tuning Advisor for comprehensive analysis");
  shortTermActions.push("Validate fixes with before/after AWR comparison");

  let mediumTermActions = [
    "Deploy validated indexes/hints to PROD during maintenance window",
    "Monitor SQL performance for 24-48 hours post-deployment",
  ];
  if (severity === "CRITICAL" || severity === "HIGH") {
    mediumTermActions.push(
      "Create SQL Plan Baseline to lock good execution plan",
    );
    mediumTermActions.push(
      "Set up custom alert for this SQL_ID if it regresses",
    );
  }

  let longTermActions = [
    "Include this SQL in weekly AWR review checklist",
    "Schedule automated statistics gathering on affected tables",
  ];
  if (hasHighFreq) {
    longTermActions.push("Work with app team on caching/batching strategy");
  }
  longTermActions.push("Document fix in knowledge base for future reference");

  return `
        <div class="fix-section action-plan">
            <div class="fix-section-header">
                <span class="fix-icon">📋</span>
                <h4>DBA Action Plan</h4>
            </div>
            <div class="fix-section-content">
                <div class="action-phase immediate">
                    <h5>🔥 IMMEDIATE (Next 1 Hour):</h5>
                    <ul>
                        ${immediateActions.map((a) => `<li>${a}</li>`).join("")}
                    </ul>
                </div>
                <div class="action-phase short-term">
                    <h5>⚡ SHORT-TERM (Today/Tomorrow):</h5>
                    <ul>
                        ${shortTermActions.map((a) => `<li>${a}</li>`).join("")}
                    </ul>
                </div>
                <div class="action-phase medium-term">
                    <h5>📅 MEDIUM-TERM (This Week):</h5>
                    <ul>
                        ${mediumTermActions.map((a) => `<li>${a}</li>`).join("")}
                    </ul>
                </div>
                <div class="action-phase long-term">
                    <h5>🔄 LONG-TERM (Ongoing):</h5>
                    <ul>
                        ${longTermActions.map((a) => `<li>${a}</li>`).join("")}
                    </ul>
                </div>
            </div>
        </div>
    `;
}

function formatRecommendationContent(content, _sqlId) {
  if (!content) return "";

  // ⚡ COMPLETELY REWRITTEN: Use INLINE STYLES like working DBA Action Plan section
  // Strategy: First extract SQL blocks, process text, then re-insert SQL blocks

  const sqlBlockPlaceholders = [];
  let processedContent = content;

  // Step 1: Extract SQL blocks and replace with placeholders
  processedContent = processedContent.replace(
    /```sql\n?([\s\S]*?)```/g,
    (_match, code) => {
      const escapedCode = code
        .trim()
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
      const placeholder = `__SQL_BLOCK_${sqlBlockPlaceholders.length}__`;
      // Use EXACT inline styles from working DBA Actions section
      sqlBlockPlaceholders.push(`
            <div style="position: relative; margin: 12px 0;">
                <pre style="background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 12px; padding-right: 80px; overflow-x: auto; font-family: Consolas, Monaco, monospace; font-size: 0.85rem; color: #e2e8f0; white-space: pre-wrap; margin: 0;"><code>${escapedCode}</code></pre>
                <button onclick="navigator.clipboard.writeText(this.previousElementSibling.textContent); this.textContent='✓ Copied!'; setTimeout(() => this.textContent='📋 Copy', 1500);" style="position: absolute; top: 8px; right: 8px; background: #3b82f6; color: white; border: none; padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 0.8rem; font-weight: 500;">📋 Copy</button>
            </div>
        `);
      return placeholder;
    },
  );

  // Step 2: Extract other code blocks
  processedContent = processedContent.replace(
    /```\n?([\s\S]*?)```/g,
    (_match, code) => {
      const escapedCode = code
        .trim()
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
      const placeholder = `__SQL_BLOCK_${sqlBlockPlaceholders.length}__`;
      sqlBlockPlaceholders.push(`
            <div style="position: relative; margin: 12px 0;">
                <pre style="background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 12px; padding-right: 80px; overflow-x: auto; font-family: Consolas, Monaco, monospace; font-size: 0.85rem; color: #e2e8f0; white-space: pre-wrap; margin: 0;"><code>${escapedCode}</code></pre>
                <button onclick="navigator.clipboard.writeText(this.previousElementSibling.textContent); this.textContent='✓ Copied!'; setTimeout(() => this.textContent='📋 Copy', 1500);" style="position: absolute; top: 8px; right: 8px; background: #3b82f6; color: white; border: none; padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 0.8rem; font-weight: 500;">📋 Copy</button>
            </div>
        `);
      return placeholder;
    },
  );

  // Step 3: Process inline formatting (on text only, SQL blocks are safe)
  processedContent = processedContent.replace(
    /`([^`]+)`/g,
    '<code style="background: #1e293b; padding: 2px 6px; border-radius: 4px; font-family: Consolas, monospace; color: #f59e0b;">$1</code>',
  );
  processedContent = processedContent.replace(
    /\*\*(.*?)\*\*/g,
    '<strong style="color: #f1f5f9;">$1</strong>',
  );
  processedContent = processedContent.replace(/\*(.*?)\*/g, "<em>$1</em>");

  // Step 4: Process lines (text only - SQL placeholders are single tokens)
  const lines = processedContent.split("\n");
  let result = [];
  let inList = false;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // Skip placeholders - they'll be re-inserted as-is
    if (trimmed.startsWith("__SQL_BLOCK_")) {
      if (inList) {
        result.push("</ul>");
        inList = false;
      }
      result.push(trimmed);
      continue;
    }

    if (trimmed.startsWith("#### ")) {
      if (inList) {
        result.push("</ul>");
        inList = false;
      }
      result.push(
        `<h5 style="color: #22c55e; font-size: 1rem; margin: 16px 0 8px 0; font-weight: 600;">${trimmed.substring(5)}</h5>`,
      );
    } else if (trimmed.startsWith("### ")) {
      if (inList) {
        result.push("</ul>");
        inList = false;
      }
      result.push(
        `<h4 style="color: #3b82f6; font-size: 1.1rem; margin: 20px 0 12px 0; font-weight: 600; border-bottom: 1px solid #334155; padding-bottom: 8px;">${trimmed.substring(4)}</h4>`,
      );
    } else if (trimmed.startsWith("## ")) {
      if (inList) {
        result.push("</ul>");
        inList = false;
      }
      result.push(
        `<h3 style="color: #f59e0b; font-size: 1.2rem; margin: 24px 0 12px 0; font-weight: 700;">${trimmed.substring(3)}</h3>`,
      );
    } else if (trimmed.startsWith("• ") || trimmed.startsWith("- ")) {
      if (!inList) {
        result.push('<ul style="margin: 8px 0; padding-left: 20px;">');
        inList = true;
      }
      result.push(
        `<li style="color: #cbd5e1; margin: 4px 0;">${trimmed.substring(2)}</li>`,
      );
    } else if (trimmed.match(/^\d+\.\s/)) {
      if (inList) {
        result.push("</ul>");
        inList = false;
      }
      result.push(
        `<p style="color: #94a3b8; margin: 8px 0; padding-left: 8px; border-left: 3px solid #3b82f6;">${trimmed}</p>`,
      );
    } else {
      if (inList) {
        result.push("</ul>");
        inList = false;
      }
      result.push(
        `<p style="color: #cbd5e1; margin: 8px 0; line-height: 1.6;">${trimmed}</p>`,
      );
    }
  }

  if (inList) result.push("</ul>");

  // Step 5: Re-insert SQL blocks
  let finalHtml = result.join("\n");
  sqlBlockPlaceholders.forEach((html, index) => {
    finalHtml = finalHtml.replace(`__SQL_BLOCK_${index}__`, html);
  });

  return finalHtml;
}

function formatActionPlan(plan) {
  if (!plan) return "";

  const lines = plan.split("\n");
  let html = "";
  let currentPhase = "";

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // Phase headers
    if (trimmed.includes("IMMEDIATE") || trimmed.includes("Next 1 Hour")) {
      html += `<div class="action-phase immediate"><h5>🔥 IMMEDIATE (Next 1 Hour)</h5><ul>`;
      currentPhase = "immediate";
    } else if (trimmed.includes("SHORT-TERM") || trimmed.includes("Today")) {
      if (currentPhase) html += "</ul></div>";
      html += `<div class="action-phase short-term"><h5>⚡ SHORT-TERM (Today/Tomorrow)</h5><ul>`;
      currentPhase = "short-term";
    } else if (
      trimmed.includes("MEDIUM-TERM") ||
      trimmed.includes("This Week")
    ) {
      if (currentPhase) html += "</ul></div>";
      html += `<div class="action-phase medium-term"><h5>📅 MEDIUM-TERM (This Week)</h5><ul>`;
      currentPhase = "medium-term";
    } else if (trimmed.includes("LONG-TERM") || trimmed.includes("Ongoing")) {
      if (currentPhase) html += "</ul></div>";
      html += `<div class="action-phase long-term"><h5>🔄 LONG-TERM (Ongoing)</h5><ul>`;
      currentPhase = "long-term";
    } else if (
      currentPhase &&
      (trimmed.startsWith("•") ||
        trimmed.startsWith("-") ||
        trimmed.match(/^\d+\./))
    ) {
      const text = trimmed.replace(/^[•\-]\s*/, "").replace(/^\d+\.\s*/, "");
      html += `<li>${text}</li>`;
    }
  }

  if (currentPhase) html += "</ul></div>";

  return html || formatMarkdown(plan);
}

function formatMarkdown(text) {
  if (!text) return "";

  return text
    .replace(
      /```sql\n([\s\S]*?)```/g,
      '<pre class="sql-code"><code>$1</code></pre>',
    )
    .replace(
      /```([\s\S]*?)```/g,
      '<pre class="code-block"><code>$1</code></pre>',
    )
    .replace(/`([^`]+)`/g, '<code class="inline-code">$1</code>')
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.*?)\*/g, "<em>$1</em>")
    .replace(/\n\n/g, "</p><p>")
    .replace(/\n/g, "<br>");
}
