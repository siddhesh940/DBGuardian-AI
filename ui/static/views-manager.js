// ===============================================
// MULTI-PAGE VIEW MANAGER v10.0
// SQL Workload RCA - Enterprise SaaS Edition
// Premium Production-Ready Tab System
// ===============================================

// ===============================================
// VIEW SWITCHING
// ===============================================
function switchView(viewName) {
  document
    .querySelectorAll(".rca-tab")
    .forEach((t) => t.classList.remove("active"));
  document
    .querySelectorAll(".rca-view")
    .forEach((v) => v.classList.remove("active"));
  const tab = document.querySelector('.rca-tab[data-view="' + viewName + '"]');
  const view = document.getElementById("view-" + viewName);
  if (tab) tab.classList.add("active");
  if (view) {
    view.classList.add("active");
    view.querySelectorAll(".vm-card").forEach(function (card, i) {
      card.style.animation = "none";
      card.offsetHeight;
      card.style.animation = "vmSlideUp 0.45s ease " + i * 0.07 + "s both";
    });
  }
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function scrollToFinding(index) {
  setTimeout(function () {
    var el = document.getElementById("finding-rca-" + index);
    if (el) el.scrollIntoView({ behavior: "smooth", block: "center" });
  }, 400);
}

// ===============================================
// COPY FUNCTIONS
// ===============================================
function copyAllQueries(viewId) {
  var view = document.getElementById(viewId);
  if (!view) return;
  var blocks = view.querySelectorAll("pre code, pre.vm-sql-pre");
  var queries = [];
  blocks.forEach(function (b) {
    var t = b.textContent.trim();
    if (t) queries.push(t);
  });
  if (!queries.length) {
    showToast("No queries found", "warning");
    return;
  }
  var text = queries.join(
    "\n\n-- ========================================\n\n",
  );
  navigator.clipboard
    .writeText(text)
    .then(function () {
      showToast(queries.length + " queries copied!", "success");
    })
    .catch(function () {
      showToast("Copy failed", "error");
    });
}

function copyQueryBtn(btn) {
  var block = btn.closest(".vm-query-block");
  var pre = block ? block.querySelector("pre") : null;
  if (!pre) return;
  var code = pre.querySelector("code") || pre;
  navigator.clipboard
    .writeText(code.textContent)
    .then(function () {
      var orig = btn.innerHTML;
      btn.innerHTML =
        '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20,6 9,17 4,12"/></svg> Copied!';
      btn.classList.add("copied");
      setTimeout(function () {
        btn.innerHTML = orig;
        btn.classList.remove("copied");
      }, 2000);
    })
    .catch(function () {
      btn.textContent = "Failed";
      setTimeout(function () {
        btn.textContent = "Copy";
      }, 2000);
    });
}

function copySqlId(id) {
  navigator.clipboard.writeText(id).then(function () {
    showToast("SQL ID " + id + " copied!", "success");
  });
}

// ===============================================
// TOAST
// ===============================================
function showToast(message, type) {
  type = type || "info";
  var toast = document.createElement("div");
  toast.className = "vm-toast vm-toast-" + type;
  var icons = { success: "✓", warning: "⚠", error: "✕", info: "ℹ" };
  toast.innerHTML =
    '<span class="vm-toast-icon">' +
    (icons[type] || "ℹ") +
    "</span> " +
    message;
  document.body.appendChild(toast);
  requestAnimationFrame(function () {
    toast.classList.add("show");
  });
  setTimeout(function () {
    toast.classList.remove("show");
    setTimeout(function () {
      toast.remove();
    }, 300);
  }, 3000);
}

// ===============================================
// HTML HELPERS
// ===============================================
function esc(text) {
  if (!text) return "";
  var d = document.createElement("div");
  d.textContent = text;
  return d.innerHTML;
}

function fmtMd(text) {
  if (!text) return "";
  if (typeof formatMarkdown === "function") return formatMarkdown(text);
  return text.replace(/\n/g, "<br>");
}

function severityClass(sev) {
  var s = (sev || "").toLowerCase();
  if (s === "critical") return "vm-sev-critical";
  if (s === "high") return "vm-sev-high";
  if (s === "medium") return "vm-sev-medium";
  return "vm-sev-low";
}

function severityColor(sev) {
  var s = (sev || "").toLowerCase();
  if (s === "critical") return "#ef4444";
  if (s === "high") return "#f97316";
  if (s === "medium") return "#eab308";
  return "#22c55e";
}

function catIcon(cat) {
  var m = {
    IO_DOMINANT: "💾",
    IO_REDUCTION: "💾",
    MISSING_INDEX: "🎯",
    SQL_ACCESS_ADVISOR: "🎯",
    HIGH_CPU: "🔥",
    CPU_REDUCTION: "🔥",
    PX_INEFFECTIVE: "⚡",
    PARALLEL_EXECUTION: "⚡",
    PLAN_STABILITY: "📌",
  };
  return m[cat] || "🔧";
}

function catColor(cat) {
  var m = {
    IO_DOMINANT: "#ef4444",
    IO_REDUCTION: "#ef4444",
    MISSING_INDEX: "#f59e0b",
    SQL_ACCESS_ADVISOR: "#f59e0b",
    HIGH_CPU: "#f97316",
    CPU_REDUCTION: "#f97316",
    PX_INEFFECTIVE: "#3b82f6",
    PARALLEL_EXECUTION: "#3b82f6",
    PLAN_STABILITY: "#8b5cf6",
  };
  return m[cat] || "#64748b";
}

// ===============================================
// SQL QUERY BLOCK RENDERER
// ===============================================
function renderQueryBlock(sql, stepTitle) {
  var h = '<div class="vm-query-block">';
  if (stepTitle) {
    h += '<div class="vm-query-step">' + esc(stepTitle) + "</div>";
  }
  h +=
    '<div class="vm-query-code-wrap">' +
    '<pre class="vm-sql-pre"><code>' +
    esc(sql) +
    "</code></pre>" +
    '<button class="vm-copy-btn" onclick="copyQueryBtn(this)" title="Copy to clipboard">' +
    '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg> Copy' +
    "</button></div></div>";
  return h;
}

// ===============================================
// METRIC CARD
// ===============================================
function metricCard(label, value, color, icon) {
  return (
    '<div class="vm-metric">' +
    '<div class="vm-metric-icon" style="color:' +
    (color || "#818cf8") +
    '">' +
    (icon || "📊") +
    "</div>" +
    '<div class="vm-metric-body">' +
    '<div class="vm-metric-val" style="color:' +
    (color || "#f1f5f9") +
    '">' +
    value +
    "</div>" +
    '<div class="vm-metric-lbl">' +
    label +
    "</div>" +
    "</div></div>"
  );
}

// ===============================================
// FINDING HEADER
// ===============================================
function findingHeader(finding, index, idPrefix) {
  var sqlId = finding.sql_id || "SQL_" + (index + 1);
  var sev = finding.severity || "HIGH";
  return (
    '<div class="vm-finding-header" id="finding-' +
    idPrefix +
    "-" +
    index +
    '">' +
    '<div class="vm-fh-left">' +
    '<span class="vm-fh-num">#' +
    (index + 1) +
    "</span>" +
    '<span class="vm-fh-sqlid" onclick="copySqlId(\'' +
    sqlId +
    '\')" title="Click to copy">' +
    '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg> ' +
    sqlId +
    "</span>" +
    "</div>" +
    '<span class="vm-sev-badge ' +
    severityClass(sev) +
    '">' +
    sev +
    "</span>" +
    "</div>"
  );
}

// ===============================================
// ROOT CAUSE ANALYSIS RENDERER
// ===============================================
function analyzeRootCause(finding) {
  var tp = finding.technical_parameters || {};
  var cpuPct = parseFloat(tp.cpu_percentage || tp.cpu_pct) || 0;
  var ioPct = parseFloat(tp.io_percentage || tp.io_pct) || 0;
  var dbTimePct =
    parseFloat(tp.contribution_to_db_time_pct || tp.db_time_pct) || 0;
  var executions = tp.executions || 0;
  var avgExecTime = tp.avg_time || tp.avg_elapsed_per_exec_s || 0;
  var elapsed = tp.elapsed || tp.total_elapsed_time_s || 0;

  var cause = "Mixed Workload",
    causeIcon = "⚡",
    causeColor = "#8b5cf6";
  var explanation = "";

  if (cpuPct >= 80 && ioPct <= 10) {
    cause = "CPU-Bound";
    causeIcon = "🔥";
    causeColor = "#ef4444";
    explanation =
      "High CPU utilization (" +
      cpuPct +
      "%) with minimal IO. Query is performing CPU-intensive operations like complex calculations, hash joins, or full table scans without disk reads.";
  } else if (ioPct >= 40) {
    cause = "IO-Bound";
    causeIcon = "💾";
    causeColor = "#f59e0b";
    explanation =
      "High IO wait (" +
      ioPct +
      "%) indicates excessive physical reads. Missing indexes are likely forcing full table scans, causing disk I/O bottleneck.";
  } else if (ioPct >= 20 && cpuPct < 50) {
    cause = "IO-Heavy";
    causeIcon = "📑";
    causeColor = "#f59e0b";
    explanation =
      "Moderate IO wait (" +
      ioPct +
      "%) with CPU at " +
      cpuPct +
      "%. Physical reads from suboptimal access paths contributing to the load.";
  } else if (cpuPct >= 50) {
    cause = "CPU-Intensive";
    causeIcon = "🔴";
    causeColor = "#ef4444";
    explanation =
      "CPU-intensive operations at " +
      cpuPct +
      "% utilization. Complex joins, sorting, or analytical functions consuming compute resources.";
  } else {
    explanation =
      "Mixed resource consumption — CPU at " +
      cpuPct +
      "%, IO at " +
      ioPct +
      "% across " +
      executions +
      " executions.";
  }

  var h = "";

  // Root Cause Banner
  h +=
    '<div class="vm-rca-cause" style="--rca-color:' +
    causeColor +
    '">' +
    '<div class="vm-rca-cause-icon">' +
    causeIcon +
    "</div>" +
    '<div class="vm-rca-cause-body">' +
    '<div class="vm-rca-cause-label">Primary Root Cause</div>' +
    '<div class="vm-rca-cause-title">' +
    cause +
    "</div>" +
    "</div></div>";

  // Explanation
  h +=
    '<div class="vm-rca-explain">' +
    '<div class="vm-rca-explain-title"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg> Analysis</div>' +
    "<p>" +
    explanation +
    "</p></div>";

  // Metrics Grid
  h += '<div class="vm-rca-metrics">';
  if (dbTimePct)
    h += metricCard("DB Time Impact", dbTimePct + "%", "#ef4444", "📊");
  if (cpuPct) h += metricCard("CPU Usage", cpuPct + "%", "#f97316", "🔥");
  if (ioPct) h += metricCard("IO Wait", ioPct + "%", "#f59e0b", "💾");
  h += metricCard("Executions", executions, "#3b82f6", "🔄");
  if (avgExecTime)
    h += metricCard("Avg Exec Time", avgExecTime + "s", "#8b5cf6", "⏱️");
  if (elapsed) h += metricCard("Total Elapsed", elapsed + "s", "#6366f1", "⏳");
  h += "</div>";

  // Problem Summary
  if (finding.problem_summary) {
    h +=
      '<div class="vm-rca-detail">' +
      '<div class="vm-rca-detail-title">📋 Problem Summary</div>' +
      '<div class="vm-rca-detail-body">' +
      fmtMd(finding.problem_summary) +
      "</div></div>";
  }

  // DBA Interpretation
  if (finding.dba_interpretation) {
    h +=
      '<div class="vm-rca-detail">' +
      '<div class="vm-rca-detail-title">🧠 DBA Interpretation</div>' +
      '<div class="vm-rca-detail-body">' +
      fmtMd(finding.dba_interpretation) +
      "</div></div>";
  }

  // Execution Pattern
  if (finding.execution_pattern) {
    var ep = finding.execution_pattern;
    h +=
      '<div class="vm-rca-pattern">' +
      '<div class="vm-rca-pattern-header">' +
      '<span class="vm-rca-pattern-type">' +
      (ep.pattern_type || "UNKNOWN").replace(/_/g, " ") +
      "</span>" +
      "</div>";
    if (ep.description)
      h += '<p class="vm-rca-pattern-desc">' + fmtMd(ep.description) + "</p>";
    if (ep.dba_assessment) {
      h +=
        '<div class="vm-rca-assessment">' +
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#fbbf24" stroke-width="2"><path d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>' +
        "<div><strong>DBA Assessment</strong><p>" +
        ep.dba_assessment +
        "</p></div></div>";
    }
    h += "</div>";
  }

  return h;
}

// ===============================================
// DBA ACTION CARD RENDERER
// ===============================================
function renderActionCard(action) {
  var color = catColor(action.category);
  var icon = catIcon(action.category);

  var h = '<div class="vm-action-card" style="--ac-color:' + color + '">';

  h +=
    '<div class="vm-ac-header">' +
    '<div class="vm-ac-header-left">' +
    '<span class="vm-ac-icon">' +
    icon +
    "</span>" +
    "<h4>" +
    (action.title || "Action") +
    "</h4>" +
    "</div>" +
    '<span class="vm-ac-priority">Priority ' +
    (action.priority || "-") +
    "</span>" +
    "</div>";

  if (action.why_this_helps) {
    h +=
      '<div class="vm-ac-why">' +
      '<div class="vm-ac-why-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg> Why This Helps</div>' +
      "<p>" +
      action.why_this_helps +
      "</p></div>";
  }

  if (action.sql_queries && action.sql_queries.length) {
    h +=
      '<div class="vm-ac-queries">' +
      '<div class="vm-ac-queries-label">' +
      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg>' +
      " DBA-Executable Queries (" +
      action.sql_queries.length +
      ")" +
      "</div>";
    action.sql_queries.forEach(function (q, i) {
      h += renderQueryBlock(q, "Step " + (i + 1));
    });
    h += "</div>";
  }

  if (action.dba_action_text) {
    h +=
      '<div class="vm-ac-rec">' +
      '<span class="vm-ac-rec-icon">💡</span>' +
      "<div><strong>Recommendation</strong><p>" +
      action.dba_action_text +
      "</p></div></div>";
  }

  h += "</div>";
  return h;
}

// ===============================================
// FIX RECOMMENDATIONS RENDERER
// ===============================================
function renderFixRecsForFinding(finding) {
  var sqlId = finding.sql_id || "SQL_UNKNOWN";
  var fixRecs = finding.fix_recommendations;

  if (fixRecs && fixRecs.fix_sections && fixRecs.fix_sections.length > 0) {
    return renderStructuredFixRecs(fixRecs, sqlId);
  }

  return generateSmartFixRecs(finding, sqlId);
}

function renderStructuredFixRecs(fixRecs, _sqlId) {
  var sections = fixRecs.fix_sections || [];
  var issues = fixRecs.detected_issues || [];

  var h = "";

  if (issues.length) {
    h +=
      '<div class="vm-fix-issues">' +
      '<div class="vm-fix-issues-label">Detected Issues</div>' +
      '<div class="vm-fix-issues-chips">';
    issues.forEach(function (issue) {
      h +=
        '<span class="vm-issue-chip">' +
        catIcon(issue) +
        " " +
        issue.replace(/_/g, " ") +
        "</span>";
    });
    h += "</div></div>";
  }

  sections.forEach(function (section) {
    var color = catColor(section.category);
    var icon = section.section_icon || "🔧";

    h += '<div class="vm-action-card" style="--ac-color:' + color + '">';
    h +=
      '<div class="vm-ac-header">' +
      '<div class="vm-ac-header-left"><span class="vm-ac-icon">' +
      icon +
      "</span><h4>" +
      section.section_title +
      "</h4></div>" +
      '<span class="vm-ac-priority">' +
      section.priority_tag +
      "</span></div>";

    if (section.why_shown) {
      h +=
        '<div class="vm-ac-why"><div class="vm-ac-why-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg> Why This Fix</div><p>' +
        section.why_shown +
        "</p></div>";
    }

    if (section.steps && section.steps.length) {
      h +=
        '<div class="vm-ac-queries"><div class="vm-ac-queries-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg> Fix Steps (' +
        section.steps.length +
        ")</div>";
      section.steps.forEach(function (step) {
        h += renderQueryBlock(step.sql_code, step.title);
      });
      h += "</div>";
    }

    if (section.expected_improvement) {
      h +=
        '<div class="vm-ac-rec"><span class="vm-ac-rec-icon">💡</span><div><strong>Expected Improvement</strong><p>' +
        section.expected_improvement +
        "</p></div></div>";
    }
    h += "</div>";
  });

  return h;
}

function generateSmartFixRecs(finding, sqlId) {
  var tp = finding.technical_parameters || {};
  var cpuPct = parseFloat(tp.cpu_percentage || tp.cpu_pct) || 0;
  var ioPct = parseFloat(tp.io_percentage || tp.io_pct) || 0;
  var executions = tp.executions || 0;
  var avgExec = tp.avg_time || tp.avg_elapsed_per_exec_s || 0;

  var h = "";

  // Detected Issues Banner
  var detectedIssues = [];
  if (ioPct >= 40) detectedIssues.push("IO_DOMINANT");
  if (cpuPct >= 50) detectedIssues.push("HIGH_CPU");
  if (avgExec >= 5) detectedIssues.push("SLOW_EXECUTION");
  if (executions >= 100) detectedIssues.push("HIGH_FREQUENCY");
  if (!detectedIssues.length) detectedIssues.push("PERFORMANCE_ANOMALY");

  h +=
    '<div class="vm-fix-issues">' +
    '<div class="vm-fix-issues-label">Auto-Detected Issues</div>' +
    '<div class="vm-fix-issues-chips">';
  detectedIssues.forEach(function (issue) {
    h +=
      '<span class="vm-issue-chip">' +
      catIcon(issue) +
      " " +
      issue.replace(/_/g, " ") +
      "</span>";
  });
  h += "</div></div>";

  // 1. Execution Plan Analysis
  h += '<div class="vm-action-card" style="--ac-color:#3b82f6">';
  h +=
    '<div class="vm-ac-header"><div class="vm-ac-header-left"><span class="vm-ac-icon">📊</span><h4>Execution Plan Analysis</h4></div><span class="vm-ac-priority">Priority 1</span></div>';
  h +=
    '<div class="vm-ac-why"><div class="vm-ac-why-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg> Why This Fix</div><p>Understanding the execution plan is the first step to identify full table scans, inefficient joins, or missing indexes causing performance issues.</p></div>';
  h +=
    '<div class="vm-ac-queries"><div class="vm-ac-queries-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg> Fix Steps (2)</div>';
  h += renderQueryBlock(
    "-- Step 1: Get current execution plan with runtime statistics\nSELECT * FROM TABLE(\n  DBMS_XPLAN.DISPLAY_CURSOR(\n    sql_id => '" +
      sqlId +
      "',\n    cursor_child_no => NULL,\n    format => 'ALLSTATS LAST +ALIAS +OUTLINE +PREDICATE'\n  )\n);",
    "Capture Current Plan",
  );
  h += renderQueryBlock(
    "-- Step 2: Check AWR historical plans for plan instability\nSELECT * FROM TABLE(\n  DBMS_XPLAN.DISPLAY_AWR(\n    sql_id => '" +
      sqlId +
      "',\n    format => 'ALLSTATS +ALIAS +OUTLINE'\n  )\n);",
    "Review Historical Plans",
  );
  h += "</div>";
  h +=
    '<div class="vm-ac-rec"><span class="vm-ac-rec-icon">💡</span><div><strong>Expected Improvement</strong><p>Identifies specific operations causing slowness — full table scans, nested loops on large tables, or sort operations that can be optimized.</p></div></div></div>';

  // 2. SQL Tuning Advisor
  h += '<div class="vm-action-card" style="--ac-color:#f59e0b">';
  h +=
    '<div class="vm-ac-header"><div class="vm-ac-header-left"><span class="vm-ac-icon">🎯</span><h4>SQL Tuning Advisor</h4></div><span class="vm-ac-priority">Priority 2</span></div>';
  h +=
    '<div class="vm-ac-why"><div class="vm-ac-why-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg> Why This Fix</div><p>Oracle\'s built-in advisor analyzes the SQL and recommends indexes, SQL profiles, and restructured access paths. Average improvement: 40-80%.</p></div>';
  h +=
    '<div class="vm-ac-queries"><div class="vm-ac-queries-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg> Fix Steps (3)</div>';
  h += renderQueryBlock(
    "-- Create tuning task\nDECLARE\n  l_task VARCHAR2(64);\nBEGIN\n  l_task := DBMS_SQLTUNE.CREATE_TUNING_TASK(\n    sql_id      => '" +
      sqlId +
      "',\n    scope       => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,\n    time_limit  => 300,\n    task_name   => 'TUNE_" +
      sqlId +
      "'\n  );\n  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name => l_task);\nEND;\n/",
    "Create & Execute Tuning Task",
  );
  h += renderQueryBlock(
    "-- View tuning recommendations\nSELECT DBMS_SQLTUNE.REPORT_TUNING_TASK('TUNE_" +
      sqlId +
      "') AS report FROM DUAL;",
    "Get Recommendations Report",
  );
  h += renderQueryBlock(
    "-- Accept SQL Profile if recommended\nBEGIN\n  DBMS_SQLTUNE.ACCEPT_SQL_PROFILE(\n    task_name   => 'TUNE_" +
      sqlId +
      "',\n    name        => 'PROFILE_" +
      sqlId +
      "',\n    force_match => TRUE\n  );\nEND;\n/",
    "Accept SQL Profile (if recommended)",
  );
  h += "</div>";
  h +=
    '<div class="vm-ac-rec"><span class="vm-ac-rec-icon">💡</span><div><strong>Expected Improvement</strong><p>SQL Profiles can improve performance by 40-80% by guiding the optimizer to better execution plans without modifying SQL code.</p></div></div></div>';

  // 3. IO-specific fix
  if (ioPct >= 30) {
    h += '<div class="vm-action-card" style="--ac-color:#ef4444">';
    h +=
      '<div class="vm-ac-header"><div class="vm-ac-header-left"><span class="vm-ac-icon">💾</span><h4>IO Reduction — Index Analysis</h4></div><span class="vm-ac-priority">Priority ' +
      (ioPct >= 60 ? "1" : "2") +
      "</span></div>";
    h +=
      '<div class="vm-ac-why"><div class="vm-ac-why-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg> Why This Fix</div><p>IO wait at ' +
      ioPct +
      "% indicates excessive physical reads. Adding proper indexes can reduce IO by 60-90%.</p></div>";
    h +=
      '<div class="vm-ac-queries"><div class="vm-ac-queries-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg> Fix Steps (2)</div>';
    h += renderQueryBlock(
      "-- Identify tables accessed by this SQL\nSELECT DISTINCT\n  object_owner, object_name, object_type,\n  operation, options\nFROM v$sql_plan\nWHERE sql_id = '" +
        sqlId +
        "'\n  AND object_owner IS NOT NULL\nORDER BY object_owner, object_name;",
      "Identify Accessed Objects",
    );
    h += renderQueryBlock(
      "-- Check existing indexes on those tables\nSELECT\n  i.table_owner, i.table_name, i.index_name,\n  ic.column_name, ic.column_position,\n  i.uniqueness, i.status\nFROM dba_indexes i\nJOIN dba_ind_columns ic\n  ON i.index_name = ic.index_name AND i.owner = ic.index_owner\nWHERE i.table_name IN (\n  SELECT object_name FROM v$sql_plan\n  WHERE sql_id = '" +
        sqlId +
        "' AND object_type LIKE 'TABLE%'\n)\nORDER BY i.table_name, i.index_name, ic.column_position;",
      "Review Existing Indexes",
    );
    h += "</div>";
    h +=
      '<div class="vm-ac-rec"><span class="vm-ac-rec-icon">💡</span><div><strong>Expected Improvement</strong><p>Creating proper indexes can reduce IO by 60-90%, bringing ' +
      ioPct +
      "% IO wait down significantly.</p></div></div></div>";
  }

  // 4. CPU-specific fix
  if (cpuPct >= 40) {
    h += '<div class="vm-action-card" style="--ac-color:#f97316">';
    h +=
      '<div class="vm-ac-header"><div class="vm-ac-header-left"><span class="vm-ac-icon">🔥</span><h4>CPU Reduction Strategy</h4></div><span class="vm-ac-priority">Priority 2</span></div>';
    h +=
      '<div class="vm-ac-why"><div class="vm-ac-why-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg> Why This Fix</div><p>CPU at ' +
      cpuPct +
      "% — check for unnecessary sorting, complex expressions, or inefficient PL/SQL loops.</p></div>";
    h +=
      '<div class="vm-ac-queries"><div class="vm-ac-queries-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg> Fix Steps (1)</div>';
    h += renderQueryBlock(
      "-- Check for high CPU operations in execution plan\nSELECT\n  id, operation, options,\n  object_name, cardinality, cost,\n  cpu_cost, io_cost, temp_space\nFROM v$sql_plan\nWHERE sql_id = '" +
        sqlId +
        "'\nORDER BY id;",
      "Identify CPU-Intensive Operations",
    );
    h += "</div>";
    h +=
      '<div class="vm-ac-rec"><span class="vm-ac-rec-icon">💡</span><div><strong>Expected Improvement</strong><p>Optimizing CPU-intensive operations can reduce CPU usage by 30-60%.</p></div></div></div>';
  }

  return h;
}

// ===============================================
// ACTION PLAN RENDERER
// ===============================================
function renderActionPlan(finding) {
  var recs = finding.recommendations || {};
  var sqlId = finding.sql_id || "SQL_UNKNOWN";
  var h = "";

  if (!recs || typeof recs !== "object" || Object.keys(recs).length === 0) {
    return renderBasicActionPlan(finding, sqlId);
  }

  // Priority Banner
  if (recs.priority_description) {
    h +=
      '<div class="vm-plan-priority">' +
      '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>' +
      "<span>" +
      fmtMd(recs.priority_description) +
      "</span></div>";
  }

  // What DBA Should Do Next
  if (recs.what_dba_should_do_next) {
    h +=
      '<div class="vm-plan-section vm-plan-diagnostic">' +
      '<div class="vm-plan-section-header">' +
      '<span class="vm-plan-section-icon">🛠️</span>' +
      "<h4>What DBA Should Do Next</h4></div>" +
      '<div class="vm-plan-section-body">' +
      fmtMd(recs.what_dba_should_do_next) +
      "</div></div>";

    // Extract SQL blocks
    var sqlMatches = (recs.what_dba_should_do_next || "").match(
      /```sql([\s\S]*?)```/g,
    );
    if (sqlMatches && sqlMatches.length) {
      h +=
        '<div class="vm-plan-queries-extracted">' +
        '<div class="vm-ac-queries-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg> Extracted Diagnostic Queries (' +
        sqlMatches.length +
        ")</div>";
      sqlMatches.forEach(function (match, i) {
        var sql = match
          .replace(/```sql\n?/, "")
          .replace(/```$/, "")
          .trim();
        h += renderQueryBlock(sql, "Diagnostic Query " + (i + 1));
      });
      h += "</div>";
    }
  }

  // DBA Action Plan
  if (recs.dba_action_plan) {
    h +=
      '<div class="vm-plan-section vm-plan-actions">' +
      '<div class="vm-plan-section-header">' +
      '<span class="vm-plan-section-icon">📋</span>' +
      "<h4>DBA Action Plan</h4></div>" +
      '<div class="vm-plan-section-body">' +
      fmtMd(recs.dba_action_plan) +
      "</div></div>";
  }

  // Expected Improvement
  if (recs.expected_improvement) {
    h +=
      '<div class="vm-plan-improvement">' +
      '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#22c55e" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>' +
      "<div><strong>Expected Improvement</strong><p>" +
      recs.expected_improvement +
      "</p></div></div>";
  }

  // Why Shown
  if (recs.why_shown && recs.why_shown.length) {
    h +=
      '<div class="vm-plan-reasons">' +
      '<div class="vm-plan-reasons-label">📊 Why These Actions Were Selected</div><ul>';
    recs.why_shown.forEach(function (reason) {
      h += "<li>" + reason + "</li>";
    });
    h += "</ul></div>";
  }

  // Why Hidden
  if (recs.why_hidden && recs.why_hidden.length) {
    h +=
      '<div class="vm-plan-excluded">' +
      '<div class="vm-plan-reasons-label">⚠️ Actions Not Recommended</div><ul>';
    recs.why_hidden.forEach(function (reason) {
      h += "<li>" + reason + "</li>";
    });
    h += "</ul></div>";
  }

  return h;
}

function renderBasicActionPlan(finding, sqlId) {
  var sev = finding.severity || "HIGH";

  var h =
    '<div class="vm-plan-priority">' +
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>' +
    "<span>" +
    sev +
    " Priority — Investigate and optimize within 24 hours</span></div>";

  h +=
    '<div class="vm-plan-section vm-plan-actions">' +
    '<div class="vm-plan-section-header"><span class="vm-plan-section-icon">📋</span><h4>Recommended Action Plan for ' +
    sqlId +
    "</h4></div>" +
    '<div class="vm-plan-section-body">' +
    '<div class="vm-plan-timeline">' +
    '<div class="vm-plan-timeblock"><div class="vm-plan-timeblock-header vm-urgent">🔥 Immediate (Next 1 Hour)</div>' +
    "<ul><li>Capture current execution plan using DBMS_XPLAN</li><li>Check for blocking sessions and lock contention</li><li>Review recent AWR/ASH data for this SQL ID</li></ul></div>" +
    '<div class="vm-plan-timeblock"><div class="vm-plan-timeblock-header vm-short">⚡ Short-Term (Today)</div>' +
    "<ul><li>Run SQL Tuning Advisor for optimization recommendations</li><li>Review index usage and consider adding missing indexes</li><li>Analyze wait event breakdown for this SQL</li></ul></div>" +
    '<div class="vm-plan-timeblock"><div class="vm-plan-timeblock-header vm-medium">📅 Medium-Term (This Week)</div>' +
    "<ul><li>Implement recommended indexes after testing</li><li>Consider SQL Profile or SQL Plan Baseline</li><li>Review application-level optimization opportunities</li></ul></div>" +
    "</div></div></div>";

  h +=
    '<div class="vm-plan-queries-extracted">' +
    '<div class="vm-ac-queries-label"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16,18 22,12 16,6"/><polyline points="8,6 2,12 8,18"/></svg> Diagnostic Queries (2)</div>';
  h += renderQueryBlock(
    "-- Capture execution plan\nSELECT * FROM TABLE(\n  DBMS_XPLAN.DISPLAY_CURSOR('" +
      sqlId +
      "', NULL, 'ALLSTATS LAST')\n);\n\n-- Check AWR history\nSELECT * FROM TABLE(\n  DBMS_XPLAN.DISPLAY_AWR('" +
      sqlId +
      "'));",
    "Plan Analysis",
  );
  h += renderQueryBlock(
    "-- Run SQL Tuning Advisor\nDECLARE\n  l_task VARCHAR2(30);\nBEGIN\n  l_task := DBMS_SQLTUNE.CREATE_TUNING_TASK(\n    sql_id => '" +
      sqlId +
      "',\n    scope  => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,\n    time_limit => 300\n  );\n  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name => l_task);\nEND;\n/\n\nSELECT DBMS_SQLTUNE.REPORT_TUNING_TASK(l_task) FROM DUAL;",
    "SQL Tuning Advisor",
  );
  h += "</div>";

  return h;
}

// ===============================================
// MAIN: displayDBAExpertAnalysis
// ===============================================
function showDashboardView() {
  // Restore dashboard UI (reverse of what displayDBAExpertAnalysis hides)
  var tabNav = document.getElementById("resultsTabNav");
  if (tabNav) tabNav.style.display = "none";
  var uploadSection = document.getElementById("uploadSection");
  if (uploadSection) uploadSection.style.display = "";
  var banner = document.getElementById("existingDataBanner");
  if (banner) banner.style.display = "flex";
  var grid = document.querySelector(".dash-grid");
  if (grid) grid.style.display = "";
  var hero = document.querySelector(".dash-hero");
  if (hero) hero.style.display = "";

  // Hide all result views
  [
    "view-overview",
    "view-rca",
    "view-dba-actions",
    "view-fix-recs",
    "view-action-plan",
  ].forEach(function (id) {
    var el = document.getElementById(id);
    if (el) el.style.display = "none";
  });
}

// Handle browser back button
window.addEventListener("popstate", function (event) {
  if (event.state && event.state.view === "dashboard") {
    showDashboardView();
  } else if (!event.state || event.state.view !== "results") {
    // Default: show dashboard when going back
    showDashboardView();
  }
});

// Push initial dashboard state on page load
(function () {
  if (!history.state) {
    history.replaceState({ view: "dashboard" }, "", window.location.href);
  }
})();

function displayDBAExpertAnalysis(dbaAnalysis) {
  var summary = dbaAnalysis.workload_summary || {};
  var findings = dbaAnalysis.problematic_sql_findings || [];

  // Push state so browser back returns to dashboard
  history.pushState({ view: "results" }, "", window.location.href);

  // Show tab nav
  var tabNav = document.getElementById("resultsTabNav");
  if (tabNav) tabNav.style.display = "flex";

  // Hide upload/sidebar to remove empty white space
  var uploadSection = document.getElementById("uploadSection");
  if (uploadSection) uploadSection.style.display = "none";
  var banner = document.getElementById("existingDataBanner");
  if (banner) banner.style.display = "none";
  var grid = document.querySelector(".dash-grid");
  if (grid) grid.style.display = "none";
  var hero = document.querySelector(".dash-hero");
  if (hero) hero.style.display = "none";

  // Update badges
  var pb = document.getElementById("patternBadge");
  if (pb) pb.textContent = summary.pattern || "Unknown";
  var pb2 = document.getElementById("patternBadge2");
  if (pb2) pb2.textContent = (summary.pattern || "Unknown").replace(/_/g, " ");

  // ============ OVERVIEW TAB ============
  var overviewHtml = "";

  overviewHtml +=
    '<div class="vm-overview-header">' +
    '<h3 class="vm-section-title"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg> Workload Summary</h3></div>';

  overviewHtml += '<div class="vm-overview-metrics">';
  overviewHtml += metricCard(
    "Pattern",
    (summary.pattern || "N/A").replace(/_/g, " "),
    "#818cf8",
    "🧠",
  );
  overviewHtml += metricCard(
    "Total Elapsed",
    (summary.total_elapsed_s || 0) + "s",
    "#ef4444",
    "⏱️",
  );
  overviewHtml += metricCard(
    "Total CPU",
    (summary.total_cpu_s || 0) + "s",
    "#f97316",
    "🔥",
  );
  overviewHtml += metricCard(
    "SQL Analyzed",
    summary.sql_analyzed || 0,
    "#3b82f6",
    "📊",
  );
  overviewHtml += metricCard(
    "Problematic Found",
    summary.problematic_found || 0,
    "#ef4444",
    "⚠️",
  );
  overviewHtml += metricCard(
    "Total Executions",
    summary.total_executions || 0,
    "#22c55e",
    "🔄",
  );
  if (summary.dominant_wait_event) {
    overviewHtml += metricCard(
      "Dominant Wait",
      summary.dominant_wait_event,
      "#f59e0b",
      "⏳",
    );
  }
  overviewHtml += "</div>";

  // Findings Preview
  if (findings.length > 0) {
    overviewHtml +=
      '<h3 class="vm-section-title" style="margin-top:28px"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#ef4444" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg> Problematic SQL Summary (' +
      findings.length +
      " findings)</h3>";
    overviewHtml += '<div class="vm-findings-grid">';

    findings.forEach(function (finding, index) {
      var sqlId = finding.sql_id || "SQL_" + (index + 1);
      var sev = finding.severity || "MEDIUM";
      var tp = finding.technical_parameters || {};
      var cpuPct = parseFloat(tp.cpu_percentage || tp.cpu_pct) || 0;
      var ioPct = parseFloat(tp.io_percentage || tp.io_pct) || 0;
      var dbTimePct =
        parseFloat(tp.contribution_to_db_time_pct || tp.db_time_pct) || 0;

      var cause = "Mixed",
        causeIcon = "⚡",
        causeColor = "#8b5cf6";
      if (cpuPct >= 80) {
        cause = "CPU-Bound";
        causeIcon = "🔥";
        causeColor = "#ef4444";
      } else if (ioPct >= 40) {
        cause = "IO-Bound";
        causeIcon = "💾";
        causeColor = "#f59e0b";
      } else if (cpuPct >= 50) {
        cause = "CPU-Intensive";
        causeIcon = "🔴";
        causeColor = "#ef4444";
      }

      overviewHtml +=
        '<div class="vm-fp-card" onclick="switchView(\'rca\');scrollToFinding(' +
        index +
        ')">' +
        '<div class="vm-fp-top">' +
        '<span class="vm-fp-num">Finding #' +
        (index + 1) +
        "</span>" +
        '<span class="vm-sev-badge ' +
        severityClass(sev) +
        '">' +
        sev +
        "</span></div>" +
        '<div class="vm-fp-sqlid">' +
        sqlId +
        "</div>" +
        '<div class="vm-fp-cause" style="color:' +
        causeColor +
        '">' +
        causeIcon +
        " " +
        cause +
        "</div>" +
        '<div class="vm-fp-metrics">' +
        (dbTimePct
          ? '<div class="vm-fp-met"><span class="vm-fp-met-val">' +
            dbTimePct +
            '%</span><span class="vm-fp-met-lbl">DB Time</span></div>'
          : "") +
        (cpuPct
          ? '<div class="vm-fp-met"><span class="vm-fp-met-val">' +
            cpuPct +
            '%</span><span class="vm-fp-met-lbl">CPU</span></div>'
          : "") +
        (ioPct
          ? '<div class="vm-fp-met"><span class="vm-fp-met-val">' +
            ioPct +
            '%</span><span class="vm-fp-met-lbl">IO</span></div>'
          : "") +
        "</div>" +
        '<div class="vm-fp-action">View Root Cause Analysis <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9,18 15,12 9,6"/></svg></div>' +
        "</div>";
    });
    overviewHtml += "</div>";
  }

  // DBA Conclusion
  if (dbaAnalysis.dba_conclusion) {
    overviewHtml +=
      '<div class="vm-conclusion">' +
      '<div class="vm-conclusion-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22,4 12,14.01 9,11.01"/></svg> Final DBA Conclusion</div>' +
      '<div class="vm-conclusion-body">' +
      fmtMd(dbaAnalysis.dba_conclusion) +
      "</div></div>";
  }

  // ============ BUILD ALL TABS ============
  var rcaHtml = "";
  var dbaActionsHtml = "";
  var fixRecsHtml = "";
  var actionPlanHtml = "";

  if (findings.length > 0) {
    findings.forEach(function (finding, index) {
      var lra = finding.load_reduction_actions;
      var hasLRA = lra && lra.actions && lra.actions.length > 0;

      // RCA
      rcaHtml +=
        '<div class="vm-card">' +
        findingHeader(finding, index, "rca") +
        '<div class="vm-card-body">' +
        analyzeRootCause(finding) +
        "</div></div>";

      // DBA Actions
      if (hasLRA) {
        dbaActionsHtml +=
          '<div class="vm-card">' +
          findingHeader(finding, index, "actions") +
          '<div class="vm-card-body">' +
          '<div class="vm-actions-summary">' +
          '<span class="vm-actions-count">' +
          lra.total_actions +
          " Actions Available</span>";
        if (lra.detected_root_causes && lra.detected_root_causes.length) {
          dbaActionsHtml += '<div class="vm-actions-causes">';
          lra.detected_root_causes.forEach(function (rc) {
            dbaActionsHtml +=
              '<span class="vm-issue-chip">' +
              catIcon(rc) +
              " " +
              rc.replace(/_/g, " ") +
              "</span>";
          });
          dbaActionsHtml += "</div>";
        }
        dbaActionsHtml += "</div>";
        lra.actions.forEach(function (action) {
          dbaActionsHtml += renderActionCard(action);
        });
        dbaActionsHtml += "</div></div>";
      } else {
        dbaActionsHtml +=
          '<div class="vm-card">' +
          findingHeader(finding, index, "actions") +
          '<div class="vm-card-body"><div class="vm-empty-state small"><p>No specific load reduction actions generated.</p></div></div></div>';
      }

      // Fix Recommendations
      fixRecsHtml +=
        '<div class="vm-card">' +
        findingHeader(finding, index, "fix") +
        '<div class="vm-card-body">' +
        renderFixRecsForFinding(finding) +
        "</div></div>";

      // Action Plan
      actionPlanHtml +=
        '<div class="vm-card">' +
        findingHeader(finding, index, "plan") +
        '<div class="vm-card-body">' +
        renderActionPlan(finding) +
        "</div></div>";
    });
  } else {
    var emptyMsg =
      '<div class="vm-empty-state"><svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="#22c55e" stroke-width="1.5"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22,4 12,14.01 9,11.01"/></svg><h3>No Problematic SQL Identified</h3><p>Database workload appears healthy.</p></div>';
    rcaHtml = emptyMsg;
    dbaActionsHtml = emptyMsg;
    fixRecsHtml = emptyMsg;
    actionPlanHtml = emptyMsg;
  }

  // Badge
  var countBadge = document.getElementById("rcaFindingCount");
  if (countBadge)
    countBadge.textContent =
      findings.length + " Finding" + (findings.length !== 1 ? "s" : "");

  // Populate
  var ws = document.getElementById("workloadSummaryContent");
  if (ws) {
    ws.innerHTML = overviewHtml;
    document.getElementById("workloadSummarySection").style.display = "block";
  }
  var rc = document.getElementById("rcaContent");
  if (rc) rc.innerHTML = rcaHtml;
  var da = document.getElementById("dbaActionsContent");
  if (da) da.innerHTML = dbaActionsHtml;
  var fr = document.getElementById("fixRecsContent");
  if (fr) fr.innerHTML = fixRecsHtml;
  var ap = document.getElementById("actionPlanContent");
  if (ap) ap.innerHTML = actionPlanHtml;

  switchView("overview");
}

// ===============================================
// FALLBACK HELPERS
// ===============================================
if (typeof formatRootCauseAnalysis !== "function") {
  window.formatRootCauseAnalysis = function (finding) {
    return analyzeRootCause(finding);
  };
}
if (typeof formatFixRecommendations !== "function") {
  window.formatFixRecommendations = function (_recs, _sqlId, finding) {
    return renderActionPlan(finding);
  };
}
if (typeof formatIssueName !== "function") {
  window.formatIssueName = function (issue) {
    return catIcon(issue) + " " + issue.replace(/_/g, " ");
  };
}
if (typeof getCategoryColor !== "function") {
  window.getCategoryColor = function (cat) {
    return catColor(cat);
  };
}

// Explicitly expose critical functions to global scope
window.displayDBAExpertAnalysis = displayDBAExpertAnalysis;
window.switchView = switchView;
window.showDashboardView = showDashboardView;
