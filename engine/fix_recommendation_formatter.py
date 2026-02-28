"""
Fix Recommendation Formatter - UI-Ready DBA Executable Actions
===============================================================

This module transforms Load Reduction Engine output into UI-ready,
DBA-executable fix recommendations that match the DBA Action Plan format.

CRITICAL IDENTITY:
This system is NOT an analysis tool.
It is a DATABASE LOAD REDUCTION ASSISTANT.

OUTPUT REQUIREMENTS:
1. Fix Recommendations MUST look IDENTICAL to DBA Action Plan sections
2. Same section headers, code blocks, step numbering, copy buttons
3. Every problematic SQL gets executable DBA queries
4. No plain text analysis - only execution-ready fixes

MANDATORY OUTPUT STRUCTURE:
For EVERY problematic SQL_ID, output must contain:
A) Problem Summary (existing - keep it)
B) DBA ACTION PLAN (analysis + checks)
C) FIX RECOMMENDATIONS (DIRECT LOAD REDUCTION QUERIES)

DECISION LOGIC (STRICT - DO NOT DUMP ALL FIXES BLINDLY):
- IF io_wait_pct > 60% → Enable IO Reduction + SQL Access Advisor
- IF avg_exec_time > threshold AND executions < 50 → Enable PX Section
- IF plan_instability_detected → Enable Plan Stability Section
- IF cpu_pct > 50% → Enable CPU Section
- Each SQL_ID may trigger MULTIPLE sections
"""

from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum


class FixCategory(Enum):
    """Fix recommendation categories based on root cause"""
    IO_REDUCTION = "IO_REDUCTION"
    SQL_ACCESS_ADVISOR = "SQL_ACCESS_ADVISOR"
    PARALLEL_EXECUTION = "PARALLEL_EXECUTION"
    PLAN_STABILITY = "PLAN_STABILITY"
    CPU_REDUCTION = "CPU_REDUCTION"


@dataclass
class FixStep:
    """A single executable step in a fix recommendation"""
    step_number: int
    title: str
    sql_code: str
    why_this_helps: str
    priority: str  # CRITICAL, HIGH, MEDIUM


@dataclass
class FixSection:
    """A complete fix recommendation section"""
    category: FixCategory
    section_title: str
    section_icon: str
    priority_tag: str
    why_shown: str
    steps: List[FixStep]
    expected_improvement: str


@dataclass
class FixRecommendationResult:
    """Complete fix recommendations for a SQL"""
    sql_id: str
    detected_issues: List[str]
    fix_sections: List[FixSection]
    summary: str


class FixRecommendationFormatter:
    """
    Formats fix recommendations into UI-ready, DBA-executable format.
    
    OUTPUT MUST MATCH DBA ACTION PLAN STYLE:
    - Section headers with icons
    - Numbered steps
    - Code blocks with copy buttons
    - Priority tags
    - "Why this helps" explanations
    
    This is NOT plain text output. It's structured for UI rendering.
    """
    
    # Thresholds for fix selection (STRICT LOGIC)
    IO_THRESHOLD = 60.0        # io_wait_pct > 60%
    CPU_THRESHOLD = 50.0       # cpu_pct > 50%
    BATCH_EXEC_TIME = 5.0      # avg_exec_time > 5s
    BATCH_MAX_EXECS = 50       # executions < 50
    
    def __init__(self) -> None:
        self._format_log: List[str] = []
    
    def generate_fix_recommendations(
        self,
        sql_id: str,
        io_wait_pct: float,
        cpu_pct: float,
        avg_exec_time: float,
        executions: int,
        total_elapsed: float,
        plan_instability: bool = False,
        full_table_scan: bool = False,
        high_io_detected: bool = False
    ) -> FixRecommendationResult:
        """
        Generate fix recommendations based on SIGNAL-BASED LOGIC.
        
        DO NOT dump all fixes blindly.
        Only show relevant fixes based on detected root causes.
        
        Args:
            sql_id: The SQL ID to generate fixes for
            io_wait_pct: IO wait percentage (0-100)
            cpu_pct: CPU percentage (0-100)
            avg_exec_time: Average execution time in seconds
            executions: Number of executions
            total_elapsed: Total elapsed time in seconds
            plan_instability: Whether plan instability was detected
            full_table_scan: Whether full table scans were detected
            high_io_detected: Whether high IO was detected from ASH
        
        Returns:
            FixRecommendationResult with UI-ready fix sections
        """
        detected_issues: List[str] = []
        fix_sections: List[FixSection] = []
        
        # =====================================================================
        # SIGNAL-BASED DECISION LOGIC (MANDATORY - DO NOT SKIP)
        # =====================================================================
        
        # 1️⃣ IO DOMINANT → Show IO Reduction + SQL Access Advisor
        is_io_dominant: bool = io_wait_pct > self.IO_THRESHOLD or full_table_scan or high_io_detected
        if is_io_dominant:
            detected_issues.append("IO_DOMINANT")
            fix_sections.append(self._generate_io_reduction_section(sql_id, io_wait_pct))
            # SQL Access Advisor is MANDATORY for IO-dominant queries
            fix_sections.append(self._generate_sql_access_advisor_section(sql_id, io_wait_pct))
        
        # 2️⃣ BATCH PATTERN → Show Parallel Execution
        is_batch_pattern: bool = avg_exec_time > self.BATCH_EXEC_TIME and executions < self.BATCH_MAX_EXECS
        if is_batch_pattern:
            detected_issues.append("BATCH_PATTERN")
            fix_sections.append(self._generate_parallel_execution_section(sql_id, avg_exec_time, executions))
        
        # 3️⃣ PLAN INSTABILITY → Show Plan Stability
        if plan_instability:
            detected_issues.append("PLAN_INSTABILITY")
            fix_sections.append(self._generate_plan_stability_section(sql_id))
        
        # 4️⃣ HIGH CPU → Show CPU Reduction
        is_cpu_dominant: bool = cpu_pct > self.CPU_THRESHOLD
        if is_cpu_dominant:
            detected_issues.append("HIGH_CPU")
            fix_sections.append(self._generate_cpu_reduction_section(sql_id, cpu_pct))
        
        # 5️⃣ If no specific issue detected but high impact, suggest general optimization
        if not fix_sections and total_elapsed > 30:
            detected_issues.append("HIGH_IMPACT")
            fix_sections.append(self._generate_general_optimization_section(sql_id, total_elapsed))
        
        # Sort by priority (CRITICAL first)
        priority_order: Dict[str, int] = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}
        fix_sections.sort(key=lambda s: priority_order.get(s.priority_tag, 3))
        
        # Generate summary
        summary: str = self._generate_summary(sql_id, detected_issues, fix_sections)
        
        return FixRecommendationResult(
            sql_id=sql_id,
            detected_issues=detected_issues,
            fix_sections=fix_sections,
            summary=summary
        )
    
    # =========================================================================
    # SECTION 1: IO REDUCTION - Missing Index Analysis
    # =========================================================================
    def _generate_io_reduction_section(self, sql_id: str, io_wait_pct: float) -> FixSection:
        """
        Generate IO Reduction fix section.
        
        Trigger: io_wait_pct > 60% OR full_table_scan detected
        """
        steps: List[FixStep] = [
            FixStep(
                step_number=1,
                title="Identify Objects Accessed by SQL",
                sql_code=f"""-- Step 1: Find all objects accessed by this SQL
SELECT DISTINCT
    p.object_owner,
    p.object_name,
    p.object_type,
    p.operation,
    p.options
FROM v$sql_plan p
WHERE p.sql_id = '{sql_id}'
  AND p.object_owner IS NOT NULL
ORDER BY p.object_owner, p.object_name;""",
                why_this_helps="Identifies which tables are being accessed - focus indexing efforts here",
                priority="CRITICAL"
            ),
            FixStep(
                step_number=2,
                title="Check Existing Indexes on Accessed Tables",
                sql_code=f"""-- Step 2: Review existing indexes
SELECT
    ic.table_owner,
    ic.table_name,
    ic.index_name,
    LISTAGG(ic.column_name, ', ') WITHIN GROUP (ORDER BY ic.column_position) AS index_columns,
    i.visibility,
    i.status
FROM dba_ind_columns ic
JOIN dba_indexes i ON ic.index_name = i.index_name AND ic.index_owner = i.owner
WHERE ic.table_name IN (
    SELECT object_name FROM v$sql_plan 
    WHERE sql_id = '{sql_id}' AND object_type = 'TABLE'
)
GROUP BY ic.table_owner, ic.table_name, ic.index_name, i.visibility, i.status
ORDER BY ic.table_name, ic.index_name;""",
                why_this_helps="Reveals what indexes exist - may need composite index or different column order",
                priority="HIGH"
            ),
            FixStep(
                step_number=3,
                title="Find High Physical Read Segments (Index Candidates)",
                sql_code=f"""-- Step 3: High physical read segments
SELECT
    ss.owner,
    ss.object_name,
    ss.object_type,
    ss.statistic_name,
    ss.value AS physical_reads
FROM v$segment_statistics ss
WHERE ss.statistic_name = 'physical reads'
  AND ss.object_name IN (
    SELECT object_name FROM v$sql_plan WHERE sql_id = '{sql_id}'
  )
ORDER BY ss.value DESC;""",
                why_this_helps="High physical reads = disk IO = slow. These segments need indexes most urgently.",
                priority="HIGH"
            )
        ]
        
        # Determine why_shown text based on IO wait percentage
        if io_wait_pct < 10:
            why_shown_text = "IO wait is negligible; CPU is the primary root cause. High physical reads indicate full table scans. Adding appropriate indexes will reduce IO dramatically."
        else:
            why_shown_text: str = f"IO wait is {io_wait_pct:.1f}% (threshold: 60%). High physical reads indicate full table scans. Adding appropriate indexes will reduce IO dramatically."
        
        return FixSection(
            category=FixCategory.IO_REDUCTION,
            section_title="1️⃣ IO Reduction – Missing Index Analysis",
            section_icon="🔴",
            priority_tag="CRITICAL" if io_wait_pct > 80 else "HIGH",
            why_shown=why_shown_text,
            steps=steps,
            expected_improvement="40-70% reduction in elapsed time after proper indexing"
        )
    
    # =========================================================================
    # SECTION 2: SQL ACCESS ADVISOR - Index Recommendation (HIGHEST ROI)
    # =========================================================================
    def _generate_sql_access_advisor_section(self, sql_id: str, io_wait_pct: float) -> FixSection:
        """
        Generate SQL Access Advisor section.
        
        MANDATORY when IO-dominant. This is the HIGHEST ROI action.
        """
        steps: List[FixStep] = [
            FixStep(
                step_number=1,
                title="Create SQL Tuning Task for Index Recommendations",
                sql_code=f"""-- Step 1: Create tuning task
DECLARE
    l_task_name VARCHAR2(30);
BEGIN
    l_task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(
        sql_id          => '{sql_id}',
        scope           => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,
        time_limit      => 300,
        task_name       => 'TUNE_{sql_id}',
        description     => 'Index recommendation task for SQL {sql_id}'
    );
    DBMS_OUTPUT.PUT_LINE('Task created: ' || l_task_name);
END;
/""",
                why_this_helps="Creates a comprehensive tuning analysis job that Oracle will execute",
                priority="CRITICAL"
            ),
            FixStep(
                step_number=2,
                title="Execute the Tuning Task",
                sql_code=f"""-- Step 2: Execute the task
BEGIN
    DBMS_SQLTUNE.EXECUTE_TUNING_TASK(
        task_name => 'TUNE_{sql_id}'
    );
END;
/

-- Check task status
SELECT task_name, status, execution_start, execution_end
FROM dba_advisor_log
WHERE task_name = 'TUNE_{sql_id}';""",
                why_this_helps="Runs Oracle's optimizer to analyze this specific SQL and generate recommendations",
                priority="CRITICAL"
            ),
            FixStep(
                step_number=3,
                title="View Index Recommendations",
                sql_code=f"""-- Step 3: Get recommendations
SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK('TUNE_{sql_id}') AS recommendations
FROM dual;

-- Alternative: View specific findings
SELECT type, message, impact
FROM dba_advisor_findings
WHERE task_name = 'TUNE_{sql_id}'
ORDER BY impact DESC;""",
                why_this_helps="Shows Oracle's specific recommendations - may include CREATE INDEX statements ready to run",
                priority="HIGH"
            ),
            FixStep(
                step_number=4,
                title="[Optional] SQL Access Advisor Workflow",
                sql_code=f"""-- Alternative: Full SQL Access Advisor
DECLARE
    l_task_name VARCHAR2(30) := 'ACCESS_ADV_{sql_id}';
BEGIN
    -- Create advisor task
    DBMS_ADVISOR.CREATE_TASK(
        advisor_name => 'SQL Access Advisor',
        task_name    => l_task_name
    );
    
    -- Set task parameters
    DBMS_ADVISOR.SET_TASK_PARAMETER(
        task_name => l_task_name,
        parameter => 'VALID_TABLE_LIST',
        value     => 'SCHEMA.%'  -- Adjust schema
    );
    
    -- Execute
    DBMS_ADVISOR.EXECUTE_TASK(task_name => l_task_name);
    
    DBMS_OUTPUT.PUT_LINE('Access Advisor complete: ' || l_task_name);
END;
/""",
                why_this_helps="Full SQL Access Advisor can recommend materialized views and partitioning in addition to indexes",
                priority="MEDIUM"
            )
        ]
        
        return FixSection(
            category=FixCategory.SQL_ACCESS_ADVISOR,
            section_title="2️⃣ SQL Access Advisor – Index Recommendation (HIGHEST ROI)",
            section_icon="🎯",
            priority_tag="CRITICAL",
            why_shown=f"IO wait at {io_wait_pct:.1f}%. SQL Access Advisor provides automated index recommendations with expected improvement percentages. This is the safest, highest ROI action.",
            steps=steps,
            expected_improvement="60-90% IO reduction with advisor-recommended indexes"
        )
    
    # =========================================================================
    # SECTION 3: PARALLEL EXECUTION - Batch Runtime Reduction
    # =========================================================================
    def _generate_parallel_execution_section(self, sql_id: str, avg_exec_time: float, executions: int) -> FixSection:
        """
        Generate Parallel Execution section.
        
        Trigger: avg_exec_time > 5s AND executions < 50 (batch pattern)
        """
        steps: List[FixStep] = [
            FixStep(
                step_number=1,
                title="Check Current PX Usage for This SQL",
                sql_code=f"""-- Step 1: Check parallel server usage
SELECT
    sql_id,
    executions,
    px_servers_executions,
    ROUND(px_servers_executions / NULLIF(executions, 0), 2) AS avg_px_per_exec,
    elapsed_time/1e6 AS elapsed_sec,
    ROUND(elapsed_time/NULLIF(executions,0)/1e6, 2) AS avg_elapsed_sec
FROM v$sql
WHERE sql_id = '{sql_id}';""",
                why_this_helps="Shows if parallel execution is being used - if px_servers_executions is low/0, PX is not being leveraged",
                priority="HIGH"
            ),
            FixStep(
                step_number=2,
                title="Enable Parallel DML for Batch Operations",
                sql_code=f"""-- Step 2: Enable parallel DML session-level
ALTER SESSION ENABLE PARALLEL DML;
ALTER SESSION FORCE PARALLEL DML PARALLEL 4;

-- Or hint the specific SQL:
-- SELECT /*+ PARALLEL(t, 4) */ ... FROM table_name t ...

-- For DML:
-- INSERT /*+ APPEND PARALLEL(4) */ INTO target_table ...
-- UPDATE /*+ PARALLEL(t, 4) */ table_name t SET ...""",
                why_this_helps="Parallel execution divides work across multiple CPU cores - can reduce batch runtime by 50-80%",
                priority="HIGH"
            ),
            FixStep(
                step_number=3,
                title="Validate PX in Execution Plan",
                sql_code=f"""-- Step 3: Check if parallel is in the plan
SELECT
    id,
    operation,
    options,
    object_name,
    other_tag,
    distribution
FROM v$sql_plan
WHERE sql_id = '{sql_id}'
  AND (operation LIKE '%PX%' 
       OR other_tag LIKE '%PX%'
       OR distribution IS NOT NULL)
ORDER BY id;

-- If empty, PX is not being used for this SQL""",
                why_this_helps="Confirms whether parallel execution is actually happening - 'PX COORDINATOR' in plan means parallel is active",
                priority="MEDIUM"
            )
        ]
        
        return FixSection(
            category=FixCategory.PARALLEL_EXECUTION,
            section_title="3️⃣ Parallel Execution – Batch Runtime Reduction",
            section_icon="⚡",
            priority_tag="HIGH",
            why_shown=f"Average execution time is {avg_exec_time:.1f}s with only {executions} executions. This batch pattern can benefit from parallel execution to reduce runtime by 50-70%.",
            steps=steps,
            expected_improvement="50-70% runtime reduction with proper parallel configuration"
        )
    
    # =========================================================================
    # SECTION 4: PLAN STABILITY - Prevent Regression
    # =========================================================================
    def _generate_plan_stability_section(self, sql_id: str) -> FixSection:
        """
        Generate Plan Stability section.
        
        Trigger: plan instability detected OR high IO + low CPU pattern
        """
        steps: List[FixStep] = [
            FixStep(
                step_number=1,
                title="Capture Current Execution Plan with Statistics",
                sql_code=f"""-- Step 1: Get current plan with runtime stats
SELECT * FROM TABLE(
    DBMS_XPLAN.DISPLAY_CURSOR(
        sql_id => '{sql_id}',
        format => 'ALLSTATS LAST +ALIAS +OUTLINE +IOSTATS'
    )
);

-- Compare E-Rows vs A-Rows for cardinality issues
-- Look for operations with high STARTS count""",
                why_this_helps="Captures the current plan - if it's good, we'll lock it; if bad, we'll investigate further",
                priority="CRITICAL"
            ),
            FixStep(
                step_number=2,
                title="Load Good Plan into SQL Plan Baseline",
                sql_code=f"""-- Step 2: Create SQL Plan Baseline
DECLARE
    l_plans PLS_INTEGER;
BEGIN
    l_plans := DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(
        sql_id          => '{sql_id}',
        plan_hash_value => NULL,  -- Load all plans for this SQL
        enabled         => 'YES',
        fixed           => 'NO'   -- Set to 'YES' to force this plan
    );
    DBMS_OUTPUT.PUT_LINE('Plans loaded: ' || l_plans);
END;
/""",
                why_this_helps="SQL Plan Baseline prevents the optimizer from choosing a worse plan in the future",
                priority="CRITICAL"
            ),
            FixStep(
                step_number=3,
                title="Verify Baseline Was Created",
                sql_code=f"""-- Step 3: Confirm baseline exists
SELECT
    sql_handle,
    plan_name,
    origin,
    enabled,
    accepted,
    fixed,
    created,
    last_executed
FROM dba_sql_plan_baselines
WHERE signature = (
    SELECT exact_matching_signature
    FROM v$sql
    WHERE sql_id = '{sql_id}'
    AND ROWNUM = 1
);

-- If you need to fix the baseline (force it):
-- BEGIN
--     DBMS_SPM.ALTER_SQL_PLAN_BASELINE(
--         sql_handle => '<sql_handle>',
--         plan_name  => '<plan_name>',
--         attribute_name => 'FIXED',
--         attribute_value => 'YES'
--     );
-- END;
-- /""",
                why_this_helps="Confirms the baseline is active - 'accepted=YES' means optimizer will use this plan",
                priority="HIGH"
            )
        ]
        
        return FixSection(
            category=FixCategory.PLAN_STABILITY,
            section_title="4️⃣ Execution Plan Stability – Prevent Regression",
            section_icon="📌",
            priority_tag="HIGH",
            why_shown="Plan instability detected. Locking a known good plan prevents unpredictable performance spikes that cause load problems.",
            steps=steps,
            expected_improvement="Eliminates surprise load events from plan regression"
        )
    
    # =========================================================================
    # SECTION 5: CPU REDUCTION - High CPU SQL Optimization
    # =========================================================================
    def _generate_cpu_reduction_section(self, sql_id: str, cpu_pct: float) -> FixSection:
        """
        Generate CPU Reduction section.
        
        Trigger: cpu_pct > 50%
        """
        steps: List[FixStep] = [
            FixStep(
                step_number=1,
                title="Identify CPU-Heavy Operations in Plan",
                sql_code=f"""-- Step 1: Find CPU-expensive operations
SELECT
    id,
    operation,
    options,
    object_name,
    cpu_cost,
    io_cost,
    cardinality,
    bytes,
    ROUND(cpu_cost / NULLIF(io_cost, 0), 2) AS cpu_to_io_ratio
FROM v$sql_plan
WHERE sql_id = '{sql_id}'
  AND cpu_cost > 0
ORDER BY cpu_cost DESC;

-- High cpu_cost with low io_cost = CPU-bound operation
-- Common culprits: SORT, HASH JOIN, FILTER""",
                why_this_helps="Pinpoints which operations are consuming CPU - focus optimization efforts here",
                priority="CRITICAL"
            ),
            FixStep(
                step_number=2,
                title="Review Execution Plan for CPU Hotspots",
                sql_code=f"""-- Step 2: Full plan with CPU metrics
SELECT * FROM TABLE(
    DBMS_XPLAN.DISPLAY_CURSOR(
        sql_id => '{sql_id}',
        format => 'ALLSTATS LAST +COST'
    )
);

-- Look for:
-- 1. SORT ORDER BY with high Buffers - consider index to avoid sort
-- 2. HASH JOIN with many rows - check join order
-- 3. FILTER with many STARTS - scalar subquery issue""",
                why_this_helps="Detailed plan shows exactly where CPU is being consumed and why",
                priority="HIGH"
            ),
            FixStep(
                step_number=3,
                title="Consider Join Method Optimization",
                sql_code=f"""-- Step 3: Test alternative join methods
-- If HASH JOIN is expensive, try NESTED LOOPS:
SELECT /*+ USE_NL(a b) INDEX(b idx_name) */ ...

-- If NESTED LOOPS is expensive on large sets, try HASH:
SELECT /*+ USE_HASH(a b) */ ...

-- Force specific join order:
SELECT /*+ LEADING(small_table big_table) USE_HASH(big_table) */ ...

-- Current top CPU SQLs for context:
SELECT sql_id, cpu_time/1e6 cpu_sec, executions, buffer_gets
FROM v$sql
ORDER BY cpu_time DESC
FETCH FIRST 10 ROWS ONLY;""",
                why_this_helps="Wrong join method is #1 cause of CPU waste - changing join method can reduce CPU by 50%+",
                priority="HIGH"
            )
        ]
        
        return FixSection(
            category=FixCategory.CPU_REDUCTION,
            section_title="5️⃣ High CPU SQL Reduction",
            section_icon="🔥",
            priority_tag="HIGH" if cpu_pct > 70 else "MEDIUM",
            why_shown=f"CPU percentage is {cpu_pct:.1f}% (threshold: 50%). High CPU often indicates inefficient join methods, excessive sorting, or scalar subqueries.",
            steps=steps,
            expected_improvement="30-50% CPU reduction with optimized join methods"
        )
    
    # =========================================================================
    # SECTION 6: GENERAL OPTIMIZATION (Fallback)
    # =========================================================================
    def _generate_general_optimization_section(self, sql_id: str, total_elapsed: float) -> FixSection:
        """
        Generate general optimization section for high-impact queries
        without specific IO/CPU signals.
        """
        steps: List[FixStep] = [
            FixStep(
                step_number=1,
                title="Run Comprehensive SQL Tuning Advisor",
                sql_code=f"""-- Run full SQL Tuning Advisor
DECLARE
    l_task VARCHAR2(30);
BEGIN
    l_task := DBMS_SQLTUNE.CREATE_TUNING_TASK(
        sql_id      => '{sql_id}',
        scope       => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,
        time_limit  => 600,  -- 10 minutes
        task_name   => 'COMPREHENSIVE_TUNE_{sql_id}'
    );
    
    DBMS_SQLTUNE.EXECUTE_TUNING_TASK(l_task);
    DBMS_OUTPUT.PUT_LINE('Task complete: ' || l_task);
END;
/

SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK('COMPREHENSIVE_TUNE_{sql_id}') 
FROM dual;""",
                why_this_helps="Comprehensive analysis covers indexes, statistics, SQL profiles, and restructuring",
                priority="HIGH"
            ),
            FixStep(
                step_number=2,
                title="Verify Statistics Are Current",
                sql_code=f"""-- Check statistics age
SELECT
    table_name,
    last_analyzed,
    num_rows,
    stale_stats,
    ROUND(SYSDATE - last_analyzed) AS days_old
FROM dba_tab_statistics
WHERE table_name IN (
    SELECT DISTINCT object_name 
    FROM v$sql_plan 
    WHERE sql_id = '{sql_id}' 
    AND object_type = 'TABLE'
)
ORDER BY last_analyzed NULLS FIRST;

-- Gather fresh statistics if stale
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname          => 'SCHEMA_NAME',
        tabname          => 'TABLE_NAME',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt       => 'FOR ALL COLUMNS SIZE AUTO',
        cascade          => TRUE
    );
END;
/""",
                why_this_helps="Stale statistics cause optimizer to choose bad plans - refresh fixes many issues",
                priority="MEDIUM"
            )
        ]
        
        return FixSection(
            category=FixCategory.IO_REDUCTION,
            section_title="🔧 General SQL Optimization",
            section_icon="🔧",
            priority_tag="MEDIUM",
            why_shown=f"Total elapsed time is {total_elapsed:.1f}s - high impact query that warrants optimization even without specific IO/CPU signals.",
            steps=steps,
            expected_improvement="20-40% improvement with comprehensive tuning"
        )
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    def _generate_summary(
        self,
        sql_id: str,
        detected_issues: List[str],
        fix_sections: List[FixSection]
    ) -> str:
        """Generate summary of fix recommendations"""
        if not fix_sections:
            return f"SQL {sql_id}: No specific fix recommendations - standard monitoring advised."
        
        issue_str: str = ", ".join(detected_issues)
        section_count: int = len(fix_sections)
        
        improvements = []
        for section in fix_sections:
            improvements.append(f"• {section.section_title}: {section.expected_improvement}")
        
        return f"""SQL {sql_id} - Fix Recommendations
Detected Issues: {issue_str}
Total Fix Sections: {section_count}

Expected Improvements:
{chr(10).join(improvements)}"""
    
    def to_dict(self, result: FixRecommendationResult) -> Dict[str, Any]:
        """Convert FixRecommendationResult to dictionary for JSON/UI consumption"""
        return {
            "sql_id": result.sql_id,
            "detected_issues": result.detected_issues,
            "summary": result.summary,
            "total_sections": len(result.fix_sections),
            "fix_sections": [
                {
                    "category": section.category.value,
                    "section_title": section.section_title,
                    "section_icon": section.section_icon,
                    "priority_tag": section.priority_tag,
                    "why_shown": section.why_shown,
                    "expected_improvement": section.expected_improvement,
                    "steps": [
                        {
                            "step_number": step.step_number,
                            "title": step.title,
                            "sql_code": step.sql_code,
                            "why_this_helps": step.why_this_helps,
                            "priority": step.priority
                        }
                        for step in section.steps
                    ]
                }
                for section in result.fix_sections
            ]
        }
    
    def to_ui_html(self, result: FixRecommendationResult) -> str:
        """
        Generate UI-ready HTML that matches DBA Action Plan format.
        
        This is for server-side rendering if needed.
        Client-side rendering is preferred via results.js
        """
        if not result.fix_sections:
            return ""
        
        html_parts: List[str] = [
            '<div class="fix-recommendations-container">',
            f'<div class="fix-recommendations-header">',
            f'  <span class="fix-icon">⚡</span>',
            f'  <h3>Fix Recommendations - SQL_ID: {result.sql_id}</h3>',
            f'  <span class="fix-count-badge">{len(result.fix_sections)} Action{"s" if len(result.fix_sections) > 1 else ""}</span>',
            f'</div>',
            f'<div class="detected-issues">',
            f'  <strong>Detected Issues:</strong> {", ".join(result.detected_issues)}',
            f'</div>'
        ]
        
        for section in result.fix_sections:
            html_parts.append(f'<div class="fix-section category-{section.category.value.lower()}">')
            html_parts.append(f'  <div class="fix-section-header">')
            html_parts.append(f'    <span class="section-icon">{section.section_icon}</span>')
            html_parts.append(f'    <h4>{section.section_title}</h4>')
            html_parts.append(f'    <span class="priority-badge priority-{section.priority_tag.lower()}">{section.priority_tag}</span>')
            html_parts.append(f'  </div>')
            html_parts.append(f'  <div class="why-shown">')
            html_parts.append(f'    <strong>Why:</strong> {section.why_shown}')
            html_parts.append(f'  </div>')
            html_parts.append(f'  <div class="fix-steps">')
            
            for step in section.steps:
                html_parts.append(f'    <div class="fix-step">')
                html_parts.append(f'      <div class="step-header">')
                html_parts.append(f'        <span class="step-number">Step {step.step_number}</span>')
                html_parts.append(f'        <span class="step-title">{step.title}</span>')
                html_parts.append(f'      </div>')
                html_parts.append(f'      <pre class="sql-code"><code>{step.sql_code}</code></pre>')
                html_parts.append(f'      <button class="copy-sql-btn" onclick="copySqlToClipboard(this)">📋 Copy</button>')
                html_parts.append(f'      <p class="why-helps"><strong>Why:</strong> {step.why_this_helps}</p>')
                html_parts.append(f'    </div>')
            
            html_parts.append(f'  </div>')
            html_parts.append(f'  <div class="expected-improvement">')
            html_parts.append(f'    <span class="improvement-icon">💡</span>')
            html_parts.append(f'    <strong>Expected Improvement:</strong> {section.expected_improvement}')
            html_parts.append(f'  </div>')
            html_parts.append(f'</div>')
        
        html_parts.append('</div>')
        
        return '\n'.join(html_parts)


# =========================================================================
# INTEGRATION HELPER FUNCTION
# =========================================================================
def generate_fix_recommendations_for_finding(finding: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate fix recommendations for a DBA finding.
    
    This function integrates with DBA Expert Engine findings.
    
    Args:
        finding: A finding dictionary from DBA Expert Engine
    
    Returns:
        Fix recommendations as dictionary ready for UI consumption
    """
    formatter = FixRecommendationFormatter()
    
    # Extract metrics from finding
    sql_id = finding.get('sql_id', 'UNKNOWN')
    exec_pattern = finding.get('execution_pattern', {})
    technical_params = finding.get('technical_parameters', {})
    interpretation = finding.get('dba_interpretation', '').lower()
    
    # Get metrics - try multiple sources
    io_wait_pct = technical_params.get('io_percentage', 0) or exec_pattern.get('io_pct', 0)
    cpu_pct = technical_params.get('cpu_percentage', 0) or exec_pattern.get('cpu_pct', 0)
    avg_exec_time = technical_params.get('avg_elapsed_per_exec_s', 0) or exec_pattern.get('avg_elapsed_per_exec', 0)
    executions = technical_params.get('executions', 0) or exec_pattern.get('total_executions', 0)
    total_elapsed = technical_params.get('total_elapsed_time_s', 0) or exec_pattern.get('total_elapsed', 0)
    
    # Detect conditions from interpretation text
    plan_instability: bool = any(x in interpretation for x in ['plan instab', 'plan regression', 'unstable plan'])
    full_table_scan: bool = any(x in interpretation for x in ['full scan', 'table scan', 'full table'])
    high_io_detected: bool = any(x in interpretation for x in ['i/o', 'io-heavy', 'disk read', 'physical read'])
    
    result: FixRecommendationResult = formatter.generate_fix_recommendations(
        sql_id=sql_id,
        io_wait_pct=io_wait_pct,
        cpu_pct=cpu_pct,
        avg_exec_time=avg_exec_time,
        executions=executions,
        total_elapsed=total_elapsed,
        plan_instability=plan_instability,
        full_table_scan=full_table_scan,
        high_io_detected=high_io_detected
    )
    
    return formatter.to_dict(result)
