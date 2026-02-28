"""
Load Reduction Engine - DBA Action Query Generator
===================================================

This module transforms the RCA system from an "Analysis Tool" into a 
"Load Reduction Assistant" by generating DBA-executable queries that 
help REDUCE high database load for each problematic SQL.

CRITICAL PRINCIPLES:
1. Agent must NOT dump all queries blindly
2. Agent must classify the root cause per SQL
3. Based on detected root cause, generate ONLY relevant DBA action queries
4. Queries must be practical, production-safe, and DBA-ready
5. Every problematic SQL must have its own condition-based action set

ROOT CAUSE CATEGORIES:
1. IO_DOMINANT - io_wait_pct > 60%, heavy physical reads, full table scans
2. PX_INEFFECTIVE - high avg_exec_time, low executions (batch pattern), PX not used
3. BAD_EXECUTION_PLAN - plan instability, row estimate mismatch, sudden regression
4. HIGH_CPU - cpu_pct > 50%, high CPU time consumption
5. MISSING_INDEX - IO dominant SQL, missing index suspected (triggers SQL Access Advisor)
"""

from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum


class RootCauseCategory(Enum):
    """Root cause categories for load reduction actions"""
    IO_DOMINANT = "IO_DOMINANT"
    PX_INEFFECTIVE = "PX_INEFFECTIVE"
    BAD_EXECUTION_PLAN = "BAD_EXECUTION_PLAN"
    HIGH_CPU = "HIGH_CPU"
    MISSING_INDEX = "MISSING_INDEX"
    MIXED = "MIXED"


@dataclass
class LoadReductionAction:
    """A single load reduction action with SQL and explanation"""
    category: RootCauseCategory
    title: str
    sql_queries: List[str]
    dba_action_text: str
    why_this_helps: str
    priority: int  # 1 = highest priority


@dataclass
class LoadReductionResult:
    """Complete load reduction result for a SQL"""
    sql_id: str
    detected_root_causes: List[RootCauseCategory]
    actions: List[LoadReductionAction]
    summary: str
    total_actions: int


class LoadReductionEngine:
    """
    Load Reduction Engine
    
    Analyzes SQL signals and generates targeted DBA action queries
    based on detected root causes. This engine follows strict condition-based
    logic to ensure only relevant actions are shown.
    
    AGENT DECISION RULES (STRICT):
    - IF io_wait_pct > 60% → Show IO Section + SQL Access Advisor Section
    - IF avg_exec_time > threshold AND executions < threshold → Show PX Section
    - IF plan_instability_detected → Show Plan Stability Section
    - IF cpu_pct > 50% → Show CPU Section
    """
    
    # Thresholds for root cause detection
    IO_DOMINANT_THRESHOLD = 60.0      # io_wait_pct > 60%
    HIGH_CPU_THRESHOLD = 50.0         # cpu_pct > 50%
    BATCH_SQL_MIN_EXEC_TIME = 5.0     # avg_exec_time > 5s
    BATCH_SQL_MAX_EXECUTIONS = 50     # executions < 50
    
    def __init__(self) -> None:
        """Initialize the Load Reduction Engine"""
        self._generation_log: List[Dict[str, Any]] = []
    
    def analyze_and_generate_actions(
        self,
        sql_id: str,
        io_wait_pct: float,
        cpu_pct: float,
        avg_exec_time: float,
        executions: int,
        total_elapsed: float,
        plan_instability: bool = False,
        full_table_scan_detected: bool = False
    ) -> LoadReductionResult:
        """
        Main entry point - Analyze SQL and generate load reduction actions
        
        Args:
            sql_id: The SQL ID to analyze
            io_wait_pct: IO wait percentage (0-100)
            cpu_pct: CPU percentage (0-100)
            avg_exec_time: Average execution time in seconds
            executions: Number of executions
            total_elapsed: Total elapsed time in seconds
            plan_instability: Whether plan instability was detected
            full_table_scan_detected: Whether full table scans were detected
        
        Returns:
            LoadReductionResult with all applicable actions
        """
        detected_causes: List[RootCauseCategory] = []
        actions: List[LoadReductionAction] = []
        
        # =====================================================================
        # ROOT CAUSE CLASSIFICATION (MANDATORY LOGIC)
        # =====================================================================
        
        # 1️⃣ IO DOMINANT CHECK
        is_io_dominant: bool = io_wait_pct > self.IO_DOMINANT_THRESHOLD or full_table_scan_detected
        if is_io_dominant:
            detected_causes.append(RootCauseCategory.IO_DOMINANT)
            actions.append(self._generate_io_dominant_actions(sql_id, io_wait_pct))
            # IO dominant also triggers SQL Access Advisor (Section 5)
            detected_causes.append(RootCauseCategory.MISSING_INDEX)
            actions.append(self._generate_sql_access_advisor_actions(sql_id))
        
        # 2️⃣ PARALLEL EXECUTION INEFFECTIVE CHECK
        is_batch_pattern: bool = (
            avg_exec_time > self.BATCH_SQL_MIN_EXEC_TIME and 
            executions < self.BATCH_SQL_MAX_EXECUTIONS
        )
        if is_batch_pattern:
            detected_causes.append(RootCauseCategory.PX_INEFFECTIVE)
            actions.append(self._generate_px_actions(sql_id, avg_exec_time, executions))
        
        # 3️⃣ BAD EXECUTION PLAN CHECK
        if plan_instability:
            detected_causes.append(RootCauseCategory.BAD_EXECUTION_PLAN)
            actions.append(self._generate_plan_stability_actions(sql_id))
        
        # 4️⃣ HIGH CPU CHECK
        is_cpu_dominant: bool = cpu_pct > self.HIGH_CPU_THRESHOLD
        if is_cpu_dominant:
            detected_causes.append(RootCauseCategory.HIGH_CPU)
            actions.append(self._generate_cpu_reduction_actions(sql_id, cpu_pct))
        
        # Sort actions by priority
        actions.sort(key=lambda a: a.priority)
        
        # Generate summary
        summary: str = self._generate_summary(sql_id, detected_causes, io_wait_pct, cpu_pct, avg_exec_time)
        
        return LoadReductionResult(
            sql_id=sql_id,
            detected_root_causes=detected_causes,
            actions=actions,
            summary=summary,
            total_actions=len(actions)
        )
    
    def analyze_from_signals(self, signals: Any) -> LoadReductionResult:
        """
        Analyze from NormalizedSignals object from decision engine
        
        Args:
            signals: NormalizedSignals object
        
        Returns:
            LoadReductionResult with all applicable actions
        """
        return self.analyze_and_generate_actions(
            sql_id=signals.sql_id,
            io_wait_pct=signals.io_wait_pct,
            cpu_pct=signals.cpu_pct,
            avg_exec_time=signals.avg_exec_time,
            executions=signals.executions,
            total_elapsed=signals.total_elapsed
        )
    
    # =========================================================================
    # SECTION 1: IO DOMINANT → Missing Index & IO Reduction
    # =========================================================================
    def _generate_io_dominant_actions(self, sql_id: str, io_wait_pct: float) -> LoadReductionAction:
        """
        Generate IO reduction actions
        
        Trigger Condition:
        - io_wait_pct > 60%
        - heavy physical reads
        - full table scans detected
        """
        queries: List[str] = [
            f"""-- 1️⃣ Identify objects accessed by the SQL
SELECT DISTINCT
    object_owner,
    object_name,
    object_type
FROM v$sql_plan
WHERE sql_id = '{sql_id}'
  AND object_owner IS NOT NULL;""",
            
            f"""-- 2️⃣ Check existing indexes on accessed tables
SELECT
    table_owner,
    table_name,
    index_name,
    column_name,
    column_position
FROM dba_ind_columns
WHERE table_name IN (
    SELECT object_name
    FROM v$sql_plan
    WHERE sql_id = '{sql_id}'
)
ORDER BY table_name, index_name, column_position;""",
            
            """-- 3️⃣ High physical read segments (index candidates)
SELECT
    owner,
    object_name,
    physical_reads
FROM v$segment_statistics
WHERE statistic_name = 'physical reads'
ORDER BY physical_reads DESC
FETCH FIRST 10 ROWS ONLY;"""
        ]
        
        # Determine the appropriate "why this helps" text based on IO wait percentage
        if io_wait_pct < 10:
            # IO is negligible, CPU is likely the primary root cause
            why_text = (
                "IO wait is negligible; CPU is the primary root cause. "
                "High physical reads indicate the SQL is performing full table scans. "
                "Adding appropriate indexes will allow index range scans instead of full scans, "
                "dramatically reducing IO and database load."
            )
        else:
            why_text: str = (
                f"IO wait is {io_wait_pct:.1f}% (threshold: 60%). "
                "High physical reads indicate the SQL is performing full table scans. "
                "Adding appropriate indexes will allow index range scans instead of full scans, "
                "dramatically reducing IO and database load."
            )
        
        return LoadReductionAction(
            category=RootCauseCategory.IO_DOMINANT,
            title="🔴 IO Reduction - Missing Index Analysis",
            sql_queries=queries,
            dba_action_text=(
                "Create indexes on filter and join columns to reduce full table scans. "
                "This will reduce physical IO and overall database load."
            ),
            why_this_helps=why_text,
            priority=1  # Highest priority for IO issues
        )
    
    # =========================================================================
    # SECTION 2: PX INEFFECTIVE → Batch Runtime Reduction
    # =========================================================================
    def _generate_px_actions(self, sql_id: str, avg_exec_time: float, executions: int) -> LoadReductionAction:
        """
        Generate Parallel Execution actions
        
        Trigger Condition:
        - avg_exec_time very high
        - executions low (batch pattern)
        - px not used or ineffective
        """
        queries: List[str] = [
            f"""-- 1️⃣ Check PX server usage for this SQL
SELECT
    sql_id,
    executions,
    px_servers_executions,
    ROUND(px_servers_executions / NULLIF(executions,0), 2) AS avg_px
FROM v$sql
WHERE sql_id = '{sql_id}';""",
            
            """-- 2️⃣ Enable parallel DML for batch operations
ALTER SESSION ENABLE PARALLEL DML;""",
            
            f"""-- 3️⃣ Check if parallel degree is appropriate
SELECT 
    sql_id,
    child_number,
    plan_hash_value,
    operation,
    options,
    other_tag
FROM v$sql_plan
WHERE sql_id = '{sql_id}'
  AND (operation LIKE '%PX%' OR other_tag LIKE '%PX%')
ORDER BY id;"""
        ]
        
        return LoadReductionAction(
            category=RootCauseCategory.PX_INEFFECTIVE,
            title="⚡ Parallel Execution - Batch Runtime Reduction",
            sql_queries=queries,
            dba_action_text=(
                "Fix DOP (Degree of Parallelism) or PX downgrade issues so batch SQL finishes faster, "
                "reducing the load window and concurrency overlap."
            ),
            why_this_helps=(
                f"Average execution time is {avg_exec_time:.1f}s with only {executions} executions. "
                "This batch pattern can benefit from parallel execution. "
                "Enabling/tuning parallel DML can reduce runtime by 50-70%, "
                "reducing the window where this SQL causes database load."
            ),
            priority=2
        )
    
    # =========================================================================
    # SECTION 3: BAD EXECUTION PLAN → Plan Stability & Join Order Fix
    # =========================================================================
    def _generate_plan_stability_actions(self, sql_id: str) -> LoadReductionAction:
        """
        Generate Plan Stability actions
        
        Trigger Condition:
        - plan instability
        - row estimate mismatch
        - sudden performance regression
        """
        queries: List[str] = [
            f"""-- 1️⃣ View current execution plan with statistics
SELECT * FROM TABLE(
    DBMS_XPLAN.DISPLAY_CURSOR(
        sql_id => '{sql_id}',
        format => 'ALLSTATS LAST +ALIAS +IOSTATS'
    )
);""",
            
            f"""-- 2️⃣ Load good plan into SQL Plan Baseline
BEGIN
    DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(
        sql_id => '{sql_id}'
    );
END;
/""",
            
            f"""-- 3️⃣ Verify baseline was created
SELECT 
    sql_handle, 
    plan_name, 
    enabled, 
    accepted, 
    fixed,
    created
FROM dba_sql_plan_baselines
WHERE signature = (
    SELECT exact_matching_signature 
    FROM v$sql 
    WHERE sql_id = '{sql_id}' 
    AND ROWNUM = 1
);"""
        ]
        
        return LoadReductionAction(
            category=RootCauseCategory.BAD_EXECUTION_PLAN,
            title="📌 Plan Stability - Prevent Regression",
            sql_queries=queries,
            dba_action_text=(
                "Stabilize a known good execution plan to avoid regressions "
                "and unpredictable load spikes."
            ),
            why_this_helps=(
                "Plan instability causes unpredictable performance. "
                "By locking a known good plan using SQL Plan Baseline, "
                "you prevent the optimizer from choosing a bad plan that causes load spikes. "
                "This eliminates surprise load events."
            ),
            priority=3
        )
    
    # =========================================================================
    # SECTION 4: HIGH CPU → CPU Load Reduction
    # =========================================================================
    def _generate_cpu_reduction_actions(self, sql_id: str, cpu_pct: float) -> LoadReductionAction:
        """
        Generate CPU reduction actions
        
        Trigger Condition:
        - cpu_pct > 50%
        - high CPU time consumption
        """
        queries: List[str] = [
            f"""-- 1️⃣ Top CPU consuming SQLs (context)
SELECT
    sql_id,
    cpu_time/1000000 AS cpu_sec,
    executions,
    ROUND(cpu_time/1000000/NULLIF(executions,0), 3) AS cpu_per_exec
FROM v$sql
ORDER BY cpu_time DESC
FETCH FIRST 10 ROWS ONLY;""",
            
            f"""-- 2️⃣ Detailed execution plan for CPU analysis
SELECT * FROM TABLE(
    DBMS_XPLAN.DISPLAY_CURSOR('{sql_id}', NULL, 'ALLSTATS LAST')
);""",
            
            f"""-- 3️⃣ Check for CPU-expensive operations
SELECT 
    id,
    operation,
    options,
    cpu_cost,
    io_cost,
    cardinality,
    bytes
FROM v$sql_plan
WHERE sql_id = '{sql_id}'
  AND cpu_cost > 0
ORDER BY cpu_cost DESC;"""
        ]
        
        return LoadReductionAction(
            category=RootCauseCategory.HIGH_CPU,
            title="🔥 CPU Load Reduction",
            sql_queries=queries,
            dba_action_text=(
                "Rewrite SQL or reduce row processing early to lower CPU usage "
                "and improve overall system concurrency."
            ),
            why_this_helps=(
                f"CPU percentage is {cpu_pct:.1f}% (threshold: 50%). "
                "High CPU often indicates inefficient join methods, excessive sorting, "
                "or scalar subqueries. Identifying and fixing the CPU-intensive operation "
                "will free CPU resources for other workloads."
            ),
            priority=2
        )
    
    # =========================================================================
    # SECTION 5: SQL ACCESS ADVISOR → Automated Missing Index (MOST IMPORTANT)
    # =========================================================================
    def _generate_sql_access_advisor_actions(self, sql_id: str) -> LoadReductionAction:
        """
        Generate SQL Access Advisor actions
        
        Trigger Condition:
        - IO dominant SQL
        - missing index suspected
        
        This is the MOST IMPORTANT section for load reduction.
        """
        queries: List[str] = [
            f"""-- 1️⃣ Create SQL Tuning Task for Index Recommendations
BEGIN
    DBMS_SQLTUNE.CREATE_TUNING_TASK(
        sql_id     => '{sql_id}',
        scope      => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,
        time_limit => 300,
        task_name  => 'IDX_ADVISOR_{sql_id}'
    );
END;
/""",
            
            f"""-- 2️⃣ Execute the Tuning Task
BEGIN
    DBMS_SQLTUNE.EXECUTE_TUNING_TASK(
        task_name => 'IDX_ADVISOR_{sql_id}'
    );
END;
/""",
            
            f"""-- 3️⃣ View Tuning Recommendations
SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK(
    'IDX_ADVISOR_{sql_id}'
) AS recommendations
FROM dual;""",
            
            f"""-- 4️⃣ [Alternative] Use SQL Access Advisor
DECLARE
    l_task_name VARCHAR2(30) := 'ACCESS_ADV_{sql_id}';
    l_workload_name VARCHAR2(30) := 'WL_{sql_id}';
BEGIN
    -- Create advisor task
    DBMS_ADVISOR.CREATE_TASK(
        advisor_name => 'SQL Access Advisor',
        task_name    => l_task_name
    );
    
    -- Add SQL to workload
    DBMS_ADVISOR.ADD_STS_REF(
        task_name    => l_task_name,
        sts_owner    => USER,
        workload_name => l_workload_name
    );
    
    -- Execute
    DBMS_ADVISOR.EXECUTE_TASK(task_name => l_task_name);
END;
/"""
        ]
        
        return LoadReductionAction(
            category=RootCauseCategory.MISSING_INDEX,
            title="🎯 SQL Access Advisor - Index Recommendations (HIGHEST ROI)",
            sql_queries=queries,
            dba_action_text=(
                "Create advisor-recommended indexes. "
                "This is the safest and highest ROI way to reduce IO and database load."
            ),
            why_this_helps=(
                "SQL Access Advisor analyzes the SQL and recommends optimal indexes. "
                "Implementing these recommendations typically provides 60-90% reduction in IO. "
                "This is the most impactful action for IO-dominant queries."
            ),
            priority=1  # Highest priority for missing index
        )
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    def _generate_summary(
        self,
        sql_id: str,
        causes: List[RootCauseCategory],
        io_wait_pct: float,
        cpu_pct: float,
        avg_exec_time: float
    ) -> str:
        """Generate a summary of the load reduction analysis"""
        
        if not causes:
            return f"SQL {sql_id}: No significant load reduction opportunities detected."
        
        cause_names: List[str] = [c.value for c in causes]
        
        summary_parts: List[str] = [
            f"SQL {sql_id} - Load Reduction Analysis",
            f"Root Causes Detected: {', '.join(cause_names)}",
            "",
            "Key Metrics:",
        ]
        
        if RootCauseCategory.IO_DOMINANT in causes or RootCauseCategory.MISSING_INDEX in causes:
            summary_parts.append(f"  • IO Wait: {io_wait_pct:.1f}% (threshold: 60%)")
        
        if RootCauseCategory.HIGH_CPU in causes:
            summary_parts.append(f"  • CPU: {cpu_pct:.1f}% (threshold: 50%)")
        
        if RootCauseCategory.PX_INEFFECTIVE in causes:
            summary_parts.append(f"  • Avg Exec Time: {avg_exec_time:.1f}s (batch pattern)")
        
        summary_parts.extend([
            "",
            "Expected Load Reduction:",
        ])
        
        if RootCauseCategory.IO_DOMINANT in causes or RootCauseCategory.MISSING_INDEX in causes:
            summary_parts.append("  • Indexing: 60-90% IO reduction")
        
        if RootCauseCategory.PX_INEFFECTIVE in causes:
            summary_parts.append("  • Parallel Tuning: 50-70% runtime reduction")
        
        if RootCauseCategory.HIGH_CPU in causes:
            summary_parts.append("  • CPU Optimization: 30-50% CPU reduction")
        
        if RootCauseCategory.BAD_EXECUTION_PLAN in causes:
            summary_parts.append("  • Plan Stability: Prevents unpredictable load spikes")
        
        return "\n".join(summary_parts)
    
    def to_dict(self, result: LoadReductionResult) -> Dict[str, Any]:
        """Convert LoadReductionResult to dictionary for JSON serialization"""
        return {
            "sql_id": result.sql_id,
            "detected_root_causes": [c.value for c in result.detected_root_causes],
            "summary": result.summary,
            "total_actions": result.total_actions,
            "actions": [
                {
                    "category": action.category.value,
                    "title": action.title,
                    "sql_queries": action.sql_queries,
                    "dba_action_text": action.dba_action_text,
                    "why_this_helps": action.why_this_helps,
                    "priority": action.priority
                }
                for action in result.actions
            ]
        }


# =========================================================================
# INTEGRATION HELPER FUNCTION
# =========================================================================
def generate_load_reduction_for_finding(finding: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate load reduction actions for a DBA finding
    
    Args:
        finding: A finding dictionary from DBA Expert Engine
        
    Returns:
        Load reduction result as dictionary
    """
    engine = LoadReductionEngine()
    
    # Extract metrics from finding
    sql_id = finding.get('sql_id', 'UNKNOWN')
    exec_pattern = finding.get('execution_pattern', {})
    
    io_wait_pct = exec_pattern.get('io_pct', 0)
    cpu_pct = exec_pattern.get('cpu_pct', 0)
    avg_exec_time = exec_pattern.get('avg_elapsed_per_exec', 0)
    executions = exec_pattern.get('total_executions', 0)
    total_elapsed = exec_pattern.get('total_elapsed', 0)
    
    # Check for plan instability from interpretation
    interpretation = finding.get('dba_interpretation', '').lower()
    plan_instability: bool = 'plan' in interpretation and ('unstable' in interpretation or 'regression' in interpretation)
    full_table_scan: bool = 'full scan' in interpretation or 'table scan' in interpretation
    
    result: LoadReductionResult = engine.analyze_and_generate_actions(
        sql_id=sql_id,
        io_wait_pct=io_wait_pct,
        cpu_pct=cpu_pct,
        avg_exec_time=avg_exec_time,
        executions=executions,
        total_elapsed=total_elapsed,
        plan_instability=plan_instability,
        full_table_scan_detected=full_table_scan
    )
    
    return engine.to_dict(result)
