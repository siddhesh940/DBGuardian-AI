"""
Decision Engine - DBA Reasoning Layer
=====================================

This module implements the core DBA brain with explicit decision gates.
It determines WHAT actions are allowed/blocked based on normalized signals.

CRITICAL PRINCIPLES:
- Every recommendation must pass through a decision gate
- No action is allowed without clear justification
- Blocked actions must be explicitly documented
- This is NOT a template system - decisions are made dynamically at runtime

DECISION GATES (MANDATORY):
1. BATCH_SQL - Slow per execution, low frequency
2. CHATTY_SQL - Fast but too many executions  
3. IO_BOUND_SQL - High IO wait percentage
4. CPU_BOUND_SQL - High CPU with low IO
5. LOW_PRIORITY - No tuning justified
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum


class SQLCategory(Enum):
    """SQL workload categories determined by decision gates"""
    BATCH_SQL = "BATCH_SQL"
    CHATTY_SQL = "CHATTY_SQL"
    IO_BOUND_SQL = "IO_BOUND_SQL"
    CPU_BOUND_SQL = "CPU_BOUND_SQL"
    MIXED_PROFILE_SQL = "MIXED_PROFILE_SQL"
    LOW_PRIORITY = "LOW_PRIORITY"


class ActionType(Enum):
    """Allowed/Blocked action types"""
    # Analysis actions
    PLAN_ANALYSIS = "PLAN_ANALYSIS"
    INDEX_REVIEW = "INDEX_REVIEW"
    IO_OPTIMIZATION = "IO_OPTIMIZATION"
    ACCESS_PATH_OPTIMIZATION = "ACCESS_PATH_OPTIMIZATION"
    JOIN_METHOD_REVIEW = "JOIN_METHOD_REVIEW"
    HASH_VS_NESTED_ANALYSIS = "HASH_VS_NESTED_ANALYSIS"
    SQL_REWRITE = "SQL_REWRITE"
    
    # Tuning actions
    BIND_TUNING = "BIND_TUNING"
    SQL_TUNING_ADVISOR = "SQL_TUNING_ADVISOR"
    SQL_ACCESS_ADVISOR = "SQL_ACCESS_ADVISOR"
    
    # Application-level actions
    APPLICATION_THROTTLING = "APPLICATION_THROTTLING"
    RESULT_CACHING = "RESULT_CACHING"
    
    # Actions that should be blocked in certain scenarios
    INDEX_CREATION = "INDEX_CREATION"
    CPU_TUNING = "CPU_TUNING"
    JOIN_HINTS = "JOIN_HINTS"
    INDEX_ONLY_FIXES = "INDEX_ONLY_FIXES"
    APP_THROTTLING = "APP_THROTTLING"
    
    # Monitoring only
    MONITOR_ONLY = "MONITOR_ONLY"


@dataclass
class NormalizedSignals:
    """
    Normalized signal block per SQL - the ONLY input to decision logic.
    This exact shape is required per the specification.
    """
    sql_id: str
    executions: int
    total_elapsed: float
    avg_exec_time: float
    cpu_time: float
    cpu_pct: float
    io_wait_pct: float
    db_time_pct: float
    
    # Additional context fields (optional)
    sql_text: Optional[str] = None
    sql_module: Optional[str] = None
    wait_class: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "sql_id": self.sql_id,
            "executions": self.executions,
            "total_elapsed": round(self.total_elapsed, 2),
            "avg_exec_time": round(self.avg_exec_time, 4),
            "cpu_time": round(self.cpu_time, 2),
            "cpu_pct": round(self.cpu_pct, 1),
            "io_wait_pct": round(self.io_wait_pct, 1),
            "db_time_pct": round(self.db_time_pct, 1)
        }


@dataclass
class DecisionResult:
    """
    Result of a decision gate evaluation.
    Contains category, allowed actions, blocked actions, and reasoning.
    """
    sql_id: str
    category: SQLCategory
    allowed_actions: List[ActionType]
    blocked_actions: List[ActionType]
    reasoning: List[str]
    signals: NormalizedSignals
    
    # Explainability fields (MANDATORY per spec)
    why_shown: List[str] = field(default_factory=list)
    why_hidden: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "sql_id": self.sql_id,
            "category": self.category.value,
            "allowed_actions": [a.value for a in self.allowed_actions],
            "blocked_actions": [a.value for a in self.blocked_actions],
            "reasoning": self.reasoning,
            "signals": self.signals.to_dict(),
            "why_shown": self.why_shown,
            "why_hidden": self.why_hidden
        }


class SignalNormalizer:
    """
    Extracts and normalizes signals from RCA output.
    Transforms raw AWR/ASH data into the standardized signal block.
    """
    
    @staticmethod
    def normalize_from_rca(sql_data: Dict[str, Any], 
                           wait_events: List[Dict] = None,
                           ash_analysis: Dict = None) -> NormalizedSignals:
        """
        Normalize signals from RCA engine output.
        This transforms various input formats into the standardized signal block.
        """
        # Extract core metrics with safe defaults
        sql_id = sql_data.get('sql_id', 'UNKNOWN')
        executions = int(sql_data.get('executions', 0) or 0)
        
        # Total elapsed time
        total_elapsed = float(
            sql_data.get('elapsed', 0) or 
            sql_data.get('elapsed_time', 0) or 
            sql_data.get('total_elapsed', 0) or 0
        )
        
        # CPU time
        cpu_time = float(
            sql_data.get('cpu', 0) or 
            sql_data.get('cpu_time', 0) or 0
        )
        
        # Calculate average execution time
        if executions > 0 and total_elapsed > 0:
            avg_exec_time: float = total_elapsed / executions
        else:
            avg_exec_time = float(sql_data.get('elapsed_per_exec', 0) or 0)
        
        # CPU percentage - from data or calculated
        cpu_pct = float(sql_data.get('pctcpu', 0) or 0)
        if cpu_pct == 0 and total_elapsed > 0 and cpu_time > 0:
            cpu_pct: float = (cpu_time / total_elapsed) * 100
        
        # IO wait percentage
        io_wait_pct = float(sql_data.get('pctio', 0) or 0)
        
        # If IO wait not available, calculate from elapsed vs CPU
        if io_wait_pct == 0 and total_elapsed > 0 and cpu_time >= 0:
            # IO wait is roughly (elapsed - cpu) / elapsed
            non_cpu_time: float = max(0, total_elapsed - cpu_time)
            io_wait_pct: float | int = (non_cpu_time / total_elapsed) * 100 if total_elapsed > 0 else 0
        
        # DB time percentage (% of total workload)
        db_time_pct = float(sql_data.get('pcttotal', 0) or sql_data.get('db_time_pct', 0) or 0)
        
        # Optional context
        sql_text: Any | None = sql_data.get('sql_text', None)
        sql_module: Any | None = sql_data.get('sql_module', None)
        wait_class: Any | None = sql_data.get('wait_class', None)
        
        # Enrich from wait events if available
        if wait_events and not wait_class:
            for we in wait_events:
                if we.get('pct_of_db_time', 0) > 20:
                    wait_class = we.get('wait_class', None)
                    break
        
        return NormalizedSignals(
            sql_id=sql_id,
            executions=executions,
            total_elapsed=total_elapsed,
            avg_exec_time=avg_exec_time,
            cpu_time=cpu_time,
            cpu_pct=cpu_pct,
            io_wait_pct=io_wait_pct,
            db_time_pct=db_time_pct,
            sql_text=sql_text,
            sql_module=sql_module,
            wait_class=wait_class
        )


class DecisionEngine:
    """
    Core DBA Brain - Decision Gates Implementation
    
    This engine implements explicit DBA-style decision gates that determine
    what actions are allowed/blocked for each SQL based on its signals.
    
    CRITICAL: No action is allowed without passing through a gate.
    """
    
    # Gate thresholds (configurable but with sensible defaults)
    BATCH_SQL_MIN_AVG_EXEC_TIME = 5.0  # seconds
    BATCH_SQL_MAX_EXECUTIONS = 50
    
    CHATTY_SQL_MIN_EXECUTIONS = 1000
    CHATTY_SQL_MAX_AVG_EXEC_TIME = 0.1  # seconds
    
    IO_BOUND_MIN_IO_WAIT_PCT = 70.0
    
    CPU_BOUND_MIN_CPU_PCT = 70.0
    CPU_BOUND_MAX_IO_WAIT_PCT = 30.0
    
    def __init__(self) -> None:
        """Initialize the decision engine"""
        self.normalizer = SignalNormalizer()
    
    def evaluate(self, signals: NormalizedSignals) -> DecisionResult:
        """
        Main entry point - evaluate a SQL's signals through decision gates.
        Returns a DecisionResult with category, allowed/blocked actions, and reasoning.
        """
        # Try each gate in priority order
        # Gate 1: Batch/Report SQL
        if self._is_batch_sql(signals):
            return self._create_batch_sql_decision(signals)
        
        # Gate 2: Chatty/OLTP SQL
        if self._is_chatty_sql(signals):
            return self._create_chatty_sql_decision(signals)
        
        # Gate 3: IO-bound SQL
        if self._is_io_bound_sql(signals):
            return self._create_io_bound_decision(signals)
        
        # Gate 4: CPU-bound SQL
        if self._is_cpu_bound_sql(signals):
            return self._create_cpu_bound_decision(signals)
        
        # Check for mixed profile (multiple characteristics)
        if self._is_mixed_profile_sql(signals):
            return self._create_mixed_profile_decision(signals)
        
        # Default: Low Priority
        return self._create_low_priority_decision(signals)
    
    def evaluate_from_rca(self, sql_data: Dict[str, Any],
                          wait_events: List[Dict] = None,
                          ash_analysis: Dict = None) -> DecisionResult:
        """
        Convenience method to evaluate directly from RCA output.
        Normalizes signals first, then evaluates.
        """
        signals: NormalizedSignals = self.normalizer.normalize_from_rca(sql_data, wait_events, ash_analysis)
        return self.evaluate(signals)
    
    # =========================================================================
    # GATE EVALUATION METHODS
    # =========================================================================
    
    def _is_batch_sql(self, signals: NormalizedSignals) -> bool:
        """
        Gate 1: Batch/Report SQL Detection
        IF avg_exec_time > 5s AND executions < 50
        """
        return (signals.avg_exec_time > self.BATCH_SQL_MIN_AVG_EXEC_TIME and 
                signals.executions < self.BATCH_SQL_MAX_EXECUTIONS)
    
    def _is_chatty_sql(self, signals: NormalizedSignals) -> bool:
        """
        Gate 2: Chatty/OLTP SQL Detection
        IF executions > 1000 AND avg_exec_time < 0.1s
        """
        return (signals.executions > self.CHATTY_SQL_MIN_EXECUTIONS and 
                signals.avg_exec_time < self.CHATTY_SQL_MAX_AVG_EXEC_TIME)
    
    def _is_io_bound_sql(self, signals: NormalizedSignals) -> bool:
        """
        Gate 3: IO-bound SQL Detection
        IF io_wait_pct > 70
        """
        return signals.io_wait_pct > self.IO_BOUND_MIN_IO_WAIT_PCT
    
    def _is_cpu_bound_sql(self, signals: NormalizedSignals) -> bool:
        """
        Gate 4: CPU-bound SQL Detection
        IF cpu_pct > 70 AND io_wait_pct < 30
        """
        return (signals.cpu_pct > self.CPU_BOUND_MIN_CPU_PCT and 
                signals.io_wait_pct < self.CPU_BOUND_MAX_IO_WAIT_PCT)
    
    def _is_mixed_profile_sql(self, signals: NormalizedSignals) -> bool:
        """
        Mixed Profile: SQL that shows multiple concerning characteristics
        but doesn't clearly fit one category
        """
        concerning_traits = 0
        
        if signals.avg_exec_time > 1.0:  # More than 1s per exec
            concerning_traits += 1
        if signals.executions > 100:  # Moderate frequency
            concerning_traits += 1
        if signals.io_wait_pct > 40:  # Some IO wait
            concerning_traits += 1
        if signals.cpu_pct > 40:  # Some CPU usage
            concerning_traits += 1
        if signals.db_time_pct > 10:  # Significant DB time
            concerning_traits += 1
        
        return concerning_traits >= 3
    
    # =========================================================================
    # DECISION CREATION METHODS
    # =========================================================================
    
    def _create_batch_sql_decision(self, signals: NormalizedSignals) -> DecisionResult:
        """
        Create decision for BATCH_SQL category
        Slow per execution, low frequency → batch/report SQL
        """
        allowed: List[ActionType] = [
            ActionType.PLAN_ANALYSIS,
            ActionType.INDEX_REVIEW,
            ActionType.IO_OPTIMIZATION,
            ActionType.SQL_ACCESS_ADVISOR,
            ActionType.SQL_REWRITE
        ]
        
        blocked: List[ActionType] = [
            ActionType.BIND_TUNING,
            ActionType.APP_THROTTLING,
            ActionType.APPLICATION_THROTTLING,
            ActionType.RESULT_CACHING  # No benefit for low frequency
        ]
        
        reasoning: List[str] = [
            f"Slow per execution ({signals.avg_exec_time:.2f}s > 5s threshold)",
            f"Low frequency ({signals.executions} executions < 50 threshold)",
            "Pattern indicates batch/report SQL workload",
            "Focus on query efficiency, not application throttling"
        ]
        
        why_shown: List[str] = [
            f"avg_exec_time = {signals.avg_exec_time:.2f}s (>5s)",
            f"executions = {signals.executions} (<50)",
            f"total_elapsed = {signals.total_elapsed:.1f}s"
        ]
        
        if signals.io_wait_pct > 30:
            why_shown.append(f"io_wait_pct = {signals.io_wait_pct:.1f}%")
        
        why_hidden: List[str] = [
            "Bind tuning skipped: low execution frequency makes cursor sharing irrelevant",
            "Application throttling skipped: not applicable for batch/report SQL",
            "Result caching skipped: low frequency means minimal cache hit benefit"
        ]
        
        return DecisionResult(
            sql_id=signals.sql_id,
            category=SQLCategory.BATCH_SQL,
            allowed_actions=allowed,
            blocked_actions=blocked,
            reasoning=reasoning,
            signals=signals,
            why_shown=why_shown,
            why_hidden=why_hidden
        )
    
    def _create_chatty_sql_decision(self, signals: NormalizedSignals) -> DecisionResult:
        """
        Create decision for CHATTY_SQL category
        Fast SQL executed too frequently → application design issue
        """
        allowed: List[ActionType] = [
            ActionType.APPLICATION_THROTTLING,
            ActionType.RESULT_CACHING,
            ActionType.BIND_TUNING  # Cursor sharing is critical for chatty SQL
        ]
        
        blocked: List[ActionType] = [
            ActionType.INDEX_CREATION,
            ActionType.SQL_TUNING_ADVISOR,
            ActionType.SQL_ACCESS_ADVISOR,
            ActionType.PLAN_ANALYSIS,  # Individual query is already fast
            ActionType.SQL_REWRITE  # Query doesn't need rewriting
        ]
        
        reasoning: List[str] = [
            f"Fast per execution ({signals.avg_exec_time:.4f}s < 0.1s)",
            f"Extremely high frequency ({signals.executions:,} executions > 1000)",
            "Pattern indicates OLTP/chatty SQL - application design issue",
            "Individual query is efficient but cumulative overhead is the problem"
        ]
        
        why_shown: List[str] = [
            f"executions = {signals.executions:,} (>1000)",
            f"avg_exec_time = {signals.avg_exec_time:.4f}s (<0.1s)",
            "Cumulative impact despite fast individual execution"
        ]
        
        why_hidden: List[str] = [
            "Index creation skipped: query already executes fast enough",
            "SQL Tuning Advisor skipped: query is already efficient",
            "SQL Access Advisor skipped: no structural changes needed",
            "Plan analysis skipped: execution plan is not the bottleneck"
        ]
        
        return DecisionResult(
            sql_id=signals.sql_id,
            category=SQLCategory.CHATTY_SQL,
            allowed_actions=allowed,
            blocked_actions=blocked,
            reasoning=reasoning,
            signals=signals,
            why_shown=why_shown,
            why_hidden=why_hidden
        )
    
    def _create_io_bound_decision(self, signals: NormalizedSignals) -> DecisionResult:
        """
        Create decision for IO_BOUND_SQL category
        High IO wait → inefficient data access
        """
        allowed: List[ActionType] = [
            ActionType.INDEX_REVIEW,
            ActionType.INDEX_CREATION,
            ActionType.ACCESS_PATH_OPTIMIZATION,
            ActionType.SQL_ACCESS_ADVISOR,
            ActionType.IO_OPTIMIZATION
        ]
        
        blocked: List[ActionType] = [
            ActionType.CPU_TUNING,
            ActionType.JOIN_HINTS,  # Join method unlikely to help IO issues
            ActionType.HASH_VS_NESTED_ANALYSIS  # CPU-focused analysis
        ]
        
        reasoning: List[str] = [
            f"High IO wait ({signals.io_wait_pct:.1f}% > 70% threshold)",
            "Query spending most time waiting for data retrieval",
            "Focus on reducing physical I/O through better access paths",
            "Index optimization likely to provide significant improvement"
        ]
        
        why_shown: List[str] = [
            f"io_wait_pct = {signals.io_wait_pct:.1f}% (>70%)",
            f"total_elapsed = {signals.total_elapsed:.1f}s",
            f"cpu_pct = {signals.cpu_pct:.1f}% (low - confirms IO bottleneck)"
        ]
        
        why_hidden: List[str] = [
            "CPU tuning skipped: CPU is not the bottleneck",
            "Join hints skipped: join method changes unlikely to reduce IO",
            "Hash vs Nested analysis skipped: IO access path is the issue, not join method"
        ]
        
        return DecisionResult(
            sql_id=signals.sql_id,
            category=SQLCategory.IO_BOUND_SQL,
            allowed_actions=allowed,
            blocked_actions=blocked,
            reasoning=reasoning,
            signals=signals,
            why_shown=why_shown,
            why_hidden=why_hidden
        )
    
    def _create_cpu_bound_decision(self, signals: NormalizedSignals) -> DecisionResult:
        """
        Create decision for CPU_BOUND_SQL category
        High CPU with low IO → inefficient joins or computation
        """
        allowed: List[ActionType] = [
            ActionType.JOIN_METHOD_REVIEW,
            ActionType.HASH_VS_NESTED_ANALYSIS,
            ActionType.SQL_REWRITE,
            ActionType.PLAN_ANALYSIS,
            ActionType.SQL_TUNING_ADVISOR
        ]
        
        blocked: List[ActionType] = [
            ActionType.INDEX_ONLY_FIXES,
            ActionType.IO_OPTIMIZATION,  # IO is not the problem
            ActionType.ACCESS_PATH_OPTIMIZATION  # Data access is efficient
        ]
        
        reasoning: List[str] = [
            f"High CPU consumption ({signals.cpu_pct:.1f}% > 70% threshold)",
            f"Low IO wait ({signals.io_wait_pct:.1f}% < 30% threshold)",
            "Query retrieving data efficiently but processing inefficiently",
            "Focus on join methods, aggregations, and computational logic"
        ]
        
        why_shown: List[str] = [
            f"cpu_pct = {signals.cpu_pct:.1f}% (>70%)",
            f"io_wait_pct = {signals.io_wait_pct:.1f}% (<30%)",
            f"cpu_time = {signals.cpu_time:.1f}s"
        ]
        
        why_hidden: List[str] = [
            "Index-only fixes skipped: data access is already efficient",
            "IO optimization skipped: IO is not the bottleneck",
            "Access path optimization skipped: physical reads are not the issue"
        ]
        
        return DecisionResult(
            sql_id=signals.sql_id,
            category=SQLCategory.CPU_BOUND_SQL,
            allowed_actions=allowed,
            blocked_actions=blocked,
            reasoning=reasoning,
            signals=signals,
            why_shown=why_shown,
            why_hidden=why_hidden
        )
    
    def _create_mixed_profile_decision(self, signals: NormalizedSignals) -> DecisionResult:
        """
        Create decision for SQL with mixed characteristics.
        Allow broader investigation but prioritize based on dominant trait.
        """
        allowed: List[ActionType] = [
            ActionType.PLAN_ANALYSIS,
            ActionType.SQL_TUNING_ADVISOR
        ]
        
        # Add specific actions based on which traits are present
        if signals.io_wait_pct > 40:
            allowed.extend([ActionType.INDEX_REVIEW, ActionType.ACCESS_PATH_OPTIMIZATION])
        if signals.cpu_pct > 40:
            allowed.extend([ActionType.JOIN_METHOD_REVIEW, ActionType.SQL_REWRITE])
        if signals.executions > 500:
            allowed.extend([ActionType.BIND_TUNING, ActionType.RESULT_CACHING])
        
        blocked = []
        
        reasoning: List[str] = [
            "SQL shows multiple concerning characteristics",
            f"Moderate execution time ({signals.avg_exec_time:.2f}s/exec)",
            f"Mixed IO ({signals.io_wait_pct:.1f}%) and CPU ({signals.cpu_pct:.1f}%) profile",
            "Comprehensive analysis recommended"
        ]
        
        why_shown: List[str] = [
            f"avg_exec_time = {signals.avg_exec_time:.2f}s",
            f"executions = {signals.executions}",
            f"io_wait_pct = {signals.io_wait_pct:.1f}%",
            f"cpu_pct = {signals.cpu_pct:.1f}%",
            f"db_time_pct = {signals.db_time_pct:.1f}%"
        ]
        
        why_hidden: List[str] = [
            "No actions explicitly blocked for mixed profile SQL",
            "Comprehensive investigation needed to identify root cause"
        ]
        
        return DecisionResult(
            sql_id=signals.sql_id,
            category=SQLCategory.MIXED_PROFILE_SQL,
            allowed_actions=list(set(allowed)),  # Remove duplicates
            blocked_actions=blocked,
            reasoning=reasoning,
            signals=signals,
            why_shown=why_shown,
            why_hidden=why_hidden
        )
    
    def _create_low_priority_decision(self, signals: NormalizedSignals) -> DecisionResult:
        """
        Create decision for LOW_PRIORITY category
        No tuning justified by workload behavior
        """
        allowed: List[ActionType] = [
            ActionType.MONITOR_ONLY
        ]
        
        blocked: List[ActionType] = [
            ActionType.INDEX_CREATION,
            ActionType.SQL_TUNING_ADVISOR,
            ActionType.SQL_ACCESS_ADVISOR,
            ActionType.SQL_REWRITE,
            ActionType.PLAN_ANALYSIS,
            ActionType.APPLICATION_THROTTLING
        ]
        
        reasoning: List[str] = [
            "No tuning justified by current workload behavior",
            f"Average execution time ({signals.avg_exec_time:.3f}s) is acceptable",
            f"Execution frequency ({signals.executions}) is not concerning",
            "SQL does not meet any problem criteria - continue monitoring"
        ]
        
        why_shown: List[str] = [
            f"avg_exec_time = {signals.avg_exec_time:.3f}s (acceptable)",
            f"executions = {signals.executions} (not excessive)",
            f"io_wait_pct = {signals.io_wait_pct:.1f}% (within range)",
            f"cpu_pct = {signals.cpu_pct:.1f}% (within range)"
        ]
        
        why_hidden: List[str] = [
            "All tuning actions skipped: workload characteristics do not justify intervention",
            "SQL Tuning Advisor skipped: no performance problem detected",
            "Index creation skipped: access patterns are efficient",
            "Query rewrite skipped: query structure is acceptable"
        ]
        
        return DecisionResult(
            sql_id=signals.sql_id,
            category=SQLCategory.LOW_PRIORITY,
            allowed_actions=allowed,
            blocked_actions=blocked,
            reasoning=reasoning,
            signals=signals,
            why_shown=why_shown,
            why_hidden=why_hidden
        )
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def is_action_allowed(self, decision: DecisionResult, action: ActionType) -> bool:
        """Check if a specific action is allowed for the given decision"""
        return action in decision.allowed_actions
    
    def is_action_blocked(self, decision: DecisionResult, action: ActionType) -> bool:
        """Check if a specific action is blocked for the given decision"""
        return action in decision.blocked_actions
    
    def get_allowed_actions_for_sql(self, sql_data: Dict[str, Any],
                                     wait_events: List[Dict] = None,
                                     ash_analysis: Dict = None) -> List[str]:
        """
        Convenience method to get allowed action names directly from SQL data.
        Returns list of action name strings.
        """
        decision: DecisionResult = self.evaluate_from_rca(sql_data, wait_events, ash_analysis)
        return [action.value for action in decision.allowed_actions]
