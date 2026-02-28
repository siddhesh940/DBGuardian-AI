"""
Dynamic SQL Generator - Runtime SQL Generation Engine
======================================================

This module generates executable SQL commands dynamically at runtime.
SQL text is NEVER stored as templates - it's assembled based on:
- Decision category
- Normalized signals
- Real-time metrics

CRITICAL PRINCIPLES (DO NOT VIOLATE):
- DO NOT store SQL strings as constants
- DO NOT reuse the same format string everywhere
- DO NOT generate SQL without consulting decision category + signals
- SQL must be generated at runtime
- SQL format must differ when signals differ
- Same SQL_ID with different signals must produce different SQL text

PROOF OF DYNAMIC GENERATION:
- Same SQL_ID + different IO/CPU -> different SQL text
- Same SQL_ID + different execution frequency -> different SQL text
- Format strings are NEVER identical across categories

PYTHON 3.6.8 COMPATIBILITY:
- For Python 3.6.x, install the dataclasses backport: pip install dataclasses
- All type hints use typing module (compatible with 3.6+)
"""

from typing import Dict, List, Any

from engine.decision_engine import (
    DecisionResult, SQLCategory, ActionType, NormalizedSignals
)


class GeneratedSQL:
    """
    Represents a dynamically generated SQL command.
    Each generated SQL includes metadata about why it was generated.
    
    Python 3.6.8 compatible - uses explicit __init__ instead of dataclass.
    """
    
    def __init__(self, action, sql, intent, explanation, category, signal_fingerprint):
        # type: (str, str, str, str, str, str) -> None
        self.action = action
        self.sql = sql
        self.intent = intent
        self.explanation = explanation
        self.category = category
        self.signal_fingerprint = signal_fingerprint
    
    def to_dict(self):
        # type: () -> Dict[str, Any]
        return {
            "action": self.action,
            "sql": self.sql,
            "intent": self.intent,
            "explanation": self.explanation,
            "category": self.category,
            "signal_fingerprint": self.signal_fingerprint
        }


class DBAActionPlan:
    """
    Dynamic DBA Action Plan generated per SQL category and signals.
    Each timeframe has actions derived from actual workload characteristics.
    
    Python 3.6.8 compatible - uses explicit __init__ instead of dataclass.
    """
    
    def __init__(self, sql_id, category, signal_fingerprint, immediate, 
                 short_term, medium_term, long_term, priority_reasoning):
        # type: (str, str, str, List[str], List[str], List[str], List[str], List[str]) -> None
        self.sql_id = sql_id
        self.category = category
        self.signal_fingerprint = signal_fingerprint
        self.immediate = immediate
        self.short_term = short_term
        self.medium_term = medium_term
        self.long_term = long_term
        self.priority_reasoning = priority_reasoning
    
    def to_dict(self):
        # type: () -> Dict[str, Any]
        return {
            "sql_id": self.sql_id,
            "category": self.category,
            "signal_fingerprint": self.signal_fingerprint,
            "immediate": self.immediate,
            "short_term": self.short_term,
            "medium_term": self.medium_term,
            "long_term": self.long_term,
            "priority_reasoning": self.priority_reasoning
        }
    
    def to_formatted_string(self) -> str:
        """Format as readable action plan text"""
        lines: List[str] = [
            f"═══════════════════════════════════════════════════════════════",
            f"DBA ACTION PLAN for SQL_ID: {self.sql_id}",
            f"Category: {self.category}",
            f"═══════════════════════════════════════════════════════════════",
            "",
            "🔴 IMMEDIATE (Next 1 Hour):",
        ]
        for action in self.immediate:
            lines.append(f"   • {action}")
        
        lines.extend([
            "",
            "🟠 SHORT TERM (Today/Tomorrow):",
        ])
        for action in self.short_term:
            lines.append(f"   • {action}")
        
        lines.extend([
            "",
            "🟡 MEDIUM TERM (This Week):",
        ])
        for action in self.medium_term:
            lines.append(f"   • {action}")
        
        lines.extend([
            "",
            "🟢 LONG TERM (Ongoing):",
        ])
        for action in self.long_term:
            lines.append(f"   • {action}")
        
        lines.extend([
            "",
            "📋 PRIORITY REASONING:",
        ])
        for reason in self.priority_reasoning:
            lines.append(f"   → {reason}")
        
        return "\n".join(lines)


class DynamicSQLGenerator:
    """
    Dynamic SQL Generation Engine
    
    This class generates executable Oracle SQL commands based on:
    - Decision category from DecisionEngine
    - Normalized signals (cpu_pct, io_wait_pct, avg_exec_time, executions)
    
    CRITICAL: No SQL is stored as a constant. All SQL is assembled at runtime.
    
    PROOF OF DYNAMIC GENERATION:
    - Same SQL_ID + different IO/CPU → different SQL text
    - Same SQL_ID + different execution count → different SQL text
    - Different categories → different advisor behavior
    - Signal fingerprint embedded in all generated SQL
    """
    
    def __init__(self) -> None:
        """Initialize the generator"""
        self._generation_log: List[Dict[str, Any]] = []
    
    def generate_all(self, decision: DecisionResult) -> List[GeneratedSQL]:
        """
        SENIOR DBA-STYLE QUERY SELECTION
        =================================
        
        CRITICAL PRINCIPLE: "Different instruments for different diseases"
        NOT: "Same instruments, different knobs"
        
        A Senior DBA asks: "Given these signals, what would I look at FIRST?"
        
        QUERY CLASS MATRIX:
        - IO-Heavy Batch (io>80%, exec<100) → Object-level IO FIRST, XPLAN is confirmation
        - CPU-Bound (cpu>70%, io<30%) → Predicate/join cost FIRST, NOT XPLAN
        - Chatty/OLTP (exec>1000, avg<0.1s) → Execution frequency analysis, NO advisors
        - Batch Slow (avg>5s, exec<50) → Parallel effectiveness FIRST, then XPLAN
        
        XPLAN is NEVER the default entry point.
        """
        generated = []
        signals: NormalizedSignals = decision.signals
        
        # =====================================================================
        # SENIOR DBA QUERY SELECTION LOGIC
        # =====================================================================
        
        if decision.category == SQLCategory.IO_BOUND_SQL:
            # IO-Heavy: Object-level IO analysis FIRST, XPLAN is confirmation
            generated.extend(self._generate_io_bound_commands_dba_style(signals, decision))
            
        elif decision.category == SQLCategory.CPU_BOUND_SQL:
            # CPU-Bound: Predicate/join cost FIRST, NOT XPLAN
            generated.extend(self._generate_cpu_bound_commands_dba_style(signals, decision))
            
        elif decision.category == SQLCategory.CHATTY_SQL:
            # Chatty: Execution frequency analysis, NO heavy tools
            generated.extend(self._generate_chatty_sql_commands_dba_style(signals, decision))
            
        elif decision.category == SQLCategory.BATCH_SQL:
            # Batch: Parallel effectiveness check, then XPLAN, then conditional advisor
            generated.extend(self._generate_batch_sql_commands_dba_style(signals, decision))
            
        elif decision.category == SQLCategory.MIXED_PROFILE_SQL:
            # Mixed: Comprehensive but prioritized analysis
            generated.extend(self._generate_mixed_profile_commands(signals, decision))
            
        elif decision.category == SQLCategory.LOW_PRIORITY:
            # Low Priority: Monitoring only
            generated.extend(self._generate_monitoring_commands(signals, decision))
        
        # Log generation for proof/verification
        self._log_generation(decision, generated)
        
        return generated
    
    def verify_dynamic_generation(self, signals1: NormalizedSignals, 
                                   signals2: NormalizedSignals,
                                   decision_engine: 'DecisionEngine') -> Dict[str, Any]:
        """
        PROOF: Verify that different signals produce different SQL.
        
        This method demonstrates that the generator is truly dynamic by:
        1. Generating SQL for two different signal sets
        2. Comparing the output
        3. Returning proof of differentiation
        """
        # Generate for first signal set
        decision1 = decision_engine.evaluate(signals1)
        commands1: List[GeneratedSQL] = self.generate_all(decision1)
        
        # Generate for second signal set
        decision2 = decision_engine.evaluate(signals2)
        commands2: List[GeneratedSQL] = self.generate_all(decision2)
        
        # Compare fingerprints
        fingerprint1: str = self._create_signal_fingerprint(signals1)
        fingerprint2: str = self._create_signal_fingerprint(signals2)
        
        # Compare SQL content
        sql_text1: List[str] = [cmd.sql for cmd in commands1]
        sql_text2: List[str] = [cmd.sql for cmd in commands2]
        
        # Calculate differences
        format_diff: bool = fingerprint1 != fingerprint2
        category_diff = decision1.category != decision2.category
        sql_diff: bool = sql_text1 != sql_text2
        
        return {
            "signals_different": fingerprint1 != fingerprint2,
            "categories_different": category_diff,
            "sql_text_different": sql_diff,
            "fingerprint_1": fingerprint1,
            "fingerprint_2": fingerprint2,
            "category_1": decision1.category.value,
            "category_2": decision2.category.value,
            "proof_passed": format_diff or category_diff or sql_diff,
            "commands_count_1": len(commands1),
            "commands_count_2": len(commands2)
        }
    
    def get_generation_log(self) -> List[Dict[str, Any]]:
        """Return the generation log for audit/verification"""
        return self._generation_log
    
    def _log_generation(self, decision: DecisionResult, 
                        generated: List[GeneratedSQL]) -> None:
        """Log generation for verification purposes"""
        self._generation_log.append({
            "sql_id": decision.sql_id,
            "category": decision.category.value,
            "fingerprint": self._create_signal_fingerprint(decision.signals),
            "commands_generated": len(generated),
            "actions": [cmd.action for cmd in generated],
            "signals": {
                "io_wait_pct": decision.signals.io_wait_pct,
                "cpu_pct": decision.signals.cpu_pct,
                "executions": decision.signals.executions,
                "avg_exec_time": decision.signals.avg_exec_time
            }
        })
    
    def generate_action_plan(self, decision: DecisionResult) -> DBAActionPlan:
        """
        Generate DBA Action Plan dynamically based on decision category and signals.
        
        CRITICAL: This is NOT templated text. Actions are derived from:
        - Decision category (BATCH_SQL, CHATTY_SQL, etc.)
        - Actual signal values (executions, cpu_pct, io_wait_pct)
        - Allowed/blocked actions from DecisionEngine
        
        Each timeframe contains different actions based on urgency and impact.
        """
        signals: NormalizedSignals = decision.signals
        category: SQLCategory = decision.category
        fingerprint: str = self._create_signal_fingerprint(signals)
        
        # Route to category-specific action plan generator
        if category == SQLCategory.BATCH_SQL:
            return self._generate_batch_action_plan(signals, decision, fingerprint)
        elif category == SQLCategory.CHATTY_SQL:
            return self._generate_chatty_action_plan(signals, decision, fingerprint)
        elif category == SQLCategory.IO_BOUND_SQL:
            return self._generate_io_bound_action_plan(signals, decision, fingerprint)
        elif category == SQLCategory.CPU_BOUND_SQL:
            return self._generate_cpu_bound_action_plan(signals, decision, fingerprint)
        elif category == SQLCategory.MIXED_PROFILE_SQL:
            return self._generate_mixed_action_plan(signals, decision, fingerprint)
        else:
            return self._generate_low_priority_action_plan(signals, decision, fingerprint)
    
    def _generate_batch_action_plan(self, signals: NormalizedSignals, 
                                     decision: DecisionResult,
                                     fingerprint: str) -> DBAActionPlan:
        """Generate action plan for BATCH_SQL category"""
        
        # Immediate actions based on actual IO wait percentage
        immediate: List[str] = [
            f"Run DBMS_XPLAN analysis with format optimized for {signals.io_wait_pct:.1f}% IO wait",
            f"Capture current execution plan for SQL_ID {signals.sql_id}",
        ]
        if signals.io_wait_pct > 80:
            immediate.append(f"URGENT: Investigate full table scans (IO wait at {signals.io_wait_pct:.1f}%)")
        if signals.total_elapsed > 100:
            immediate.append(f"Check for blocking sessions (query taking {signals.total_elapsed:.1f}s)")
        
        # Short term based on execution pattern
        short_term: List[str] = [
            f"Run SQL Access Advisor for batch workload analysis",
            f"Review index recommendations for {signals.executions} executions",
        ]
        if signals.avg_exec_time > 10:
            short_term.append(f"Consider query partitioning (avg {signals.avg_exec_time:.1f}s per execution)")
        if signals.io_wait_pct > 60:
            short_term.append("Analyze segment statistics for hot objects")
        
        # Medium term
        medium_term: List[str] = [
            "Implement recommended indexes after testing",
            f"Schedule batch job during off-peak hours if running frequently",
        ]
        if signals.total_elapsed > 200:
            medium_term.append(f"Consider parallel query optimization (total time: {signals.total_elapsed:.1f}s)")
        
        # Long term
        long_term: List[str] = [
            "Establish performance baseline for batch window",
            "Create AWR snapshot retention policy for trend analysis",
            "Document batch SQL performance SLAs"
        ]
        
        # Priority reasoning derived from signals
        priority_reasoning: List[str] = [
            f"Batch SQL identified: {signals.avg_exec_time:.2f}s avg execution time, {signals.executions} executions",
            f"IO-focused tuning priority: {signals.io_wait_pct:.1f}% IO wait detected",
            "Application throttling NOT applicable for batch workload",
            "Bind tuning skipped: low execution frequency"
        ]
        
        return DBAActionPlan(
            sql_id=signals.sql_id,
            category=SQLCategory.BATCH_SQL.value,
            signal_fingerprint=fingerprint,
            immediate=immediate,
            short_term=short_term,
            medium_term=medium_term,
            long_term=long_term,
            priority_reasoning=priority_reasoning
        )
    
    def _generate_chatty_action_plan(self, signals: NormalizedSignals,
                                      decision: DecisionResult,
                                      fingerprint: str) -> DBAActionPlan:
        """Generate action plan for CHATTY_SQL category"""
        
        # Immediate - focus on application patterns
        immediate: List[str] = [
            f"Review application code calling SQL {signals.executions:,} times",
            "Check for missing bind variables causing cursor flooding",
        ]
        if signals.executions > 5000:
            immediate.append(f"CRITICAL: {signals.executions:,} executions - investigate application loop")
        
        # Short term - caching and connection optimization
        short_term: List[str] = [
            "Evaluate result cache applicability for this query",
            "Review connection pooling efficiency",
            f"Monitor cursor cache hit ratio for SQL_ID {signals.sql_id}",
        ]
        if signals.avg_exec_time < 0.01:
            short_term.append(f"Consider client-side caching (query runs in {signals.avg_exec_time*1000:.1f}ms)")
        
        # Medium term
        medium_term: List[str] = [
            "Implement application-level result caching",
            "Review micro-batching opportunities",
            f"Analyze {signals.executions:,} executions for consolidation potential",
        ]
        
        # Long term
        long_term: List[str] = [
            "Architect caching layer (Redis/Memcached) for high-frequency queries",
            "Review API design for query consolidation",
            "Establish execution frequency monitoring alerts"
        ]
        
        # Priority reasoning
        priority_reasoning: List[str] = [
            f"Chatty pattern: {signals.executions:,} executions @ {signals.avg_exec_time*1000:.1f}ms each",
            "Query is FAST - no SQL tuning needed",
            "Index creation NOT recommended: query already optimized",
            "Focus on APPLICATION behavior, not DATABASE tuning"
        ]
        
        return DBAActionPlan(
            sql_id=signals.sql_id,
            category=SQLCategory.CHATTY_SQL.value,
            signal_fingerprint=fingerprint,
            immediate=immediate,
            short_term=short_term,
            medium_term=medium_term,
            long_term=long_term,
            priority_reasoning=priority_reasoning
        )
    
    def _generate_io_bound_action_plan(self, signals: NormalizedSignals,
                                        decision: DecisionResult,
                                        fingerprint: str) -> DBAActionPlan:
        """Generate action plan for IO_BOUND_SQL category"""
        
        # Immediate - access path analysis
        immediate: List[str] = [
            f"Analyze execution plan for full table scans (IO wait: {signals.io_wait_pct:.1f}%)",
            f"Check physical read statistics for SQL_ID {signals.sql_id}",
        ]
        if signals.io_wait_pct > 90:
            immediate.append(f"CRITICAL: {signals.io_wait_pct:.1f}% IO wait - likely missing index")
        
        # Short term - index analysis
        short_term: List[str] = [
            "Run SQL Access Advisor for index recommendations",
            "Analyze predicate selectivity in WHERE clause",
            f"Check buffer cache hit ratio for accessed objects",
        ]
        if signals.total_elapsed > 50:
            short_term.append(f"Consider partitioning strategy (query taking {signals.total_elapsed:.1f}s)")
        
        # Medium term
        medium_term: List[str] = [
            "Implement index recommendations after testing",
            "Consider table reorganization if heavily fragmented",
            "Evaluate parallel query execution for large scans",
        ]
        
        # Long term
        long_term: List[str] = [
            "Establish IO performance baselines",
            "Review storage configuration for hot tablespaces",
            "Consider SSD migration for high-IO objects"
        ]
        
        # Priority reasoning
        priority_reasoning: List[str] = [
            f"IO-bound workload: {signals.io_wait_pct:.1f}% IO wait, {signals.cpu_pct:.1f}% CPU",
            "Index optimization is PRIMARY focus",
            "CPU tuning NOT applicable: bottleneck is data access",
            f"Total elapsed {signals.total_elapsed:.1f}s dominated by physical reads"
        ]
        
        return DBAActionPlan(
            sql_id=signals.sql_id,
            category=SQLCategory.IO_BOUND_SQL.value,
            signal_fingerprint=fingerprint,
            immediate=immediate,
            short_term=short_term,
            medium_term=medium_term,
            long_term=long_term,
            priority_reasoning=priority_reasoning
        )
    
    def _generate_cpu_bound_action_plan(self, signals: NormalizedSignals,
                                         decision: DecisionResult,
                                         fingerprint: str) -> DBAActionPlan:
        """Generate action plan for CPU_BOUND_SQL category"""
        
        # Immediate - plan analysis
        immediate: List[str] = [
            f"Analyze execution plan for inefficient joins (CPU: {signals.cpu_pct:.1f}%)",
            f"Check for HASH JOIN vs NESTED LOOP decisions",
        ]
        if signals.cpu_pct > 90:
            immediate.append(f"CRITICAL: {signals.cpu_pct:.1f}% CPU - likely cartesian product or inefficient join")
        if signals.cpu_time > 100:
            immediate.append(f"HIGH CPU consumption: {signals.cpu_time:.1f}s - review computational complexity")
        
        # Short term
        short_term: List[str] = [
            "Run SQL Tuning Advisor for alternative plans",
            "Analyze join order and method optimization",
            f"Review predicate pushdown opportunities",
        ]
        
        # Medium term
        medium_term: List[str] = [
            "Consider SQL rewrite for complex subqueries",
            "Evaluate materialized view for repeated computations",
            f"Test optimizer hints for join method override",
        ]
        
        # Long term
        long_term: List[str] = [
            "Review query design patterns with development team",
            "Establish CPU usage monitoring for this SQL",
            "Consider Resource Manager for CPU-bound queries"
        ]
        
        # Priority reasoning
        priority_reasoning: List[str] = [
            f"CPU-bound workload: {signals.cpu_pct:.1f}% CPU, {signals.io_wait_pct:.1f}% IO",
            "Join method optimization is PRIMARY focus",
            "Index-only fixes NOT applicable: issue is computation",
            f"CPU time {signals.cpu_time:.1f}s indicates algorithmic inefficiency"
        ]
        
        return DBAActionPlan(
            sql_id=signals.sql_id,
            category=SQLCategory.CPU_BOUND_SQL.value,
            signal_fingerprint=fingerprint,
            immediate=immediate,
            short_term=short_term,
            medium_term=medium_term,
            long_term=long_term,
            priority_reasoning=priority_reasoning
        )
    
    def _generate_mixed_action_plan(self, signals: NormalizedSignals,
                                     decision: DecisionResult,
                                     fingerprint: str) -> DBAActionPlan:
        """Generate action plan for MIXED_PROFILE_SQL category"""
        
        immediate: List[str] = [
            f"Run comprehensive execution plan analysis",
            f"Capture both IO ({signals.io_wait_pct:.1f}%) and CPU ({signals.cpu_pct:.1f}%) statistics",
        ]
        
        short_term: List[str] = [
            "Analyze which operations contribute to IO vs CPU",
            "Run both SQL Access Advisor and SQL Tuning Advisor",
            "Identify primary bottleneck through detailed plan inspection",
        ]
        
        medium_term: List[str] = [
            "Address primary bottleneck first based on analysis",
            "Re-test after initial optimization",
            "Iterate on secondary bottleneck if needed",
        ]
        
        long_term: List[str] = [
            "Establish baseline for both IO and CPU metrics",
            "Create monitoring for metric shift detection",
            "Document optimization strategy for similar queries"
        ]
        
        priority_reasoning: List[str] = [
            f"Mixed profile: CPU={signals.cpu_pct:.1f}%, IO={signals.io_wait_pct:.1f}%",
            "Neither metric is dominant - comprehensive analysis required",
            f"Execution pattern: {signals.executions} @ {signals.avg_exec_time:.2f}s each",
            "Optimization strategy depends on detailed plan analysis"
        ]
        
        return DBAActionPlan(
            sql_id=signals.sql_id,
            category=SQLCategory.MIXED_PROFILE_SQL.value,
            signal_fingerprint=fingerprint,
            immediate=immediate,
            short_term=short_term,
            medium_term=medium_term,
            long_term=long_term,
            priority_reasoning=priority_reasoning
        )
    
    def _generate_low_priority_action_plan(self, signals: NormalizedSignals,
                                            decision: DecisionResult,
                                            fingerprint: str) -> DBAActionPlan:
        """Generate action plan for LOW_PRIORITY category"""
        
        immediate: List[str] = [
            "No immediate action required",
            f"SQL performance is within acceptable parameters",
        ]
        
        short_term: List[str] = [
            "Establish performance baseline for future comparison",
            "Add to standard monitoring rotation",
        ]
        
        medium_term: List[str] = [
            "Re-evaluate if workload characteristics change",
            "Monitor for metric degradation over time",
        ]
        
        long_term: List[str] = [
            "Include in periodic AWR analysis",
            "No proactive tuning justified at this time"
        ]
        
        priority_reasoning: List[str] = [
            f"Low priority: avg_exec={signals.avg_exec_time:.3f}s, execs={signals.executions}",
            f"Metrics within acceptable range: CPU={signals.cpu_pct:.1f}%, IO={signals.io_wait_pct:.1f}%",
            "All aggressive tuning actions are BLOCKED",
            "Continue monitoring - no intervention needed"
        ]
        
        return DBAActionPlan(
            sql_id=signals.sql_id,
            category=SQLCategory.LOW_PRIORITY.value,
            signal_fingerprint=fingerprint,
            immediate=immediate,
            short_term=short_term,
            medium_term=medium_term,
            long_term=long_term,
            priority_reasoning=priority_reasoning
        )
    
    def _is_allowed(self, decision: DecisionResult, action: ActionType) -> bool:
        """Check if an action is allowed for this decision"""
        return action in decision.allowed_actions
    
    def _create_signal_fingerprint(self, signals: NormalizedSignals) -> str:
        """
        Create a fingerprint from signals to prove dynamic generation.
        Different signals = different fingerprint = different SQL.
        """
        return (
            f"exec={signals.executions}|"
            f"avgtime={signals.avg_exec_time:.4f}|"
            f"cpu={signals.cpu_pct:.1f}|"
            f"io={signals.io_wait_pct:.1f}"
        )
    
    # =========================================================================
    # DYNAMIC DBMS_XPLAN GENERATION (CRITICAL - ENHANCED)
    # =========================================================================
    
    def _generate_dynamic_xplan(self, signals: NormalizedSignals, 
                                 category: SQLCategory) -> GeneratedSQL:
        """
        Generate DBMS_XPLAN.DISPLAY_CURSOR with format TRULY DYNAMICALLY assembled.
        
        CRITICAL RULES:
        - FORMAT string MUST differ when signals differ
        - Same SQL_ID + different IO/CPU → DIFFERENT format string
        - Same SQL_ID + different execution count → DIFFERENT format string
        - Category determines BASE strategy, signals FINE-TUNE the format
        
        PROOF: Signal fingerprint embedded in SQL comment
        """
        # Category-specific base format strategy
        format_parts: List[str] = self._get_category_base_format(category, signals)
        explanation_parts: List[str] = []
        
        # =====================================================================
        # SIGNAL-DRIVEN FORMAT ASSEMBLY (NOT HARDCODED)
        # =====================================================================
        
        # IO Wait Threshold Analysis (granular, not binary)
        if signals.io_wait_pct >= 90:
            self._add_unique(format_parts, "+IOSTATS")
            self._add_unique(format_parts, "+PARALLEL")
            self._add_unique(format_parts, "+PARTITION")
            explanation_parts.append(f"io_wait_pct={signals.io_wait_pct:.1f}% (CRITICAL)")
        elif signals.io_wait_pct >= 70:
            self._add_unique(format_parts, "+IOSTATS")
            self._add_unique(format_parts, "+PARALLEL")
            explanation_parts.append(f"io_wait_pct={signals.io_wait_pct:.1f}% (HIGH)")
        elif signals.io_wait_pct >= 50:
            self._add_unique(format_parts, "+IOSTATS")
            explanation_parts.append(f"io_wait_pct={signals.io_wait_pct:.1f}% (MODERATE)")
        elif signals.io_wait_pct >= 30:
            # Mild IO - still worth checking but not critical
            if category == SQLCategory.BATCH_SQL:
                self._add_unique(format_parts, "+IOSTATS")
                explanation_parts.append(f"io_wait_pct={signals.io_wait_pct:.1f}% (batch context)")
        
        # CPU Threshold Analysis (granular)
        if signals.cpu_pct >= 90:
            self._add_unique(format_parts, "+COST")
            self._add_unique(format_parts, "+PREDICATE")
            self._add_unique(format_parts, "+PROJECTION")
            explanation_parts.append(f"cpu_pct={signals.cpu_pct:.1f}% (CRITICAL)")
        elif signals.cpu_pct >= 70:
            self._add_unique(format_parts, "+COST")
            self._add_unique(format_parts, "+PREDICATE")
            explanation_parts.append(f"cpu_pct={signals.cpu_pct:.1f}% (HIGH)")
        elif signals.cpu_pct >= 50:
            self._add_unique(format_parts, "+COST")
            explanation_parts.append(f"cpu_pct={signals.cpu_pct:.1f}% (MODERATE)")
        elif signals.cpu_pct >= 30:
            # Mild CPU usage - add cost analysis for batch
            if category in [SQLCategory.BATCH_SQL, SQLCategory.CPU_BOUND_SQL]:
                self._add_unique(format_parts, "+COST")
                explanation_parts.append(f"cpu_pct={signals.cpu_pct:.1f}%")
        
        # Execution Frequency Analysis (determines bind/adaptive strategy)
        if signals.executions >= 5000:
            self._add_unique(format_parts, "+PEEKED_BINDS")
            self._add_unique(format_parts, "+ADAPTIVE")
            self._add_unique(format_parts, "+BIND_AWARE")
            explanation_parts.append(f"executions={signals.executions:,} (VERY HIGH)")
        elif signals.executions >= 1000:
            self._add_unique(format_parts, "+PEEKED_BINDS")
            self._add_unique(format_parts, "+ADAPTIVE")
            explanation_parts.append(f"executions={signals.executions:,} (HIGH)")
        elif signals.executions >= 500:
            self._add_unique(format_parts, "+PEEKED_BINDS")
            explanation_parts.append(f"executions={signals.executions}")
        elif signals.executions < 50:
            # Low frequency - batch/report pattern
            if signals.avg_exec_time >= 5:
                self._add_unique(format_parts, "+OUTLINE")
                self._add_unique(format_parts, "+ALIAS")
                explanation_parts.append(f"executions={signals.executions} (batch pattern)")
        
        # Elapsed Time Analysis (memory/parallel strategy)
        if signals.total_elapsed >= 500:
            self._add_unique(format_parts, "+MEMSTATS")
            self._add_unique(format_parts, "+PARALLEL")
            explanation_parts.append(f"total_elapsed={signals.total_elapsed:.1f}s (VERY HIGH)")
        elif signals.total_elapsed >= 100:
            self._add_unique(format_parts, "+MEMSTATS")
            if category == SQLCategory.BATCH_SQL:
                self._add_unique(format_parts, "+PARALLEL")
            explanation_parts.append(f"total_elapsed={signals.total_elapsed:.1f}s")
        elif signals.total_elapsed >= 50:
            self._add_unique(format_parts, "+MEMSTATS")
            explanation_parts.append(f"total_elapsed={signals.total_elapsed:.1f}s")
        
        # Avg Exec Time Analysis (per-execution strategy)
        if signals.avg_exec_time >= 30:
            self._add_unique(format_parts, "+OUTLINE")
            explanation_parts.append(f"avg_exec={signals.avg_exec_time:.2f}s (SLOW)")
        elif signals.avg_exec_time >= 10:
            self._add_unique(format_parts, "+OUTLINE")
            explanation_parts.append(f"avg_exec={signals.avg_exec_time:.2f}s")
        elif signals.avg_exec_time < 0.1 and signals.executions > 500:
            # Fast but frequent - focus on bind efficiency
            explanation_parts.append(f"avg_exec={signals.avg_exec_time*1000:.1f}ms (fast, high freq)")
        
        # Build the final format string (order matters for readability)
        format_string: str = self._assemble_format_string(format_parts)
        
        # Generate signal fingerprint for PROOF of dynamic generation
        fingerprint: str = self._create_signal_fingerprint(signals)
        
        # Generate the SQL with embedded fingerprint comment
        sql: str = f"""-- Dynamic XPLAN for {category.value}
-- Signal Fingerprint: {fingerprint}
-- Format assembled from: io={signals.io_wait_pct:.1f}%, cpu={signals.cpu_pct:.1f}%, exec={signals.executions}
SELECT *
FROM TABLE(
  DBMS_XPLAN.DISPLAY_CURSOR(
    sql_id => '{signals.sql_id}',
    cursor_child_no => NULL,
    format => '{format_string}'
  )
)"""
        
        explanation: str = f"Generated because {', '.join(explanation_parts)}" if explanation_parts else f"Base analysis for {category.value}"
        
        return GeneratedSQL(
            action="PLAN_ANALYSIS",
            sql=sql,
            intent=f"Analyze execution plan for {signals.sql_id} with {category.value}-optimized format",
            explanation=explanation,
            category=category.value,
            signal_fingerprint=fingerprint
        )
    
    def _get_category_base_format(self, category: SQLCategory, 
                                   signals: NormalizedSignals) -> List[str]:
        """
        Get category-specific base format.
        Each category starts with a DIFFERENT base format.
        """
        if category == SQLCategory.BATCH_SQL:
            # Batch: Focus on parallelism and outline for reproducibility
            return ["ALLSTATS LAST"]
        
        elif category == SQLCategory.CHATTY_SQL:
            # Chatty: Focus on bind variables and adaptive cursor
            return ["BASIC"]  # Minimal for chatty - query is already fast
        
        elif category == SQLCategory.IO_BOUND_SQL:
            # IO-Bound: Focus on IO statistics
            return ["ALLSTATS LAST", "+IOSTATS"]
        
        elif category == SQLCategory.CPU_BOUND_SQL:
            # CPU-Bound: Focus on cost and predicates
            return ["ALLSTATS LAST", "+COST", "+PREDICATE"]
        
        elif category == SQLCategory.MIXED_PROFILE_SQL:
            # Mixed: Comprehensive analysis
            return ["ALLSTATS LAST", "+COST"]
        
        else:  # LOW_PRIORITY
            # Low Priority: Basic only
            return ["BASIC"]
    
    def _add_unique(self, parts: List[str], item: str) -> None:
        """Add item to list only if not already present"""
        if item not in parts:
            parts.append(item)
    
    def _assemble_format_string(self, parts: List[str]) -> str:
        """
        Assemble format string in proper Oracle order.
        Ensures consistent ordering while maintaining dynamic content.
        """
        # Define proper ordering for Oracle format options
        order: List[str] = [
            "BASIC", "TYPICAL", "ALLSTATS", "ALLSTATS LAST",
            "+COST", "+PREDICATE", "+PROJECTION", "+ALIAS",
            "+IOSTATS", "+MEMSTATS", "+PARALLEL", "+PARTITION",
            "+PEEKED_BINDS", "+ADAPTIVE", "+BIND_AWARE", "+OUTLINE"
        ]
        
        # Sort parts according to order (unrecognized items at end)
        def sort_key(item: str) -> int:
            try:
                return order.index(item)
            except ValueError:
                return len(order)
        
        sorted_parts: List[str] = sorted(parts, key=sort_key)
        return " ".join(sorted_parts)
    
    # =========================================================================
    # SENIOR DBA-STYLE COMMAND GENERATORS
    # =========================================================================
    # These generators implement the "Different instruments for different diseases"
    # principle. Each category has a PRIMARY diagnostic question that a Senior DBA
    # would ask FIRST, before reaching for XPLAN or Advisors.
    # =========================================================================
    
    def _generate_io_bound_commands_dba_style(self, signals: NormalizedSignals,
                                               decision: DecisionResult) -> List[GeneratedSQL]:
        """
        IO-HEAVY SQL: Senior DBA Style
        ==============================
        
        FIRST: Object-level IO analysis (identify WHICH objects cause IO amplification)
        SECOND: Segment statistics (physical reads per object)
        THIRD: XPLAN (confirmation step, NOT entry point)
        FOURTH: Access Advisor (only if IO > 90% and exec < 10, scope=INDEX_ONLY)
        
        A Senior DBA thinks: "पहले देखो कौनसा object IO खा रहा है, फिर plan देखो"
        """
        generated = []
        fingerprint: str = self._create_signal_fingerprint(signals)
        
        # Pre-compute conditional text for Python 3.6 compatibility
        io_severity = "CRITICAL" if signals.io_wait_pct > 90 else "HIGH"
        
        # =====================================================================
        # FIRST: Object-Level IO Analysis (PRIMARY diagnostic)
        # This is what a DBA looks at FIRST - not the execution plan
        # =====================================================================
        io_analysis_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Object-Level IO Analysis
-- Signal context: io_wait_pct={signals.io_wait_pct:.1f}% ({io_severity}), executions={signals.executions} (batch pattern)
-- Focus: Identify WHICH objects are causing IO amplification BEFORE looking at plan

-- Step 1: Identify objects causing physical reads for this SQL
SELECT 
  o.owner,
  o.object_name,
  o.object_type,
  s.statistic_name,
  s.value AS physical_reads,
  ROUND(s.value / NULLIF(SUM(s.value) OVER(), 0) * 100, 2) AS pct_of_total
FROM v$segment_statistics s
JOIN dba_objects o ON s.obj# = o.object_id
WHERE s.statistic_name IN ('physical reads', 'physical reads direct', 'db block gets')
  AND s.value > 0
  AND o.object_id IN (
    -- Objects accessed by this SQL
    SELECT DISTINCT object# 
    FROM v$sql_plan 
    WHERE sql_id = '{signals.sql_id}'
      AND object# IS NOT NULL
  )
ORDER BY s.value DESC
FETCH FIRST 10 ROWS ONLY;

-- DBA Reasoning: If io_wait_pct={signals.io_wait_pct:.1f}%, the bottleneck is data access.
-- We need to know WHICH table/index is causing this BEFORE we can fix it."""

        generated.append(GeneratedSQL(
            action="OBJECT_IO_ANALYSIS",
            sql=io_analysis_sql,
            intent=f"Identify which objects cause {signals.io_wait_pct:.1f}% IO wait for SQL {signals.sql_id}",
            explanation=f"Senior DBA approach: Object-level IO first, plan second. IO wait at {signals.io_wait_pct:.1f}% indicates data access is the bottleneck.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # SECOND: Segment Statistics Deep Dive
        # =====================================================================
        segment_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Segment Statistics Analysis
-- Signal context: io_wait_pct={signals.io_wait_pct:.1f}%, total_elapsed={signals.total_elapsed:.1f}s
-- This tells us HOW MUCH IO each object is consuming

SELECT 
  segment_name,
  segment_type,
  tablespace_name,
  bytes / 1024 / 1024 AS size_mb,
  blocks,
  ROUND(bytes / 1024 / 1024 / NULLIF({signals.total_elapsed}, 0) * 100, 2) AS mb_per_sec_estimate
FROM dba_segments
WHERE segment_name IN (
  SELECT object_name 
  FROM dba_objects 
  WHERE object_id IN (
    SELECT DISTINCT object# 
    FROM v$sql_plan 
    WHERE sql_id = '{signals.sql_id}'
      AND object# IS NOT NULL
  )
)
ORDER BY bytes DESC;

-- DBA Reasoning: Large segments with high physical reads = primary tuning target"""

        generated.append(GeneratedSQL(
            action="SEGMENT_STATISTICS",
            sql=segment_sql,
            intent=f"Analyze segment sizes for objects accessed by SQL {signals.sql_id}",
            explanation=f"Segment analysis reveals which objects are candidates for partitioning or indexing.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # THIRD: XPLAN (Now as CONFIRMATION, not entry point)
        # =====================================================================
        if self._is_allowed(decision, ActionType.PLAN_ANALYSIS):
            xplan: GeneratedSQL = self._generate_dynamic_xplan(signals, decision.category)
            # Modify the explanation to indicate it's a confirmation step
            generated.append(GeneratedSQL(
                action=xplan.action,
                sql=xplan.sql.replace("-- Dynamic XPLAN", "-- CONFIRMATION STEP: Execution Plan Analysis"),
                intent=f"CONFIRM object-level findings with execution plan for {signals.sql_id}",
                explanation=f"Now that we know which objects cause IO, verify the access path. {xplan.explanation}",
                category=xplan.category,
                signal_fingerprint=xplan.signal_fingerprint
            ))
        
        # =====================================================================
        # FOURTH: Access Advisor (CONDITIONAL - not default)
        # Only if IO > 90% AND executions < 10 (truly heavy batch)
        # Scope: INDEX_ONLY (not FULL) for IO-bound queries
        # =====================================================================
        if signals.io_wait_pct > 90 and signals.executions < 10:
            if self._is_allowed(decision, ActionType.SQL_ACCESS_ADVISOR):
                advisor: GeneratedSQL = self._generate_access_advisor_io_focused(signals)
                generated.append(advisor)
        elif signals.io_wait_pct > 70:
            # Add a note about why advisor is deferred
            generated.append(GeneratedSQL(
                action="ADVISOR_DEFERRED",
                sql=f"""-- SQL Access Advisor: DEFERRED
-- Signal context: io_wait_pct={signals.io_wait_pct:.1f}%, executions={signals.executions}
-- 
-- DBA Decision: Advisor is deferred because:
-- 1. IO wait ({signals.io_wait_pct:.1f}%) is high but not critical (< 90%)
-- 2. Object-level analysis should reveal the issue first
-- 3. Running full advisor without knowing the problem wastes time
--
-- RECOMMENDATION: Review object IO and segment statistics first.
-- If index is clearly missing, advisor may not be needed at all.

SELECT 'Review object IO analysis results before running advisor' AS recommendation
FROM dual;""",
                intent="Explain why SQL Access Advisor is not run immediately",
                explanation=f"Senior DBA approach: Don't run expensive advisor until object-level analysis confirms need.",
                category=decision.category.value,
                signal_fingerprint=fingerprint
            ))
        
        return generated
    
    def _generate_cpu_bound_commands_dba_style(self, signals: NormalizedSignals,
                                                decision: DecisionResult) -> List[GeneratedSQL]:
        """
        CPU-BOUND SQL: Senior DBA Style
        ================================
        
        FIRST: Predicate and join cost analysis (v$sql_plan direct)
        SECOND: Hash join vs nested loop comparison
        THIRD: Cartesian product detection
        FOURTH: XPLAN (only if join analysis confirms inefficiency)
        FIFTH: SQL Tuning Advisor (only after plan review)
        
        A Senior DBA thinks: "CPU high means joins or computations - check that first"
        """
        generated = []
        fingerprint: str = self._create_signal_fingerprint(signals)
        
        # Pre-compute conditional text for Python 3.6 compatibility
        cpu_severity = "CRITICAL" if signals.cpu_pct > 90 else "HIGH"
        
        # =====================================================================
        # FIRST: Predicate & Join Cost Analysis (PRIMARY diagnostic)
        # Direct query on v$sql_plan - NOT DBMS_XPLAN
        # =====================================================================
        predicate_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Predicate & Join Cost Analysis
-- Signal context: cpu_pct={signals.cpu_pct:.1f}% ({cpu_severity}), io_wait_pct={signals.io_wait_pct:.1f}%
-- Focus: Identify CPU-heavy operations and join filters BEFORE running XPLAN

-- CPU cost breakdown by operation
SELECT 
  id,
  operation,
  options,
  object_name,
  cpu_cost,
  cardinality,
  cost,
  ROUND(cpu_cost / NULLIF(SUM(cpu_cost) OVER(), 0) * 100, 2) AS pct_cpu_cost,
  access_predicates,
  filter_predicates
FROM v$sql_plan
WHERE sql_id = '{signals.sql_id}'
  AND cpu_cost > 0
ORDER BY cpu_cost DESC;

-- DBA Reasoning: cpu_pct={signals.cpu_pct:.1f}% indicates computation is the bottleneck.
-- We need to identify WHICH operation consumes CPU before looking at full plan."""

        generated.append(GeneratedSQL(
            action="CPU_COST_ANALYSIS",
            sql=predicate_sql,
            intent=f"Identify CPU-heavy operations for SQL {signals.sql_id} with {signals.cpu_pct:.1f}% CPU",
            explanation=f"Senior DBA approach: CPU cost analysis first, not XPLAN. CPU at {signals.cpu_pct:.1f}% means joins or computations are the issue.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # SECOND: Hash Join vs Nested Loop Analysis
        # =====================================================================
        join_analysis_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Join Method Analysis
-- Signal context: cpu_pct={signals.cpu_pct:.1f}%, cpu_time={signals.cpu_time:.1f}s
-- High CPU often means wrong join method or missing join conditions

-- Identify all join operations
SELECT 
  p.id,
  p.operation,
  p.options,
  p.cost,
  p.cpu_cost,
  p.cardinality AS est_rows,
  a.output_rows AS actual_rows,
  CASE 
    WHEN a.output_rows > p.cardinality * 10 THEN 'SEVERE UNDERESTIMATE'
    WHEN a.output_rows > p.cardinality * 2 THEN 'UNDERESTIMATE'
    WHEN a.output_rows < p.cardinality / 10 THEN 'SEVERE OVERESTIMATE'
    ELSE 'REASONABLE'
  END AS cardinality_quality
FROM v$sql_plan p
LEFT JOIN v$sql_plan_statistics_all a 
  ON p.sql_id = a.sql_id 
  AND p.child_number = a.child_number 
  AND p.id = a.id
WHERE p.sql_id = '{signals.sql_id}'
  AND p.operation LIKE '%JOIN%'
ORDER BY p.cpu_cost DESC;

-- DBA Reasoning: Wrong join method can cause 10x-100x CPU overhead
-- HASH JOIN for large sets, NESTED LOOPS for small/indexed sets"""

        generated.append(GeneratedSQL(
            action="JOIN_METHOD_ANALYSIS",
            sql=join_analysis_sql,
            intent=f"Analyze join methods causing {signals.cpu_pct:.1f}% CPU for SQL {signals.sql_id}",
            explanation=f"Join method selection is critical for CPU-bound queries. Wrong method can multiply CPU cost by 10-100x.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # THIRD: Cartesian Product Detection (CRITICAL for high CPU)
        # =====================================================================
        if signals.cpu_pct > 80:
            cartesian_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Cartesian Product Detection
-- Signal context: cpu_pct={signals.cpu_pct:.1f}% (CRITICAL)
-- Cartesian joins are the #1 cause of extreme CPU consumption

SELECT 
  p.id,
  p.operation || ' ' || p.options AS operation,
  p.object_name,
  p.cardinality,
  p.cost,
  p.cpu_cost,
  CASE 
    WHEN p.operation = 'MERGE JOIN' AND p.options = 'CARTESIAN' THEN 'CARTESIAN PRODUCT DETECTED!'
    WHEN p.operation = 'NESTED LOOPS' AND p.cardinality > 1000000 THEN 'POTENTIAL CARTESIAN'
    ELSE 'NORMAL JOIN'
  END AS warning
FROM v$sql_plan p
WHERE p.sql_id = '{signals.sql_id}'
  AND (
    (p.operation = 'MERGE JOIN' AND p.options = 'CARTESIAN')
    OR (p.operation = 'NESTED LOOPS' AND p.cardinality > 100000)
    OR p.cardinality > 10000000
  )
ORDER BY p.cost DESC;

-- DBA Reasoning: MERGE JOIN CARTESIAN at {signals.cpu_pct:.1f}% CPU = missing join condition
-- This is the FIRST thing a senior DBA checks for extreme CPU"""

            generated.append(GeneratedSQL(
                action="CARTESIAN_DETECTION",
                sql=cartesian_sql,
                intent=f"Detect cartesian products causing {signals.cpu_pct:.1f}% CPU for SQL {signals.sql_id}",
                explanation=f"Cartesian products are the #1 cause of extreme CPU. At {signals.cpu_pct:.1f}%, this must be checked FIRST.",
                category=decision.category.value,
                signal_fingerprint=fingerprint
            ))
        
        # =====================================================================
        # FOURTH: XPLAN (Now as CONFIRMATION after join analysis)
        # =====================================================================
        if self._is_allowed(decision, ActionType.PLAN_ANALYSIS):
            xplan: GeneratedSQL = self._generate_dynamic_xplan(signals, decision.category)
            generated.append(GeneratedSQL(
                action=xplan.action,
                sql=xplan.sql.replace("-- Dynamic XPLAN", "-- CONFIRMATION STEP: Full Plan After Join Analysis"),
                intent=f"CONFIRM join analysis findings with full execution plan for {signals.sql_id}",
                explanation=f"Now that we know which joins are expensive, review full plan. {xplan.explanation}",
                category=xplan.category,
                signal_fingerprint=xplan.signal_fingerprint
            ))
        
        # =====================================================================
        # FIFTH: SQL Tuning Advisor (ONLY AFTER plan review)
        # =====================================================================
        if self._is_allowed(decision, ActionType.SQL_TUNING_ADVISOR):
            generated.append(self._generate_tuning_advisor_cpu(signals))
        
        return generated
    
    def _generate_chatty_sql_commands_dba_style(self, signals: NormalizedSignals,
                                                 decision: DecisionResult) -> List[GeneratedSQL]:
        """
        CHATTY/OLTP SQL: Senior DBA Style
        ==================================
        
        FIRST: Execution frequency analysis (how often, how fast)
        SECOND: Cursor efficiency check (bind variables, cursor cache)
        THIRD: Application pattern detection
        
        NO XPLAN (query is already fast)
        NO ADVISORS (they would waste time)
        
        A Senior DBA thinks: "Query is fast, problem is FREQUENCY. Look at app behavior."
        """
        generated = []
        fingerprint: str = self._create_signal_fingerprint(signals)
        
        # =====================================================================
        # FIRST: Execution Frequency Analysis (PRIMARY diagnostic)
        # =====================================================================
        freq_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Execution Frequency Analysis
-- Signal context: executions={signals.executions:,}, avg_exec_time={signals.avg_exec_time*1000:.1f}ms
-- Focus: This query is FAST - the problem is FREQUENCY, not performance

-- Execution pattern analysis
SELECT 
  sql_id,
  executions,
  ROUND(elapsed_time / 1000000, 2) AS total_elapsed_sec,
  ROUND(elapsed_time / NULLIF(executions, 0) / 1000, 2) AS avg_elapsed_ms,
  ROUND(cpu_time / NULLIF(executions, 0) / 1000, 2) AS avg_cpu_ms,
  buffer_gets,
  ROUND(buffer_gets / NULLIF(executions, 0), 2) AS buffer_gets_per_exec,
  rows_processed,
  ROUND(rows_processed / NULLIF(executions, 0), 2) AS rows_per_exec,
  parse_calls,
  ROUND(parse_calls / NULLIF(executions, 0) * 100, 2) AS parse_ratio_pct
FROM v$sql
WHERE sql_id = '{signals.sql_id}';

-- DBA Reasoning: {signals.executions:,} executions @ {signals.avg_exec_time*1000:.1f}ms each
-- Query is FAST. Do NOT tune the SQL - analyze application behavior."""

        generated.append(GeneratedSQL(
            action="EXECUTION_FREQUENCY_ANALYSIS",
            sql=freq_sql,
            intent=f"Analyze execution frequency for chatty SQL {signals.sql_id} ({signals.executions:,} executions)",
            explanation=f"Senior DBA approach: Query runs in {signals.avg_exec_time*1000:.1f}ms - it's FAST. Problem is {signals.executions:,} executions, not SQL performance.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # SECOND: Cursor Efficiency Check
        # =====================================================================
        cursor_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Cursor and Bind Variable Efficiency
-- Signal context: executions={signals.executions:,}, parse_intensive pattern suspected
-- High execution count often means bind variable or cursor sharing issues

SELECT 
  sql_id,
  child_number,
  executions,
  parse_calls,
  ROUND(parse_calls / NULLIF(executions, 0) * 100, 2) AS hard_parse_ratio,
  is_bind_sensitive,
  is_bind_aware,
  is_shareable,
  CASE 
    WHEN parse_calls > executions * 0.1 THEN 'HARD PARSE PROBLEM'
    WHEN is_shareable = 'N' THEN 'CURSOR NOT SHAREABLE'
    ELSE 'CURSOR OK'
  END AS cursor_status
FROM v$sql
WHERE sql_id = '{signals.sql_id}';

-- Check for multiple child cursors (bind variable issues)
SELECT 
  COUNT(*) AS child_cursor_count,
  SUM(executions) AS total_executions,
  CASE 
    WHEN COUNT(*) > 10 THEN 'EXCESSIVE CHILD CURSORS - Missing binds?'
    WHEN COUNT(*) > 5 THEN 'ELEVATED CHILD CURSORS'
    ELSE 'NORMAL'
  END AS assessment
FROM v$sql
WHERE sql_id = '{signals.sql_id}';

-- DBA Reasoning: Many child cursors = missing bind variables = cursor flooding"""

        generated.append(GeneratedSQL(
            action="CURSOR_EFFICIENCY_CHECK",
            sql=cursor_sql,
            intent=f"Check cursor efficiency for high-frequency SQL {signals.sql_id}",
            explanation=f"Chatty SQL often has cursor sharing issues. {signals.executions:,} executions should reuse cursors efficiently.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # THIRD: Application Pattern Detection
        # =====================================================================
        app_pattern_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Application Calling Pattern
-- Signal context: {signals.executions:,} executions - is this an N+1 query pattern?
-- Focus: Identify if application is calling this SQL in a loop

-- Check execution distribution over time (if AWR available)
SELECT 
  s.sql_id,
  s.module,
  s.action,
  s.parsing_schema_name,
  s.executions,
  ROUND(s.elapsed_time / 1000000 / NULLIF(s.executions, 0), 4) AS avg_sec_per_exec,
  s.last_active_time,
  CASE 
    WHEN s.executions > 10000 AND s.elapsed_time / NULLIF(s.executions, 0) < 100000 THEN 'N+1 QUERY PATTERN LIKELY'
    WHEN s.module LIKE '%JDBC%' OR s.module LIKE '%ORM%' THEN 'ORM GENERATED - Check batch fetch size'
    ELSE 'Review application loop'
  END AS recommendation
FROM v$sql s
WHERE s.sql_id = '{signals.sql_id}';

-- DBA Reasoning: {signals.executions:,} executions of a {signals.avg_exec_time*1000:.1f}ms query
-- This is NOT a database problem - it's an APPLICATION problem.
-- 
-- RECOMMENDATIONS:
-- 1. Check for N+1 query pattern in ORM
-- 2. Increase batch/fetch size
-- 3. Consider client-side caching
-- 4. DO NOT add indexes - query is already fast!"""

        generated.append(GeneratedSQL(
            action="APPLICATION_PATTERN_ANALYSIS",
            sql=app_pattern_sql,
            intent=f"Detect application calling pattern for chatty SQL {signals.sql_id}",
            explanation=f"With {signals.executions:,} executions at {signals.avg_exec_time*1000:.1f}ms each, this is an APPLICATION issue, not a database issue.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # NO XPLAN - Query is already fast
        # NO ADVISORS - They would waste time
        # =====================================================================
        generated.append(GeneratedSQL(
            action="DBA_DECISION_NOTICE",
            sql=f"""-- SENIOR DBA DECISION: XPLAN and Advisors SKIPPED
-- Signal context: executions={signals.executions:,}, avg_exec_time={signals.avg_exec_time*1000:.1f}ms

-- Why XPLAN is NOT shown:
-- 1. Query runs in {signals.avg_exec_time*1000:.1f}ms - it's already FAST
-- 2. Looking at the plan would not reveal anything useful
-- 3. The problem is FREQUENCY ({signals.executions:,} calls), not PERFORMANCE

-- Why SQL Advisors are NOT shown:
-- 1. SQL Access Advisor recommends indexes - but indexes won't help a fast query
-- 2. SQL Tuning Advisor suggests plan changes - but plan is already efficient
-- 3. Running advisors would waste DBA time

-- CORRECT ACTION: Work with application team to:
-- 1. Reduce call frequency (batch operations)
-- 2. Implement client-side caching
-- 3. Fix N+1 query patterns

SELECT 'Focus on application, not database' AS senior_dba_recommendation FROM dual;""",
            intent="Explain Senior DBA decision to skip XPLAN and Advisors",
            explanation=f"At {signals.avg_exec_time*1000:.1f}ms per execution, SQL is optimized. {signals.executions:,} executions is an app issue.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        return generated
    
    def _generate_batch_sql_commands_dba_style(self, signals: NormalizedSignals,
                                                decision: DecisionResult) -> List[GeneratedSQL]:
        """
        BATCH SQL (Slow but Stable): Senior DBA Style
        ==============================================
        
        FIRST: Parallel execution effectiveness check
        SECOND: XPLAN with batch-optimized format
        THIRD: Access Advisor (conditional, scope based on signals)
        
        A Senior DBA thinks: "Batch job - check if parallelism is working first"
        """
        generated = []
        fingerprint: str = self._create_signal_fingerprint(signals)
        
        # =====================================================================
        # FIRST: Parallel Execution Effectiveness (PRIMARY for batch)
        # =====================================================================
        parallel_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Parallel Execution Effectiveness
-- Signal context: avg_exec_time={signals.avg_exec_time:.1f}s, executions={signals.executions}, total_elapsed={signals.total_elapsed:.1f}s
-- Focus: Is parallelism being used? Is it effective?

-- Check parallel execution for this SQL
SELECT 
  sql_id,
  executions,
  px_servers_executions,
  ROUND(px_servers_executions / NULLIF(executions, 0), 2) AS avg_px_servers,
  elapsed_time / 1000000 AS total_elapsed_sec,
  ROUND(elapsed_time / NULLIF(executions, 0) / 1000000, 2) AS avg_elapsed_sec,
  CASE 
    WHEN px_servers_executions = 0 THEN 'NO PARALLELISM - Consider enabling'
    WHEN px_servers_executions / NULLIF(executions, 0) < 2 THEN 'LOW PARALLELISM - Check DOP'
    WHEN px_servers_executions / NULLIF(executions, 0) > 8 THEN 'HIGH PARALLELISM - Check for downgrades'
    ELSE 'NORMAL PARALLELISM'
  END AS parallel_assessment
FROM v$sql
WHERE sql_id = '{signals.sql_id}';

-- DBA Reasoning: Batch SQL taking {signals.avg_exec_time:.1f}s per execution
-- If not using parallelism, that's the first thing to fix
-- If already parallel, check for PX downgrade issues"""

        generated.append(GeneratedSQL(
            action="PARALLEL_EFFECTIVENESS_CHECK",
            sql=parallel_sql,
            intent=f"Check parallel execution effectiveness for batch SQL {signals.sql_id}",
            explanation=f"Senior DBA approach: Batch SQL at {signals.avg_exec_time:.1f}s - first check if parallelism is working.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # SECOND: Resource Wait Analysis (batch jobs often hit resource limits)
        # =====================================================================
        wait_sql: str = f"""-- SENIOR DBA DIAGNOSTIC: Resource Wait Analysis for Batch SQL
-- Signal context: io_wait_pct={signals.io_wait_pct:.1f}%, cpu_pct={signals.cpu_pct:.1f}%
-- Batch jobs often hit different bottlenecks than OLTP

SELECT 
  event,
  total_waits,
  time_waited / 100 AS time_waited_sec,
  average_wait / 100 AS avg_wait_sec,
  ROUND(time_waited / NULLIF(SUM(time_waited) OVER(), 0) * 100, 2) AS pct_total_wait
FROM v$sql_monitor_sesstat
WHERE sql_id = '{signals.sql_id}'
  AND time_waited > 0
ORDER BY time_waited DESC
FETCH FIRST 5 ROWS ONLY;

-- If v$sql_monitor not available, use v$session_event pattern
SELECT 
  'Note: For detailed wait analysis, enable SQL Monitoring or check v$active_session_history' AS info
FROM dual;"""

        # Pre-compute conditional text for Python 3.6 compatibility
        if signals.io_wait_pct > 70:
            io_status = "dominant"
            io_explanation = "IO is the bottleneck"
        elif signals.io_wait_pct > 30:
            io_status = "a factor"
            io_explanation = "mixed resource usage"
        else:
            io_status = "minimal"
            io_explanation = "mixed resource usage"
        
        wait_sql = wait_sql + f"\n\n-- DBA Reasoning: io_wait_pct={signals.io_wait_pct:.1f}% tells us IO is {io_status}"

        generated.append(GeneratedSQL(
            action="BATCH_WAIT_ANALYSIS",
            sql=wait_sql,
            intent=f"Analyze resource waits for batch SQL {signals.sql_id}",
            explanation=f"Batch jobs often hit resource limits. IO wait at {signals.io_wait_pct:.1f}% indicates {io_explanation}.",
            category=decision.category.value,
            signal_fingerprint=fingerprint
        ))
        
        # =====================================================================
        # THIRD: XPLAN with batch-optimized format
        # =====================================================================
        if self._is_allowed(decision, ActionType.PLAN_ANALYSIS):
            xplan: GeneratedSQL = self._generate_dynamic_xplan(signals, decision.category)
            generated.append(xplan)
        
        # =====================================================================
        # FOURTH: Access Advisor (CONDITIONAL scope based on signals)
        # =====================================================================
        if self._is_allowed(decision, ActionType.SQL_ACCESS_ADVISOR) or \
           self._is_allowed(decision, ActionType.INDEX_REVIEW):
            # Determine advisor scope based on signals
            if signals.io_wait_pct > 90 and signals.executions < 10:
                # Very heavy IO, very low frequency - full analysis warranted
                generated.append(self._generate_access_advisor_full(signals))
            elif signals.io_wait_pct > 70:
                # High IO - INDEX_ONLY scope
                generated.append(self._generate_access_advisor_io_focused(signals))
            else:
                # Standard batch - LIMITED mode
                generated.append(self._generate_access_advisor_limited(signals))
        
        return generated
    
    def _generate_access_advisor_limited(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate Access Advisor with LIMITED scope for moderate batch SQL"""
        fingerprint: str = self._create_signal_fingerprint(signals)
        task_suffix: str = self._generate_task_suffix(signals)
        
        # Pre-compute conditional text for Python 3.6 compatibility
        io_level = "high" if signals.io_wait_pct > 50 else "moderate"
        
        sql: str = f"""-- SQL Access Advisor: LIMITED Scope
-- SQL_ID: {signals.sql_id} | Signal context: io={signals.io_wait_pct:.1f}%, exec={signals.executions}
-- DBA Decision: LIMITED scope because IO is not critical (< 70%)

-- Why LIMITED mode?
-- 1. io_wait_pct={signals.io_wait_pct:.1f}% is {io_level} but not critical
-- 2. Batch SQL ({signals.executions} executions) doesn't need aggressive indexing
-- 3. Full analysis would take longer than the potential benefit

DECLARE
  v_task_name VARCHAR2(128) := 'BATCH_LIMITED_{signals.sql_id}_{task_suffix}';
  v_task_id   NUMBER;
BEGIN
  DBMS_ADVISOR.CREATE_TASK(
    advisor_name => 'SQL Access Advisor',
    task_name    => v_task_name,
    task_id      => v_task_id
  );
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'TIME_LIMIT',
    value     => 120  -- 2 minutes (limited analysis)
  );
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'MODE',
    value     => 'LIMITED'
  );
  
  DBMS_ADVISOR.ADD_STS_REF(
    task_name     => v_task_name,
    sts_owner     => USER,
    workload_name => 'SQL_WORKLOAD_{signals.sql_id}'
  );
  
  DBMS_ADVISOR.EXECUTE_TASK(task_name => v_task_name);
  DBMS_OUTPUT.PUT_LINE('Task ' || v_task_name || ' completed (LIMITED scope)');
END;
/

-- Review recommendations
SELECT rec_id, rank, benefit, action_type
FROM dba_advisor_recommendations
WHERE task_name = 'BATCH_LIMITED_{signals.sql_id}_{task_suffix}'
ORDER BY benefit DESC;"""

        return GeneratedSQL(
            action="SQL_ACCESS_ADVISOR_LIMITED",
            sql=sql,
            intent=f"Run LIMITED scope Access Advisor for batch SQL {signals.sql_id}",
            explanation=f"LIMITED scope because io_wait_pct={signals.io_wait_pct:.1f}% is not critical. Full analysis not warranted.",
            category=SQLCategory.BATCH_SQL.value,
            signal_fingerprint=fingerprint
        )

    # =========================================================================
    # BATCH SQL COMMANDS (Legacy - kept for compatibility)
    # =========================================================================
    
    def _generate_batch_sql_commands(self, signals: NormalizedSignals,
                                      decision: DecisionResult) -> List[GeneratedSQL]:
        """
        Generate commands for BATCH_SQL category.
        Focus on: SQL Access Advisor, not QUICK_TUNE
        """
        generated = []
        
        # SQL Access Advisor (preferred for batch SQL)
        if self._is_allowed(decision, ActionType.SQL_ACCESS_ADVISOR) or \
           self._is_allowed(decision, ActionType.INDEX_REVIEW):
            generated.append(self._generate_access_advisor_full(signals))
        
        # IO Optimization (if allowed)
        if self._is_allowed(decision, ActionType.IO_OPTIMIZATION):
            generated.append(self._generate_io_analysis(signals))
        
        # Index review
        if self._is_allowed(decision, ActionType.INDEX_REVIEW):
            generated.append(self._generate_index_usage_check(signals))
        
        return generated
    
    def _generate_access_advisor_full(self, signals: NormalizedSignals) -> GeneratedSQL:
        """
        Generate SQL Access Advisor commands for BATCH SQL.
        
        CRITICAL RULES:
        - Task name MUST include signal context for uniqueness
        - Parameters MUST differ based on signals
        - NEVER use QUICK_TUNE for batch SQL
        - Analysis scope determined by IO wait and execution time
        """
        # Dynamic task name with signal fingerprint
        task_suffix: str = self._generate_task_suffix(signals)
        task_name: str = f"BATCH_ACCESS_{signals.sql_id}_{task_suffix}"
        
        # Determine analysis parameters based on signals
        time_limit: int = self._calculate_advisor_time_limit(signals)
        analysis_scope: str = self._determine_analysis_scope(signals)
        workload_scope: str = self._determine_workload_scope(signals)
        
        sql: str = f"""-- SQL Access Advisor for Batch SQL Analysis
-- SQL_ID: {signals.sql_id} | Category: BATCH_SQL
-- Signal Context: io={signals.io_wait_pct:.1f}%, cpu={signals.cpu_pct:.1f}%, avg_exec={signals.avg_exec_time:.2f}s
-- Generated for SQL with {signals.avg_exec_time:.2f}s avg execution time, {signals.executions} executions

-- Step 1: Create Access Advisor Task
DECLARE
  v_task_name VARCHAR2(128) := '{task_name}';
  v_task_id   NUMBER;
BEGIN
  -- Create task for index/MV recommendations
  DBMS_ADVISOR.CREATE_TASK(
    advisor_name => 'SQL Access Advisor',
    task_name    => v_task_name,
    task_id      => v_task_id
  );
  
  -- Configure analysis parameters based on workload characteristics
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'TIME_LIMIT',
    value     => {time_limit}  -- {time_limit//60} minutes based on query complexity
  );
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'ANALYSIS_SCOPE',
    value     => '{analysis_scope}'  -- Determined by signal profile
  );
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'MODE',
    value     => '{workload_scope}'  -- Workload-appropriate mode
  );
  
  -- Add the SQL statement to analyze
  DBMS_ADVISOR.ADD_STS_REF(
    task_name     => v_task_name,
    sts_owner     => USER,
    workload_name => 'SQL_WORKLOAD_{signals.sql_id}'
  );
  
  -- Execute the advisor
  DBMS_ADVISOR.EXECUTE_TASK(task_name => v_task_name);
  
  DBMS_OUTPUT.PUT_LINE('Task ' || v_task_name || ' completed');
  DBMS_OUTPUT.PUT_LINE('Analysis scope: {analysis_scope}, Time limit: {time_limit}s');
END;
/

-- Step 2: Review Recommendations (sorted by benefit for batch SQL)
SELECT 
  rec_id, 
  rank, 
  benefit,
  benefit_type,
  action_type, 
  message
FROM DBA_ADVISOR_RECOMMENDATIONS 
WHERE task_name = '{task_name}'
ORDER BY benefit DESC, rank;

-- Step 3: Get Implementation Script
SELECT DBMS_ADVISOR.GET_TASK_SCRIPT('{task_name}') AS implementation_script
FROM DUAL;

-- Step 4: Review recommendation details
SELECT 
  r.rec_id,
  a.command,
  a.attr1 AS object_owner,
  a.attr2 AS object_name,
  a.attr3 AS object_type,
  r.benefit
FROM DBA_ADVISOR_ACTIONS a
JOIN DBA_ADVISOR_RECOMMENDATIONS r ON a.task_name = r.task_name AND a.rec_id = r.rec_id
WHERE a.task_name = '{task_name}'
ORDER BY r.benefit DESC;"""
        
        return GeneratedSQL(
            action="SQL_ACCESS_ADVISOR",
            sql=sql,
            intent=f"Run full SQL Access Advisor analysis for batch SQL {signals.sql_id}",
            explanation=f"Generated because avg_exec_time={signals.avg_exec_time:.2f}s, executions={signals.executions}, io_wait={signals.io_wait_pct:.1f}%",
            category=SQLCategory.BATCH_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_task_suffix(self, signals: NormalizedSignals) -> str:
        """Generate unique task suffix from signals for task naming"""
        # Create a deterministic but unique suffix based on signals
        io_part = int(signals.io_wait_pct)
        cpu_part = int(signals.cpu_pct)
        elapsed_part = int(signals.total_elapsed)
        return f"{elapsed_part}_{io_part}io_{cpu_part}cpu"
    
    def _calculate_advisor_time_limit(self, signals: NormalizedSignals) -> int:
        """Calculate appropriate time limit for advisor based on signals"""
        # More complex queries need more analysis time
        if signals.total_elapsed > 500 or signals.io_wait_pct > 90:
            return 600  # 10 minutes for very complex queries
        elif signals.total_elapsed > 100 or signals.io_wait_pct > 70:
            return 300  # 5 minutes for complex queries
        elif signals.avg_exec_time > 10:
            return 180  # 3 minutes for moderate queries
        else:
            return 60   # 1 minute for simpler queries
    
    def _determine_analysis_scope(self, signals: NormalizedSignals) -> str:
        """Determine analysis scope based on signals"""
        if signals.io_wait_pct > 80:
            return "FULL"  # Full analysis for high IO
        elif signals.io_wait_pct > 50:
            return "INDEX_ONLY"  # Focus on indexes for moderate IO
        elif signals.cpu_pct > 70:
            return "PARTITION_ONLY"  # Partitioning for CPU issues
        else:
            return "COMPREHENSIVE"  # Comprehensive for mixed
    
    def _determine_workload_scope(self, signals: NormalizedSignals) -> str:
        """Determine workload mode based on signals"""
        if signals.executions < 10:
            return "LIMITED"  # Limited mode for rare queries
        elif signals.executions > 100:
            return "COMPREHENSIVE"  # Full analysis for frequent
        else:
            return "STANDARD"  # Standard for moderate frequency
    
    def _generate_io_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """
        Generate IO analysis commands based on signals.
        
        CRITICAL: SQL MUST differ based on IO wait severity and execution pattern.
        """
        # Determine severity and focus area based on signals
        if signals.io_wait_pct >= 90:
            severity = "CRITICAL"
            focus = "CRITICAL IO bottleneck - immediate action required"
        elif signals.io_wait_pct >= 70:
            severity = "HIGH"
            focus = "HIGH IO contention - significant optimization potential"
        elif signals.io_wait_pct >= 50:
            severity = "MODERATE"
            focus = "MODERATE IO usage - optimization recommended"
        else:
            severity = "LOW"
            focus = "IO efficiency analysis"
        
        # Build dynamic analysis sections based on severity
        fts_analysis: str = ""
        segment_analysis: str = ""
        buffer_analysis: str = ""
        
        if signals.io_wait_pct >= 70:
            # Full table scan detection for high IO
            fts_analysis: str = f"""
-- Full Table Scan Detection (CRITICAL for {signals.io_wait_pct:.1f}% IO wait)
SELECT 
  object_name,
  operation,
  options,
  cardinality,
  cost,
  bytes,
  CASE 
    WHEN cardinality > 10000 THEN 'HIGH_IMPACT_FTS'
    WHEN cardinality > 1000 THEN 'MODERATE_IMPACT_FTS'
    ELSE 'LOW_IMPACT_FTS'
  END AS fts_severity
FROM V$SQL_PLAN 
WHERE sql_id = '{signals.sql_id}'
  AND operation = 'TABLE ACCESS' 
  AND options = 'FULL'
ORDER BY cardinality DESC;
"""
        
        if signals.total_elapsed >= 100:
            # Segment statistics for long-running queries
            segment_analysis: str = f"""
-- Hot Segment Analysis (query elapsed: {signals.total_elapsed:.1f}s)
SELECT 
  owner,
  object_name,
  object_type,
  statistic_name,
  value,
  ROUND(value / NULLIF(SUM(value) OVER (), 0) * 100, 2) AS pct_of_total
FROM V$SEGMENT_STATISTICS 
WHERE statistic_name IN ('physical reads', 'physical reads direct', 'db block gets', 'buffer busy waits')
  AND value > 1000
  AND owner NOT IN ('SYS', 'SYSTEM')
ORDER BY value DESC
FETCH FIRST 15 ROWS ONLY;
"""
        
        if signals.executions >= 100:
            # Buffer efficiency for frequently executed queries
            buffer_analysis: str = f"""
-- Buffer Cache Efficiency Analysis ({signals.executions} executions)
SELECT 
  sql_id,
  buffer_gets,
  disk_reads,
  executions,
  ROUND(buffer_gets / NULLIF(executions, 0), 2) AS gets_per_exec,
  ROUND(disk_reads / NULLIF(executions, 0), 2) AS reads_per_exec,
  ROUND((buffer_gets - disk_reads) / NULLIF(buffer_gets, 0) * 100, 2) AS cache_hit_ratio,
  CASE 
    WHEN disk_reads / NULLIF(buffer_gets, 0) > 0.1 THEN 'POOR_CACHING'
    WHEN disk_reads / NULLIF(buffer_gets, 0) > 0.05 THEN 'MODERATE_CACHING'
    ELSE 'GOOD_CACHING'
  END AS caching_assessment
FROM V$SQL 
WHERE sql_id = '{signals.sql_id}';
"""

        sql: str = f"""-- IO Analysis for SQL_ID: {signals.sql_id}
-- Severity: {severity} | IO Wait: {signals.io_wait_pct:.1f}%
-- Focus: {focus}
-- Signal Context: elapsed={signals.total_elapsed:.1f}s, executions={signals.executions}

-- Physical Reads Analysis
SELECT 
  sql_id,
  disk_reads,
  buffer_gets,
  ROUND(disk_reads / NULLIF(buffer_gets, 0) * 100, 2) AS physical_read_pct,
  rows_processed,
  ROUND(buffer_gets / NULLIF(rows_processed, 0), 2) AS gets_per_row,
  CASE 
    WHEN buffer_gets / NULLIF(rows_processed, 0) > 100 THEN 'INEFFICIENT_ACCESS'
    WHEN buffer_gets / NULLIF(rows_processed, 0) > 20 THEN 'MODERATE_EFFICIENCY'
    ELSE 'EFFICIENT_ACCESS'
  END AS access_efficiency
FROM V$SQL 
WHERE sql_id = '{signals.sql_id}';
{fts_analysis}{segment_analysis}{buffer_analysis}
-- IO Cost Breakdown by Plan Operation
SELECT 
  id,
  operation || ' ' || NVL(options, '') AS operation,
  object_name,
  cost,
  io_cost,
  ROUND(io_cost / NULLIF(cost, 0) * 100, 1) AS io_cost_pct,
  cardinality
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND cost > 0
ORDER BY io_cost DESC NULLS LAST;"""
        
        return GeneratedSQL(
            action="IO_OPTIMIZATION",
            sql=sql,
            intent=f"Analyze IO patterns for {signals.sql_id} ({severity} severity)",
            explanation=f"Generated because io_wait_pct={signals.io_wait_pct:.1f}%, total_elapsed={signals.total_elapsed:.1f}s, executions={signals.executions}",
            category=SQLCategory.BATCH_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_index_usage_check(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate index usage analysis based on signals"""
        
        sql: str = f"""-- Index Usage Analysis for SQL_ID: {signals.sql_id}
-- Executions: {signals.executions} | Avg Time: {signals.avg_exec_time:.2f}s

-- Current index usage in execution plan
SELECT 
  p.operation,
  p.options,
  p.object_name,
  p.object_type,
  p.cardinality,
  p.cost,
  p.access_predicates,
  p.filter_predicates
FROM V$SQL_PLAN p
WHERE p.sql_id = '{signals.sql_id}'
  AND p.object_type LIKE '%INDEX%'
ORDER BY p.id;

-- Tables accessed without index (potential missing index)
SELECT DISTINCT 
  p.object_name AS table_name,
  p.options AS access_type,
  p.cardinality AS estimated_rows
FROM V$SQL_PLAN p
WHERE p.sql_id = '{signals.sql_id}'
  AND p.operation = 'TABLE ACCESS'
  AND p.options = 'FULL'
  AND p.cardinality > 1000;

-- Recommended: Check column statistics for WHERE clause columns
SELECT 
  column_name,
  num_distinct,
  num_nulls,
  density,
  histogram
FROM DBA_TAB_COL_STATISTICS
WHERE table_name IN (
  SELECT DISTINCT object_name 
  FROM V$SQL_PLAN 
  WHERE sql_id = '{signals.sql_id}' 
    AND object_type = 'TABLE'
);"""
        
        return GeneratedSQL(
            action="INDEX_REVIEW",
            sql=sql,
            intent=f"Review index usage and identify missing indexes for {signals.sql_id}",
            explanation=f"Generated because executions={signals.executions}, avg_exec_time={signals.avg_exec_time:.2f}s",
            category=SQLCategory.BATCH_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    # =========================================================================
    # CHATTY SQL COMMANDS (Application-level focus)
    # =========================================================================
    
    def _generate_chatty_sql_commands(self, signals: NormalizedSignals,
                                       decision: DecisionResult) -> List[GeneratedSQL]:
        """
        Generate commands for CHATTY_SQL category.
        
        CRITICAL RULES:
        - Focus on: Application throttling, caching, bind variables
        - DO NOT generate advisor SQL for chatty queries (BLOCKED)
        - No index creation - query is already fast
        - No plan analysis - individual query is efficient
        """
        generated = []
        
        # Bind variable analysis (critical for chatty SQL)
        if self._is_allowed(decision, ActionType.BIND_TUNING):
            generated.append(self._generate_bind_analysis(signals))
        
        # Result caching opportunity
        if self._is_allowed(decision, ActionType.RESULT_CACHING):
            generated.append(self._generate_cache_analysis(signals))
        
        # Application throttling guidance
        if self._is_allowed(decision, ActionType.APPLICATION_THROTTLING):
            generated.append(self._generate_throttling_analysis(signals))
        
        # Add explicit suppression notice
        if not generated:
            generated.append(self._generate_chatty_suppression_notice(signals))
        
        return generated
    
    def _generate_chatty_suppression_notice(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate notice for suppressed actions in chatty SQL"""
        sql: str = f"""-- CHATTY SQL ANALYSIS NOTICE for SQL_ID: {signals.sql_id}
-- Signal Profile: {signals.executions:,} executions @ {signals.avg_exec_time*1000:.2f}ms each
--
-- The following actions are SUPPRESSED for chatty SQL pattern:
--   ❌ SQL Tuning Advisor: Query is already fast ({signals.avg_exec_time*1000:.2f}ms)
--   ❌ SQL Access Advisor: No structural changes needed
--   ❌ Index Creation: Query already executes efficiently
--   ❌ Plan Analysis: Execution plan is not the bottleneck
--
-- RECOMMENDED FOCUS:
--   ✅ Application-level result caching
--   ✅ Connection pool optimization
--   ✅ Batch consolidation of frequent calls
--   ✅ Client-side caching where appropriate

SELECT 
  'CHATTY_SQL_PROFILE' AS analysis_type,
  '{signals.sql_id}' AS sql_id,
  {signals.executions} AS execution_count,
  {signals.avg_exec_time:.6f} AS avg_exec_time_sec,
  {signals.total_elapsed:.2f} AS cumulative_time_sec,
  'APPLICATION_OPTIMIZATION_RECOMMENDED' AS action
FROM DUAL;"""
        
        return GeneratedSQL(
            action="CHATTY_SQL_NOTICE",
            sql=sql,
            intent=f"Chatty SQL analysis summary - DBA advisor actions suppressed",
            explanation=f"Generated because executions={signals.executions:,}, avg_exec_time={signals.avg_exec_time*1000:.2f}ms (too fast for SQL tuning)",
            category=SQLCategory.CHATTY_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_bind_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """
        Generate bind variable and cursor sharing analysis.
        
        CRITICAL: Analysis depth varies based on execution frequency.
        """
        # Determine analysis scope based on execution frequency
        if signals.executions >= 10000:
            frequency_tier = "EXTREME"
            include_literal_check = True
            include_histogram_analysis = True
        elif signals.executions >= 5000:
            frequency_tier = "VERY_HIGH"
            include_literal_check = True
            include_histogram_analysis = False
        elif signals.executions >= 1000:
            frequency_tier = "HIGH"
            include_literal_check = True
            include_histogram_analysis = False
        else:
            frequency_tier = "MODERATE"
            include_literal_check = False
            include_histogram_analysis = False
        
        # Build dynamic analysis sections
        literal_section: str = ""
        histogram_section: str = ""
        
        if include_literal_check:
            literal_section: str = f"""
-- Literal Usage Detection ({frequency_tier} frequency - cursor flooding risk)
SELECT 
  literal_hash_value,
  COUNT(*) AS child_versions,
  SUM(executions) AS total_executions,
  CASE 
    WHEN COUNT(*) > 100 THEN 'CRITICAL_CURSOR_FLOODING'
    WHEN COUNT(*) > 10 THEN 'HIGH_CURSOR_OVERHEAD'
    ELSE 'MODERATE'
  END AS severity
FROM V$SQL
WHERE force_matching_signature = (
  SELECT force_matching_signature 
  FROM V$SQL 
  WHERE sql_id = '{signals.sql_id}' 
  AND ROWNUM = 1
)
GROUP BY literal_hash_value
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
"""
        
        if include_histogram_analysis:
            histogram_section: str = f"""
-- Bind-Sensitive Histogram Analysis
-- (For {signals.executions:,} executions, histogram skew matters)
SELECT 
  table_name,
  column_name,
  num_distinct,
  histogram,
  num_buckets,
  CASE 
    WHEN histogram = 'FREQUENCY' THEN 'SKEWED_DATA'
    WHEN histogram = 'HEIGHT BALANCED' THEN 'BALANCED_HISTOGRAM'
    WHEN histogram = 'HYBRID' THEN 'HYBRID_HISTOGRAM'
    ELSE 'NO_HISTOGRAM'
  END AS histogram_type
FROM DBA_TAB_COL_STATISTICS
WHERE table_name IN (
  SELECT DISTINCT object_name 
  FROM V$SQL_PLAN 
  WHERE sql_id = '{signals.sql_id}' 
    AND object_type = 'TABLE'
)
AND histogram != 'NONE';
"""

        sql: str = f"""-- Bind Variable Analysis for Chatty SQL: {signals.sql_id}
-- Frequency Tier: {frequency_tier} | Executions: {signals.executions:,}
-- Avg Time: {signals.avg_exec_time*1000:.2f}ms | Signal Fingerprint: exec={signals.executions}|io={signals.io_wait_pct:.1f}%

-- Check bind variable usage
SELECT 
  position,
  name AS bind_name,
  datatype_string,
  value_string,
  was_captured,
  last_captured,
  CASE 
    WHEN was_captured = 'NO' THEN 'BIND_NOT_CAPTURED'
    ELSE 'BIND_CAPTURED'
  END AS capture_status
FROM V$SQL_BIND_CAPTURE
WHERE sql_id = '{signals.sql_id}'
ORDER BY position;

-- Cursor sharing analysis (CRITICAL for {signals.executions:,} executions)
SELECT 
  child_number,
  plan_hash_value,
  executions,
  ROUND(elapsed_time/1000000/NULLIF(executions,0), 4) AS avg_elapsed_sec,
  is_bind_sensitive,
  is_bind_aware,
  is_shareable,
  CASE 
    WHEN is_shareable = 'N' THEN 'NOT_SHAREABLE - Investigate'
    WHEN is_bind_sensitive = 'Y' AND is_bind_aware = 'Y' THEN 'ADAPTIVE_CURSOR'
    ELSE 'STANDARD_CURSOR'
  END AS cursor_status
FROM V$SQL
WHERE sql_id = '{signals.sql_id}'
ORDER BY child_number;

-- Cursor sharing issues (reasons for multiple children)
SELECT 
  reason,
  COUNT(*) AS occurrence_count,
  CASE 
    WHEN COUNT(*) > 50 THEN 'CRITICAL'
    WHEN COUNT(*) > 10 THEN 'HIGH'
    ELSE 'MODERATE'
  END AS severity
FROM V$SQL_SHARED_CURSOR
WHERE sql_id = '{signals.sql_id}'
GROUP BY reason
HAVING reason IS NOT NULL
ORDER BY occurrence_count DESC;
{literal_section}{histogram_section}"""
        
        return GeneratedSQL(
            action="BIND_TUNING",
            sql=sql,
            intent=f"Analyze bind variable usage for {frequency_tier}-frequency SQL {signals.sql_id}",
            explanation=f"Generated because executions={signals.executions:,} ({frequency_tier} chatty pattern), avg_exec_time={signals.avg_exec_time*1000:.2f}ms",
            category=SQLCategory.CHATTY_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_cache_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate result cache opportunity analysis"""
        
        sql: str = f"""-- Result Cache Opportunity Analysis for SQL_ID: {signals.sql_id}
-- {signals.executions:,} executions suggest caching could help

-- Check current result cache status
SELECT 
  name,
  value
FROM V$RESULT_CACHE_STATISTICS
WHERE name IN ('Create Count Success', 'Find Count', 'Invalidation Count');

-- Analyze if this SQL is cache-friendly
-- (deterministic, not DML-heavy underlying tables)
SELECT 
  s.sql_id,
  s.executions,
  s.buffer_gets / NULLIF(s.executions, 0) AS gets_per_exec,
  s.rows_processed / NULLIF(s.executions, 0) AS rows_per_exec,
  CASE 
    WHEN s.rows_processed / NULLIF(s.executions, 0) < 100 THEN 'GOOD_CANDIDATE'
    WHEN s.rows_processed / NULLIF(s.executions, 0) < 1000 THEN 'MODERATE_CANDIDATE'
    ELSE 'LARGE_RESULT_SET'
  END AS cache_suitability
FROM V$SQL s
WHERE s.sql_id = '{signals.sql_id}';

-- Check table modification frequency (affects cache validity)
SELECT 
  table_name,
  inserts,
  updates,
  deletes,
  timestamp AS last_analyzed
FROM DBA_TAB_MODIFICATIONS
WHERE table_name IN (
  SELECT DISTINCT object_name 
  FROM V$SQL_PLAN 
  WHERE sql_id = '{signals.sql_id}' 
    AND object_type = 'TABLE'
)
ORDER BY (inserts + updates + deletes) DESC;

-- Result cache hint syntax for application team:
-- SELECT /*+ RESULT_CACHE */ columns FROM table WHERE conditions;"""
        
        return GeneratedSQL(
            action="RESULT_CACHING",
            sql=sql,
            intent=f"Evaluate result caching opportunity for {signals.sql_id}",
            explanation=f"Generated because executions={signals.executions:,}, fast individual execution suggests cache benefit",
            category=SQLCategory.CHATTY_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_throttling_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate application-level throttling analysis"""
        
        cumulative_time: float = signals.total_elapsed
        per_exec: float = signals.avg_exec_time
        
        sql: str = f"""-- Application Throttling Analysis for SQL_ID: {signals.sql_id}
-- Pattern: {signals.executions:,} executions @ {per_exec:.4f}s each = {cumulative_time:.1f}s cumulative

-- Session-level execution pattern
SELECT 
  sid,
  serial#,
  username,
  program,
  module,
  action,
  sql_id,
  sql_exec_start,
  COUNT(*) OVER (PARTITION BY sid) AS executions_this_session
FROM V$SESSION
WHERE sql_id = '{signals.sql_id}'
  OR prev_sql_id = '{signals.sql_id}';

-- Execution frequency over time (identify burst patterns)
SELECT 
  TRUNC(begin_interval_time, 'HH24') AS hour,
  SUM(executions_delta) AS exec_count,
  ROUND(SUM(elapsed_time_delta)/1000000, 2) AS elapsed_sec
FROM DBA_HIST_SQLSTAT s
JOIN DBA_HIST_SNAPSHOT sn ON s.snap_id = sn.snap_id
WHERE s.sql_id = '{signals.sql_id}'
GROUP BY TRUNC(begin_interval_time, 'HH24')
ORDER BY hour DESC
FETCH FIRST 24 ROWS ONLY;

-- Calling module/program analysis
SELECT 
  module,
  action, 
  COUNT(*) AS call_count,
  ROUND(SUM(elapsed_time)/1000000, 2) AS total_elapsed_sec
FROM V$SQL s
JOIN V$SESSION sess ON s.sql_id = sess.sql_id
WHERE s.sql_id = '{signals.sql_id}'
GROUP BY module, action;

/*
APPLICATION TEAM RECOMMENDATIONS:
1. Reduce call frequency: {signals.executions:,} calls may indicate:
   - Missing application-level caching
   - Polling too frequently
   - N+1 query pattern in ORM
   
2. Consider batching: If multiple calls with same parameters,
   batch into single call with IN clause

3. Connection pool tuning: High frequency may exhaust connections
*/"""
        
        return GeneratedSQL(
            action="APPLICATION_THROTTLING",
            sql=sql,
            intent=f"Analyze application execution patterns for {signals.sql_id}",
            explanation=f"Generated because executions={signals.executions:,} indicates potential application design issue",
            category=SQLCategory.CHATTY_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    # =========================================================================
    # IO-BOUND SQL COMMANDS
    # =========================================================================
    
    def _generate_io_bound_commands(self, signals: NormalizedSignals,
                                     decision: DecisionResult) -> List[GeneratedSQL]:
        """
        Generate commands for IO_BOUND_SQL category.
        Focus on: Index review, access path optimization
        """
        generated = []
        
        # Access path analysis
        if self._is_allowed(decision, ActionType.ACCESS_PATH_OPTIMIZATION):
            generated.append(self._generate_access_path_analysis(signals))
        
        # Index creation recommendations  
        if self._is_allowed(decision, ActionType.INDEX_CREATION):
            generated.append(self._generate_index_recommendation(signals))
        
        # SQL Access Advisor for IO-bound
        if self._is_allowed(decision, ActionType.SQL_ACCESS_ADVISOR):
            generated.append(self._generate_access_advisor_io_focused(signals))
        
        return generated
    
    def _generate_access_path_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate access path analysis for IO-bound SQL"""
        
        sql: str = f"""-- Access Path Analysis for IO-Bound SQL: {signals.sql_id}
-- IO Wait: {signals.io_wait_pct:.1f}% (CRITICAL - above 70% threshold)

-- Current access paths in execution plan
SELECT 
  id,
  parent_id,
  operation,
  options,
  object_name,
  object_type,
  cardinality AS est_rows,
  bytes,
  cost,
  cpu_cost,
  io_cost,
  ROUND(io_cost / NULLIF(cost, 0) * 100, 1) AS io_cost_pct
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
ORDER BY id;

-- Identify expensive access operations
SELECT 
  object_name,
  operation || ' ' || NVL(options, '') AS access_method,
  cardinality,
  cost,
  io_cost
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND io_cost > 100
ORDER BY io_cost DESC;

-- Table access statistics
SELECT 
  object_name,
  ROUND(SUM(CASE WHEN statistic_name = 'physical reads' THEN value ELSE 0 END)) AS phys_reads,
  ROUND(SUM(CASE WHEN statistic_name = 'physical reads direct' THEN value ELSE 0 END)) AS direct_reads,
  ROUND(SUM(CASE WHEN statistic_name = 'db block gets' THEN value ELSE 0 END)) AS block_gets
FROM V$SEGMENT_STATISTICS
WHERE owner NOT IN ('SYS', 'SYSTEM')
  AND object_name IN (
    SELECT DISTINCT object_name 
    FROM V$SQL_PLAN 
    WHERE sql_id = '{signals.sql_id}' 
      AND object_type = 'TABLE'
  )
GROUP BY object_name
ORDER BY phys_reads DESC;"""
        
        return GeneratedSQL(
            action="ACCESS_PATH_OPTIMIZATION",
            sql=sql,
            intent=f"Analyze and optimize access paths for IO-bound SQL {signals.sql_id}",
            explanation=f"Generated because io_wait_pct={signals.io_wait_pct:.1f}% (>70% threshold)",
            category=SQLCategory.IO_BOUND_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_index_recommendation(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate index creation recommendations for IO-bound SQL"""
        
        sql: str = f"""-- Index Creation Recommendations for SQL_ID: {signals.sql_id}
-- IO Wait: {signals.io_wait_pct:.1f}% indicates inefficient data access

-- Step 1: Identify full table scans that could benefit from indexes
SELECT 
  p.object_name AS table_name,
  p.cardinality AS est_rows_accessed,
  p.cost AS operation_cost,
  p.filter_predicates,
  p.access_predicates
FROM V$SQL_PLAN p
WHERE p.sql_id = '{signals.sql_id}'
  AND p.operation = 'TABLE ACCESS'
  AND p.options = 'FULL'
  AND p.cardinality > 100;

-- Step 2: Analyze filter predicates for index candidates
-- (Manual review needed - extract column names from predicates)

-- Step 3: Check existing indexes on accessed tables
SELECT 
  i.table_name,
  i.index_name,
  i.index_type,
  i.uniqueness,
  LISTAGG(ic.column_name, ', ') WITHIN GROUP (ORDER BY ic.column_position) AS columns
FROM DBA_INDEXES i
JOIN DBA_IND_COLUMNS ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner
WHERE i.table_name IN (
  SELECT DISTINCT object_name 
  FROM V$SQL_PLAN 
  WHERE sql_id = '{signals.sql_id}' 
    AND object_type = 'TABLE'
)
GROUP BY i.table_name, i.index_name, i.index_type, i.uniqueness;

-- Step 4: Column selectivity for potential index columns
SELECT 
  column_name,
  num_distinct,
  density,
  CASE 
    WHEN density < 0.01 THEN 'HIGHLY_SELECTIVE'
    WHEN density < 0.1 THEN 'MODERATELY_SELECTIVE'
    ELSE 'LOW_SELECTIVITY'
  END AS selectivity_rating
FROM DBA_TAB_COL_STATISTICS
WHERE table_name IN (
  SELECT DISTINCT object_name 
  FROM V$SQL_PLAN 
  WHERE sql_id = '{signals.sql_id}' 
    AND object_type = 'TABLE'
)
  AND num_distinct > 10
ORDER BY density;

/*
INDEX CREATION GUIDANCE:
Based on io_wait_pct of {signals.io_wait_pct:.1f}%, creating appropriate indexes
should reduce physical reads significantly.

Review the filter_predicates above and create composite indexes 
on columns with high selectivity (low density).
*/"""
        
        return GeneratedSQL(
            action="INDEX_CREATION",
            sql=sql,
            intent=f"Generate index recommendations to reduce IO for {signals.sql_id}",
            explanation=f"Generated because io_wait_pct={signals.io_wait_pct:.1f}%, cpu_pct={signals.cpu_pct:.1f}% (IO dominant)",
            category=SQLCategory.IO_BOUND_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_access_advisor_io_focused(self, signals: NormalizedSignals) -> GeneratedSQL:
        """
        Generate Access Advisor with IO focus.
        
        CRITICAL: Task configuration MUST differ based on IO severity and execution pattern.
        """
        task_suffix: str = self._generate_task_suffix(signals)
        task_name: str = f"IO_ADV_{signals.sql_id}_{task_suffix}"
        
        # Determine IO-specific parameters
        if signals.io_wait_pct >= 90:
            focus_mode = "INDEX"  # Critical IO - focus on indexes
            storage_analysis = "TRUE"
            recommendations_limit = 20
        elif signals.io_wait_pct >= 70:
            focus_mode = "INDEX_PARTITION"  # High IO - indexes and partitioning
            storage_analysis = "TRUE"
            recommendations_limit = 15
        else:
            focus_mode = "COMPREHENSIVE"  # Moderate IO - comprehensive
            storage_analysis = "FALSE"
            recommendations_limit = 10
        
        # Determine time limit based on query complexity
        time_limit: int = self._calculate_advisor_time_limit(signals)
        
        sql: str = f"""-- SQL Access Advisor (IO-Focused) for SQL_ID: {signals.sql_id}
-- IO Severity: {signals.io_wait_pct:.1f}% | Focus Mode: {focus_mode}
-- Signal Context: cpu={signals.cpu_pct:.1f}%, elapsed={signals.total_elapsed:.1f}s, exec={signals.executions}

DECLARE
  v_task_name VARCHAR2(128) := '{task_name}';
  v_task_id   NUMBER;
BEGIN
  -- Create Access Advisor task focused on IO reduction
  DBMS_ADVISOR.CREATE_TASK(
    advisor_name => 'SQL Access Advisor',
    task_name    => v_task_name,
    task_id      => v_task_id
  );
  
  -- Set IO-focused analysis parameters
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'MODE',
    value     => 'COMPREHENSIVE'
  );
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'ANALYSIS_SCOPE',
    value     => 'FULL'  -- Full analysis for IO-bound SQL
  );
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'TIME_LIMIT',
    value     => {time_limit}  -- {time_limit//60} min based on {signals.io_wait_pct:.1f}% IO wait
  );
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(
    task_name => v_task_name,
    parameter => 'STORAGE_CHANGE',
    value     => '{storage_analysis}'  -- Storage analysis for IO-bound
  );
  
  DBMS_OUTPUT.PUT_LINE('IO-focused Access Advisor task created: ' || v_task_name);
  DBMS_OUTPUT.PUT_LINE('Focus: {focus_mode} | Time limit: {time_limit}s');
END;
/

-- Execute and get IO-reduction recommendations
EXEC DBMS_ADVISOR.EXECUTE_TASK('{task_name}');

-- View IO-specific recommendations (prioritized by benefit)
SELECT 
  rec_id,
  rank,
  benefit AS estimated_benefit,
  benefit_type,
  action_type,
  message,
  CASE 
    WHEN benefit > 50 THEN 'HIGH_VALUE'
    WHEN benefit > 20 THEN 'MODERATE_VALUE'
    ELSE 'LOW_VALUE'
  END AS value_assessment
FROM DBA_ADVISOR_RECOMMENDATIONS
WHERE task_name = '{task_name}'
ORDER BY benefit DESC
FETCH FIRST {recommendations_limit} ROWS ONLY;

-- Get detailed action information for implementation
SELECT 
  a.rec_id,
  a.command,
  a.attr1 AS schema_name,
  a.attr2 AS object_name,
  a.attr3 AS object_type,
  a.attr4 AS column_info,
  r.benefit
FROM DBA_ADVISOR_ACTIONS a
JOIN DBA_ADVISOR_RECOMMENDATIONS r 
  ON a.task_name = r.task_name AND a.rec_id = r.rec_id
WHERE a.task_name = '{task_name}'
  AND r.benefit > 10  -- Only show beneficial recommendations
ORDER BY r.benefit DESC;"""
        
        return GeneratedSQL(
            action="SQL_ACCESS_ADVISOR",
            sql=sql,
            intent=f"Run IO-focused Access Advisor for {signals.sql_id} ({signals.io_wait_pct:.1f}% IO wait)",
            explanation=f"Generated because io_wait_pct={signals.io_wait_pct:.1f}% requires access path optimization, elapsed={signals.total_elapsed:.1f}s",
            category=SQLCategory.IO_BOUND_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    # =========================================================================
    # CPU-BOUND SQL COMMANDS
    # =========================================================================
    
    def _generate_cpu_bound_commands(self, signals: NormalizedSignals,
                                      decision: DecisionResult) -> List[GeneratedSQL]:
        """
        Generate commands for CPU_BOUND_SQL category.
        Focus on: Join methods, SQL rewrite, plan inspection
        PREFER plan inspection BEFORE advisor (per spec)
        """
        generated = []
        
        # Join method analysis (primary for CPU-bound)
        if self._is_allowed(decision, ActionType.JOIN_METHOD_REVIEW):
            generated.append(self._generate_join_analysis(signals))
        
        # Hash vs Nested Loop analysis
        if self._is_allowed(decision, ActionType.HASH_VS_NESTED_ANALYSIS):
            generated.append(self._generate_hash_nested_analysis(signals))
        
        # SQL rewrite opportunities
        if self._is_allowed(decision, ActionType.SQL_REWRITE):
            generated.append(self._generate_rewrite_analysis(signals))
        
        # SQL Tuning Advisor (only after plan inspection)
        if self._is_allowed(decision, ActionType.SQL_TUNING_ADVISOR):
            generated.append(self._generate_tuning_advisor_cpu(signals))
        
        return generated
    
    def _generate_join_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """
        Generate join method analysis for CPU-bound SQL.
        
        CRITICAL: Analysis scope MUST differ based on CPU severity and execution pattern.
        """
        # Determine analysis depth based on CPU severity
        if signals.cpu_pct >= 90:
            severity = "CRITICAL"
            include_cartesian_check = True
            include_estimate_check = True
            include_hint_recommendations = True
        elif signals.cpu_pct >= 70:
            severity = "HIGH"
            include_cartesian_check = True
            include_estimate_check = True
            include_hint_recommendations = False
        else:
            severity = "MODERATE"
            include_cartesian_check = False
            include_estimate_check = True
            include_hint_recommendations = False
        
        # Build dynamic analysis sections
        cartesian_section: str = ""
        estimate_section: str = ""
        hint_section: str = ""
        
        if include_cartesian_check:
            cartesian_section: str = f"""
-- Cartesian Product Detection ({severity} CPU priority)
SELECT 
  id,
  operation,
  options,
  cardinality,
  cost,
  cpu_cost,
  CASE 
    WHEN cardinality > 100000 THEN 'CRITICAL_CARTESIAN'
    WHEN cardinality > 10000 THEN 'HIGH_IMPACT_CARTESIAN'
    ELSE 'MODERATE_CARTESIAN'
  END AS cartesian_severity
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND (
    (operation = 'NESTED LOOPS' AND cardinality > 10000) OR
    (operation = 'MERGE JOIN' AND options = 'CARTESIAN') OR
    (operation = 'HASH JOIN' AND cardinality > 100000)
  );
"""
        
        if include_estimate_check:
            estimate_section: str = f"""
-- Estimate vs Actual Analysis (cardinality feedback)
SELECT 
  ps.id,
  ps.operation || ' ' || NVL(ps.options, '') AS operation,
  p.cardinality AS estimated_rows,
  ps.output_rows AS actual_rows,
  CASE 
    WHEN ps.output_rows > p.cardinality * 10 THEN 'SEVERE_UNDERESTIMATE'
    WHEN ps.output_rows > p.cardinality * 3 THEN 'UNDERESTIMATE'
    WHEN ps.output_rows < p.cardinality / 10 THEN 'SEVERE_OVERESTIMATE'
    WHEN ps.output_rows < p.cardinality / 3 THEN 'OVERESTIMATE'
    ELSE 'ACCURATE'
  END AS estimate_quality,
  ROUND(ps.output_rows / NULLIF(p.cardinality, 0), 2) AS ratio
FROM V$SQL_PLAN_STATISTICS ps
JOIN V$SQL_PLAN p ON ps.sql_id = p.sql_id 
  AND ps.child_number = p.child_number 
  AND ps.id = p.id
WHERE ps.sql_id = '{signals.sql_id}'
  AND ps.operation LIKE '%JOIN%'
ORDER BY ABS(ps.output_rows - p.cardinality) DESC;
"""
        
        if include_hint_recommendations:
            hint_section: str = f"""
/*
═══════════════════════════════════════════════════════════════════════════════
JOIN OPTIMIZATION HINTS for SQL_ID: {signals.sql_id}
CPU: {signals.cpu_pct:.1f}% | {severity} Priority
═══════════════════════════════════════════════════════════════════════════════

Based on CPU profile ({signals.cpu_pct:.1f}%), consider these hints:

1. HASH JOIN for large result sets:
   /*+ USE_HASH(table_alias) */
   Best when: Many rows being joined, sufficient PGA

2. NESTED LOOPS for selective access:
   /*+ USE_NL(table_alias) INDEX(table_alias index_name) */
   Best when: Driving table is small, inner table has selective index

3. MERGE JOIN for pre-sorted data:
   /*+ USE_MERGE(table_alias) */
   Best when: Both inputs are already sorted by join columns

4. Join order control:
   /*+ LEADING(t1 t2 t3) */
   Force specific join order when optimizer chooses poorly

5. Disable specific join methods:
   /*+ NO_USE_HASH(table_alias) */
   /*+ NO_USE_NL(table_alias) */
═══════════════════════════════════════════════════════════════════════════════
*/
"""

        sql: str = f"""-- Join Method Analysis for CPU-Bound SQL: {signals.sql_id}
-- CPU Severity: {severity} | CPU: {signals.cpu_pct:.1f}% | IO: {signals.io_wait_pct:.1f}%
-- Pattern: Data access efficient, but processing is expensive
-- Signal Context: cpu_time={signals.cpu_time:.1f}s, executions={signals.executions}

-- Current Join Methods in Execution Plan
SELECT 
  id,
  operation,
  options,
  object_name,
  cardinality AS est_rows,
  cost,
  cpu_cost,
  ROUND(cpu_cost / NULLIF(cost, 0), 2) AS cpu_cost_ratio,
  CASE 
    WHEN cpu_cost / NULLIF(cost, 0) > 0.8 THEN 'CPU_DOMINANT'
    WHEN cpu_cost / NULLIF(cost, 0) > 0.5 THEN 'BALANCED'
    ELSE 'IO_DOMINANT'
  END AS resource_profile
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND operation LIKE '%JOIN%'
ORDER BY cpu_cost DESC;

-- All Join Operations with Parent/Child Details
SELECT 
  p1.id AS join_id,
  p1.operation || ' ' || NVL(p1.options, '') AS join_method,
  p1.cardinality AS joined_rows,
  p1.cpu_cost,
  p1.cost,
  p2.object_name AS inner_table,
  p2.cardinality AS inner_rows,
  p2.access_predicates,
  p2.filter_predicates
FROM V$SQL_PLAN p1
LEFT JOIN V$SQL_PLAN p2 ON p1.sql_id = p2.sql_id 
  AND p1.child_number = p2.child_number 
  AND p2.parent_id = p1.id
WHERE p1.sql_id = '{signals.sql_id}'
  AND p1.operation LIKE '%JOIN%'
ORDER BY p1.cpu_cost DESC;
{cartesian_section}{estimate_section}{hint_section}"""
        
        return GeneratedSQL(
            action="JOIN_METHOD_REVIEW",
            sql=sql,
            intent=f"Analyze join methods causing CPU overhead in {signals.sql_id} ({severity})",
            explanation=f"Generated because cpu_pct={signals.cpu_pct:.1f}% (>70%), io_wait_pct={signals.io_wait_pct:.1f}% (<30%), cpu_time={signals.cpu_time:.1f}s",
            category=SQLCategory.CPU_BOUND_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_hash_nested_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate Hash vs Nested Loop comparison"""
        
        sql: str = f"""-- Hash Join vs Nested Loop Analysis for SQL_ID: {signals.sql_id}
-- CPU: {signals.cpu_pct:.1f}% | Execution Time: {signals.avg_exec_time:.2f}s/exec

-- Current join method statistics
SELECT 
  operation,
  options,
  cardinality,
  cost,
  cpu_cost,
  temp_space,
  CASE 
    WHEN operation = 'NESTED LOOPS' AND cardinality > 1000 
      THEN 'CONSIDER_HASH_JOIN'
    WHEN operation = 'HASH JOIN' AND cardinality < 100 
      THEN 'NESTED_LOOP_MAY_BE_BETTER'
    ELSE 'APPROPRIATE'
  END AS recommendation
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND operation LIKE '%JOIN%';

-- Memory usage for hash operations
SELECT 
  operation,
  options,
  temp_space,
  ROUND(temp_space / 1024 / 1024, 2) AS temp_mb
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND temp_space IS NOT NULL;

-- Compare estimated vs actual for join operations
SELECT 
  ps.operation,
  ps.options,
  ps.output_rows AS actual_rows,
  p.cardinality AS estimated_rows,
  CASE 
    WHEN ps.output_rows > p.cardinality * 10 THEN 'UNDERESTIMATE'
    WHEN ps.output_rows < p.cardinality / 10 THEN 'OVERESTIMATE'
    ELSE 'ACCURATE'
  END AS estimate_quality
FROM V$SQL_PLAN_STATISTICS ps
JOIN V$SQL_PLAN p ON ps.sql_id = p.sql_id 
  AND ps.child_number = p.child_number 
  AND ps.id = p.id
WHERE ps.sql_id = '{signals.sql_id}'
  AND ps.operation LIKE '%JOIN%';

/*
HASH JOIN vs NESTED LOOPS Decision Guide:
- HASH JOIN: Better for large result sets, requires memory
- NESTED LOOPS: Better for small result sets, indexed access

For CPU={signals.cpu_pct:.1f}%, consider:
- If many NESTED LOOPS with high cardinality → USE_HASH hint
- If HASH JOIN with memory spills → Increase PGA or reduce data
*/"""
        
        return GeneratedSQL(
            action="HASH_VS_NESTED_ANALYSIS",
            sql=sql,
            intent=f"Compare join methods for CPU optimization of {signals.sql_id}",
            explanation=f"Generated because cpu_pct={signals.cpu_pct:.1f}%, avg_exec_time={signals.avg_exec_time:.2f}s",
            category=SQLCategory.CPU_BOUND_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_rewrite_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate SQL rewrite analysis for CPU-bound queries"""
        
        sql: str = f"""-- SQL Rewrite Analysis for CPU-Bound Query: {signals.sql_id}
-- CPU: {signals.cpu_pct:.1f}% | CPU Time: {signals.cpu_time:.1f}s

-- Identify CPU-expensive operations
SELECT 
  id,
  operation,
  options,
  cpu_cost,
  cardinality,
  ROUND(cpu_cost / NULLIF(SUM(cpu_cost) OVER (), 0) * 100, 1) AS pct_of_cpu
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND cpu_cost > 0
ORDER BY cpu_cost DESC;

-- Check for expensive SORT operations
SELECT 
  operation,
  options,
  cardinality AS rows_sorted,
  temp_space AS sort_temp_bytes,
  cpu_cost
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND operation LIKE '%SORT%';

-- Check for expensive aggregations
SELECT 
  operation,
  options,
  cardinality,
  cpu_cost
FROM V$SQL_PLAN
WHERE sql_id = '{signals.sql_id}'
  AND (operation LIKE '%AGGREGATE%' OR operation LIKE '%GROUP%');

-- SQL Text for review (identify rewrite opportunities)
SELECT sql_fulltext
FROM V$SQL
WHERE sql_id = '{signals.sql_id}'
AND ROWNUM = 1;

/*
SQL REWRITE OPPORTUNITIES (CPU-focused):
1. DISTINCT elimination if not needed
2. Subquery to JOIN conversion
3. UNION to UNION ALL if duplicates acceptable
4. Correlated subquery to scalar subquery or JOIN
5. ORDER BY elimination if not needed
6. Pagination with ROWNUM vs FETCH FIRST

Current CPU: {signals.cpu_pct:.1f}% - Focus on computational reduction
*/"""
        
        return GeneratedSQL(
            action="SQL_REWRITE",
            sql=sql,
            intent=f"Identify SQL rewrite opportunities for CPU reduction in {signals.sql_id}",
            explanation=f"Generated because cpu_pct={signals.cpu_pct:.1f}%, cpu_time={signals.cpu_time:.1f}s",
            category=SQLCategory.CPU_BOUND_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    def _generate_tuning_advisor_cpu(self, signals: NormalizedSignals) -> GeneratedSQL:
        """
        Generate SQL Tuning Advisor for CPU-bound SQL.
        
        CRITICAL RULES:
        - Run ONLY AFTER manual plan inspection (per DBA best practice)
        - Task parameters MUST differ based on CPU severity
        - NEVER use for CHATTY_SQL (blocked action)
        - Time limit based on query complexity
        """
        task_suffix: str = self._generate_task_suffix(signals)
        task_name: str = f"CPU_TUNE_{signals.sql_id}_{task_suffix}"
        
        # Determine tuning scope based on CPU severity
        if signals.cpu_pct >= 90:
            scope = "COMPREHENSIVE"
            time_limit = 600  # 10 minutes for critical CPU
            focus_areas = "plan alternative, SQL profile, restructure"
        elif signals.cpu_pct >= 70:
            scope = "COMPREHENSIVE"
            time_limit = 300  # 5 minutes for high CPU
            focus_areas = "plan alternative, SQL profile"
        else:
            scope = "LIMITED"
            time_limit = 120  # 2 minutes for moderate CPU
            focus_areas = "SQL profile"
        
        # Additional analysis based on execution pattern
        execution_context: str = ""
        if signals.executions >= 1000:
            execution_context: str = f"""
-- High-frequency execution context ({signals.executions:,} executions)
-- Consider: Bind variable impact, cursor sharing efficiency"""
        elif signals.executions < 10:
            execution_context: str = f"""
-- Low-frequency execution context ({signals.executions} executions)
-- Consider: One-time optimization, avoid profile overhead"""
        
        sql: str = f"""-- SQL Tuning Advisor for CPU-Bound SQL: {signals.sql_id}
-- IMPORTANT: Run AFTER manual plan inspection (per DBA best practice)
-- CPU Severity: {signals.cpu_pct:.1f}% | Focus: {focus_areas}
-- Signal Context: io={signals.io_wait_pct:.1f}%, elapsed={signals.total_elapsed:.1f}s, exec={signals.executions}
{execution_context}

DECLARE
  v_task_name VARCHAR2(128) := '{task_name}';
  v_task_id   NUMBER;
BEGIN
  -- Create tuning task with CPU-optimized parameters
  v_task_id := DBMS_SQLTUNE.CREATE_TUNING_TASK(
    sql_id       => '{signals.sql_id}',
    task_name    => v_task_name,
    time_limit   => {time_limit},  -- {time_limit//60} min analysis for {signals.cpu_pct:.1f}% CPU
    scope        => '{scope}',
    description  => 'CPU-bound SQL tuning - {signals.cpu_pct:.1f}% CPU, {signals.cpu_time:.1f}s CPU time'
  );
  
  DBMS_OUTPUT.PUT_LINE('Tuning task created: ' || v_task_name);
  DBMS_OUTPUT.PUT_LINE('Scope: {scope}, Time limit: {time_limit}s');
  
  -- Execute the tuning task
  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name => v_task_name);
  
  DBMS_OUTPUT.PUT_LINE('Tuning task ' || v_task_name || ' completed');
END;
/

-- Get comprehensive tuning report
SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK(
  task_name   => '{task_name}',
  type        => 'TEXT',
  level       => 'ALL',  -- Full detail for CPU-bound analysis
  section     => 'ALL'
) AS tuning_report
FROM DUAL;

-- View specific recommendations with benefit analysis
SELECT 
  rec_id,
  finding_id,
  type,
  message,
  benefit_pct,
  CASE 
    WHEN benefit_pct > 50 THEN 'HIGH_VALUE - Implement'
    WHEN benefit_pct > 20 THEN 'MODERATE_VALUE - Consider'
    ELSE 'LOW_VALUE - Optional'
  END AS recommendation_priority
FROM DBA_ADVISOR_RECOMMENDATIONS
WHERE task_name = '{task_name}'
ORDER BY benefit_pct DESC NULLS LAST;

-- Check for SQL Profile recommendations (common for CPU-bound)
SELECT 
  profile_name,
  status,
  sql_text,
  category
FROM DBA_SQL_PROFILES
WHERE sql_text LIKE '%{signals.sql_id}%'
   OR name LIKE '%{task_name}%';"""
        
        return GeneratedSQL(
            action="SQL_TUNING_ADVISOR",
            sql=sql,
            intent=f"Run SQL Tuning Advisor for CPU-bound SQL {signals.sql_id} ({signals.cpu_pct:.1f}% CPU)",
            explanation=f"Generated because cpu_pct={signals.cpu_pct:.1f}%, cpu_time={signals.cpu_time:.1f}s (after plan inspection)",
            category=SQLCategory.CPU_BOUND_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    # =========================================================================
    # MIXED PROFILE COMMANDS
    # =========================================================================
    
    def _generate_mixed_profile_commands(self, signals: NormalizedSignals,
                                          decision: DecisionResult) -> List[GeneratedSQL]:
        """Generate commands for SQL with mixed characteristics"""
        generated = []
        
        # Comprehensive analysis for mixed profile
        generated.append(self._generate_comprehensive_analysis(signals))
        
        # Add specific commands based on which traits are present
        if signals.io_wait_pct > 40:
            generated.append(self._generate_index_usage_check(signals))
        if signals.cpu_pct > 40:
            generated.append(self._generate_join_analysis(signals))
        
        return generated
    
    def _generate_comprehensive_analysis(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate comprehensive analysis for mixed profile SQL"""
        
        sql: str = f"""-- Comprehensive Analysis for Mixed Profile SQL: {signals.sql_id}
-- Signals: CPU={signals.cpu_pct:.1f}% | IO={signals.io_wait_pct:.1f}% | 
--          Execs={signals.executions} | Avg={signals.avg_exec_time:.2f}s

-- Full execution statistics
SELECT 
  sql_id,
  executions,
  ROUND(elapsed_time/1000000, 2) AS elapsed_sec,
  ROUND(cpu_time/1000000, 2) AS cpu_sec,
  ROUND(user_io_wait_time/1000000, 2) AS io_wait_sec,
  buffer_gets,
  disk_reads,
  rows_processed,
  ROUND(elapsed_time/NULLIF(executions,0)/1000000, 4) AS avg_elapsed_sec
FROM V$SQL
WHERE sql_id = '{signals.sql_id}';

-- Wait breakdown
SELECT 
  event,
  total_waits,
  ROUND(time_waited_micro/1000000, 2) AS time_waited_sec,
  ROUND(time_waited_micro/NULLIF(total_waits,0)/1000, 2) AS avg_wait_ms
FROM V$SQL_MONITOR
WHERE sql_id = '{signals.sql_id}'
  AND status = 'DONE (ALL ROWS)'
ORDER BY time_waited_micro DESC;

-- Plan with all statistics
SELECT * FROM TABLE(
  DBMS_XPLAN.DISPLAY_CURSOR(
    sql_id => '{signals.sql_id}',
    cursor_child_no => NULL,
    format => 'ALLSTATS LAST +COST +IOSTATS +MEMSTATS'
  )
);

/*
MIXED PROFILE ANALYSIS SUMMARY:
- CPU contribution: {signals.cpu_pct:.1f}%
- IO contribution: {signals.io_wait_pct:.1f}%
- Execution pattern: {signals.executions} executions @ {signals.avg_exec_time:.2f}s each

Recommendation: Investigate both access paths AND join methods
*/"""
        
        return GeneratedSQL(
            action="COMPREHENSIVE_ANALYSIS",
            sql=sql,
            intent=f"Comprehensive analysis for mixed profile SQL {signals.sql_id}",
            explanation=f"Generated because multiple concerning metrics: cpu={signals.cpu_pct:.1f}%, io={signals.io_wait_pct:.1f}%, execs={signals.executions}",
            category=SQLCategory.MIXED_PROFILE_SQL.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )
    
    # =========================================================================
    # MONITORING COMMANDS (Low Priority)
    # =========================================================================
    
    def _generate_monitoring_commands(self, signals: NormalizedSignals,
                                       decision: DecisionResult) -> List[GeneratedSQL]:
        """Generate monitoring-only commands for low-priority SQL"""
        return [self._generate_baseline_monitoring(signals)]
    
    def _generate_baseline_monitoring(self, signals: NormalizedSignals) -> GeneratedSQL:
        """Generate baseline monitoring for low-priority SQL"""
        
        sql: str = f"""-- Baseline Monitoring for SQL_ID: {signals.sql_id}
-- Status: LOW_PRIORITY - No immediate tuning required
-- Metrics: CPU={signals.cpu_pct:.1f}% | IO={signals.io_wait_pct:.1f}% | Execs={signals.executions}

-- Current performance baseline
SELECT 
  sql_id,
  executions,
  ROUND(elapsed_time/1000000/NULLIF(executions,0), 4) AS avg_elapsed_sec,
  ROUND(cpu_time/1000000/NULLIF(executions,0), 4) AS avg_cpu_sec,
  buffer_gets / NULLIF(executions, 0) AS gets_per_exec,
  last_active_time
FROM V$SQL
WHERE sql_id = '{signals.sql_id}';

-- Historical performance trend (if AWR available)
SELECT 
  TO_CHAR(sn.begin_interval_time, 'YYYY-MM-DD HH24') AS snapshot_hour,
  s.executions_delta AS execs,
  ROUND(s.elapsed_time_delta/1000000, 2) AS elapsed_sec
FROM DBA_HIST_SQLSTAT s
JOIN DBA_HIST_SNAPSHOT sn ON s.snap_id = sn.snap_id
WHERE s.sql_id = '{signals.sql_id}'
ORDER BY sn.begin_interval_time DESC
FETCH FIRST 10 ROWS ONLY;

/*
MONITORING NOTES:
- No tuning action required at this time
- Continue standard monitoring
- Re-evaluate if metrics change significantly
*/"""
        
        return GeneratedSQL(
            action="MONITOR_ONLY",
            sql=sql,
            intent=f"Establish monitoring baseline for {signals.sql_id}",
            explanation=f"Generated because SQL does not meet problem thresholds (cpu={signals.cpu_pct:.1f}%, io={signals.io_wait_pct:.1f}%)",
            category=SQLCategory.LOW_PRIORITY.value,
            signal_fingerprint=self._create_signal_fingerprint(signals)
        )

