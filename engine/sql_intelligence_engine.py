"""
SQL Text Intelligence Engine - Phase 2 + 3 Intelligent Upgrade

This module adds intelligent SQL text analysis on top of the existing RCA system.
It analyzes SQL patterns and provides targeted, realistic DBA-level recommendations.
"""

import re
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any


class SQLIntelligenceEngine:
    """
    Analyzes SQL text patterns and provides intelligent recommendations
    based on actual SQL content combined with performance metrics.
    """
    
    def __init__(self, csv_data: Dict[str, Any]):
        """Initialize with AWR CSV data"""
        self.csv_data = csv_data
        self.sql_stats = csv_data.get("sql_stats")
        self.wait_events = csv_data.get("wait_events") 
        self.instance_stats = csv_data.get("instance_stats")
        
        # Prepare SQL stats data if available
        if self.sql_stats is not None:
            self.sql_stats.columns = self.sql_stats.columns.str.strip().str.lower()
            self._clean_sql_data()

    def _clean_sql_data(self):
        """Clean and prepare SQL data for analysis"""
        # Ensure numeric columns are properly converted
        numeric_cols = ['elapsed__time_s', 'executions', 'elapsed_time_per_exec_s', 
                       'pcttotal', 'pctcpu', 'pctio']
        
        for col in numeric_cols:
            if col in self.sql_stats.columns:
                self.sql_stats[col] = pd.to_numeric(self.sql_stats[col], errors='coerce').fillna(0)

    def analyze_sql_patterns(self, sql_id: str, sql_text: str, metrics: Dict) -> Dict[str, Any]:
        """
        Main intelligence analysis method.
        Analyzes SQL text patterns and combines with performance metrics.
        """
        if not sql_text or pd.isna(sql_text):
            return self._create_default_analysis(sql_id, metrics)
        
        # 1. Detect SQL patterns
        patterns = self._detect_sql_patterns(sql_text)
        
        # 2. Analyze metrics context
        metric_context = self._analyze_metrics_context(metrics)
        
        # 3. Apply condition-meaning-action brain logic
        intelligence_result = self._apply_intelligence_brain(
            sql_id, sql_text, patterns, metric_context, metrics
        )
        
        return intelligence_result

    def _detect_sql_patterns(self, sql_text: str) -> Dict[str, bool]:
        """
        🧩 Detect SQL Patterns - Intelligent pattern recognition
        """
        sql_upper = sql_text.upper()
        patterns = {}
        
        # 1. Full Table Scan possibility
        patterns['full_table_scan'] = (
            'SELECT * FROM' in sql_upper or
            'COUNT(*)' in sql_upper or
            (not re.search(r'WHERE\s+\w+\s*=', sql_upper, re.IGNORECASE) and 
             'SELECT' in sql_upper and 'FROM' in sql_upper)
        )
        
        # 2. Too many joins (3+ tables)
        join_count = len(re.findall(r'\bJOIN\b|\bINNER\b|\bLEFT\b|\bRIGHT\b|\bOUTER\b', sql_upper))
        from_tables = len(re.findall(r'FROM\s+(\w+)', sql_upper))
        patterns['too_many_joins'] = (join_count >= 3 or from_tables >= 4)
        
        # 3. Correlated subqueries
        patterns['correlated_subqueries'] = (
            'EXISTS (' in sql_upper or
            re.search(r'IN\s*\(\s*SELECT', sql_upper) or
            re.search(r'=\s*\(\s*SELECT', sql_upper)
        )
        
        # 4. Heavy DISTINCT usage
        patterns['heavy_distinct'] = (
            'DISTINCT' in sql_upper and
            ('ORDER BY' in sql_upper or 'GROUP BY' in sql_upper or join_count > 0)
        )
        
        # 5. Heavy ORDER BY / GROUP BY
        patterns['heavy_sorting'] = (
            'ORDER BY' in sql_upper and 'GROUP BY' in sql_upper
        ) or (
            ('ORDER BY' in sql_upper or 'GROUP BY' in sql_upper) and join_count > 1
        )
        
        # 6. Functions in WHERE clause
        patterns['functions_in_where'] = bool(re.search(
            r'WHERE.*(?:UPPER|LOWER|TO_CHAR|TO_DATE|SUBSTR|NVL|DECODE|CASE)\s*\(', 
            sql_upper
        ))
        
        # 7. Literal values instead of bind variables
        patterns['literal_values'] = (
            re.search(r"=\s*'[^']+'", sql_text) or
            re.search(r'=\s*\d+(?!\s*[,)])', sql_text) or
            re.search(r"IN\s*\([^:)]*'[^']+'\s*[^)]*\)", sql_text)
        )
        
        # 8. RMAN / background jobs
        patterns['rman_background'] = (
            'RMAN@' in sql_upper or
            'SYS.DBMS_BACKUP_RESTORE' in sql_upper or
            'X$K' in sql_upper or
            'DBMS_STATS' in sql_upper or
            'KSXM:TAKE_SNPSHOT' in sql_upper or
            'SYS.KUPC$' in sql_upper
        )
        
        # 9. PL/SQL blocks
        patterns['plsql_blocks'] = (
            sql_upper.strip().startswith('DECLARE') or
            sql_upper.strip().startswith('BEGIN') or
            'DECLARE' in sql_upper or
            'BEGIN' in sql_upper
        )
        
        # 10. DDL queries inside workload
        patterns['ddl_operations'] = bool(re.search(
            r'\b(?:CREATE|ALTER|DROP|TRUNCATE|ANALYZE)\s+(?:TABLE|INDEX|VIEW|SEQUENCE)', 
            sql_upper
        ))
        
        return patterns

    def _analyze_metrics_context(self, metrics: Dict) -> Dict[str, Any]:
        """
        Analyze performance metrics to understand query behavior context
        """
        elapsed_time = float(metrics.get('elapsed_time', 0))
        executions = int(metrics.get('executions', 0))
        cpu_time = float(metrics.get('cpu_time', 0))
        elapsed_per_exec = float(metrics.get('elapsed_per_exec', 0))
        pcttotal = float(metrics.get('pcttotal', 0))
        pctcpu = float(metrics.get('pctcpu', 0))
        pctio = float(metrics.get('pctio', 0))
        
        context = {
            'is_high_cpu': cpu_time > 20 or pctcpu > 90,
            'is_high_elapsed': elapsed_time > 50,
            'is_high_frequency': executions > 800,
            'is_slow_per_exec': elapsed_per_exec > 0.5,
            'is_io_bound': pctio > 30,
            'is_cpu_bound': pctcpu > 85,
            'workload_percentage': pcttotal,
            'is_significant_workload': pcttotal > 10
        }
        
        # Categorize query behavior
        if context['is_high_frequency'] and elapsed_time < 15:
            context['query_type'] = 'HIGH_FREQUENCY_LOW_IMPACT'
        elif context['is_high_elapsed'] and executions < 200:
            context['query_type'] = 'LOW_FREQUENCY_HIGH_IMPACT'
        elif context['is_high_cpu'] and context['is_high_elapsed']:
            context['query_type'] = 'CPU_BOTTLENECK'
        elif context['is_io_bound']:
            context['query_type'] = 'IO_BOTTLENECK'
        else:
            context['query_type'] = 'STABLE'
            
        return context

    def _apply_intelligence_brain(self, sql_id: str, sql_text: str, patterns: Dict, 
                                context: Dict, metrics: Dict) -> Dict[str, Any]:
        """
        🧠 Apply Intelligence Brain - Condition → Meaning → Action logic
        """
        
        # Determine primary condition based on patterns + metrics
        primary_condition = self._determine_primary_condition(patterns, context, metrics)
        
        # Apply brain logic based on condition
        if primary_condition == 'HIGH_CPU_HIGH_ELAPSED':
            return self._handle_cpu_bottleneck(sql_id, sql_text, patterns, context, metrics)
        elif primary_condition == 'HIGH_FREQUENCY_LOW_ELAPSED':
            return self._handle_frequency_load(sql_id, sql_text, patterns, context, metrics)
        elif primary_condition == 'HIGH_IO_PATTERN':
            return self._handle_io_bottleneck(sql_id, sql_text, patterns, context, metrics)
        elif primary_condition == 'RMAN_SYSTEM_SQL':
            return self._handle_background_load(sql_id, sql_text, patterns, context, metrics)
        elif primary_condition == 'STABLE_PERFORMANCE':
            return self._handle_stable_query(sql_id, sql_text, patterns, context, metrics)
        else:
            return self._handle_general_optimization(sql_id, sql_text, patterns, context, metrics)

    def _determine_primary_condition(self, patterns: Dict, context: Dict, metrics: Dict) -> str:
        """Determine the primary condition for this SQL based on patterns and metrics"""
        
        # RMAN/System queries get highest priority
        if patterns.get('rman_background'):
            return 'RMAN_SYSTEM_SQL'
        
        # High CPU + High Elapsed = CPU Bottleneck
        if context.get('is_high_cpu') and context.get('is_high_elapsed'):
            return 'HIGH_CPU_HIGH_ELAPSED'
        
        # High frequency + Low individual impact = Frequency Load
        if context.get('query_type') == 'HIGH_FREQUENCY_LOW_IMPACT':
            return 'HIGH_FREQUENCY_LOW_ELAPSED'
        
        # IO bound with pattern indicators
        if context.get('is_io_bound') or patterns.get('full_table_scan') or patterns.get('too_many_joins'):
            return 'HIGH_IO_PATTERN'
        
        # Stable performance
        if context.get('query_type') == 'STABLE':
            return 'STABLE_PERFORMANCE'
        
        return 'GENERAL_OPTIMIZATION'

    def _handle_cpu_bottleneck(self, sql_id: str, sql_text: str, patterns: Dict, 
                             context: Dict, metrics: Dict) -> Dict[str, Any]:
        """Handle HIGH CPU + HIGH ELAPSED condition"""
        
        elapsed_time = metrics.get('elapsed_time', 0)
        cpu_time = metrics.get('cpu_time', 0) 
        executions = metrics.get('executions', 0)
        
        # Index recommendations
        index_rec = []
        if patterns.get('functions_in_where'):
            index_rec.append("• CREATE function-based indexes for WHERE clause functions")
        if patterns.get('full_table_scan'):
            index_rec.append("• ADD selective indexes to eliminate full table scans")
        if patterns.get('too_many_joins'):
            index_rec.append("• OPTIMIZE join indexes - ensure proper foreign key indexes")
        if not index_rec:
            index_rec.append("• REBUILD existing indexes to reduce CPU overhead")
            
        # Query rewrite recommendations
        rewrite_rec = []
        if patterns.get('correlated_subqueries'):
            rewrite_rec.append("• REPLACE correlated subqueries with JOINs")
        if patterns.get('heavy_distinct'):
            rewrite_rec.append("• ELIMINATE unnecessary DISTINCT operations")
        if patterns.get('functions_in_where'):
            rewrite_rec.append("• MOVE functions out of WHERE clause when possible")
        if patterns.get('heavy_sorting'):
            rewrite_rec.append("• OPTIMIZE ORDER BY/GROUP BY - reduce sorting overhead")
        if not rewrite_rec:
            rewrite_rec.append("• REVIEW query execution plan for CPU-intensive operations")
        
        # Risk assessment
        risk_assessment = f"CRITICAL: SQL_ID {sql_id} consuming {cpu_time:.1f}s CPU • {elapsed_time:.1f}s elapsed • {executions} executions\n• CPU bottleneck requires immediate attention\n• Query consuming significant database resources"
        
        # DBA recommendations
        dba_recs = [
            f"🎯 Tune High-Elapsed SQL\n   • Focus on SQL_ID {sql_id} ({elapsed_time:.1f}s elapsed)\n   • Use SQL Tuning Advisor: EXEC DBMS_SQLTUNE.CREATE_TUNING_TASK\n   • Review execution plan for costly operations",
            "⚡ Control CPU and Workload\n   • Implement Resource Manager to limit CPU consumption\n   • EXEC DBMS_RESOURCE_MANAGER.CREATE_CONSUMER_GROUP\n   • Monitor CPU queue waits in V$SYSMETRIC",
            "📊 Update Optimizer Statistics\n   • EXEC DBMS_STATS.GATHER_SCHEMA_STATS(estimate_percent=>10)\n   • Set proper histogram collection for skewed data\n   • Update table and index statistics"
        ]
        
        return {
            'condition': 'HIGH_CPU_HIGH_ELAPSED',
            'risk_level': 'HIGH',
            'index_recommendations': "\n".join(index_rec),
            'query_rewrite': "\n".join(rewrite_rec),
            'risk_assessment': risk_assessment,
            'dba_recommendations': "\n\n".join(dba_recs),
            'patterns_detected': [k for k, v in patterns.items() if v]
        }

    def _handle_frequency_load(self, sql_id: str, sql_text: str, patterns: Dict, 
                             context: Dict, metrics: Dict) -> Dict[str, Any]:
        """Handle HIGH FREQUENCY + LOW ELAPSED condition"""
        
        executions = metrics.get('executions', 0)
        elapsed_time = metrics.get('elapsed_time', 0)
        cpu_time = metrics.get('cpu_time', 0)
        
        # Index recommendations
        index_rec = "Current index structure appears adequate for workload\n• Monitor for changes in access patterns"
        
        # Query rewrite recommendations  
        rewrite_rec = []
        if patterns.get('literal_values'):
            rewrite_rec.append("• USE bind variables instead of literals")
        rewrite_rec.append("• IMPLEMENT result caching for repeated queries")
        rewrite_rec.append("• BATCH multiple calls if possible")
        rewrite_rec.append("• REVIEW application logic for excessive query triggering")
        
        # Risk assessment
        risk_assessment = f"MEDIUM: SQL_ID {sql_id} moderate frequency impact • {executions} executions • {elapsed_time:.1f}s total\n• Individual performance acceptable but frequency creates workload pressure\n• Monitor execution patterns and application behavior"
        
        # DBA recommendations
        dba_recs = [
            f"🔧 Bind Variables & Cursor Optimization\n   • Ensure bind variables used for SQL_ID {sql_id}\n   • Monitor cursor sharing: V$SQL_SHARED_CURSOR\n   • Check for cursor cache misses",
            "🏗️ Session & Connection Optimization\n   • Review connection pooling efficiency\n   • Monitor session management overhead\n   • Optimize cursor management in application"
        ]
        
        return {
            'condition': 'HIGH_FREQUENCY_LOW_ELAPSED', 
            'risk_level': 'MEDIUM',
            'index_recommendations': index_rec,
            'query_rewrite': "\n".join(rewrite_rec),
            'risk_assessment': risk_assessment,
            'dba_recommendations': "\n\n".join(dba_recs),
            'patterns_detected': [k for k, v in patterns.items() if v]
        }

    def _handle_io_bottleneck(self, sql_id: str, sql_text: str, patterns: Dict, 
                            context: Dict, metrics: Dict) -> Dict[str, Any]:
        """Handle HIGH IO condition"""
        
        elapsed_time = metrics.get('elapsed_time', 0)
        pctio = metrics.get('pctio', 0)
        executions = metrics.get('executions', 0)
        
        # Index recommendations
        index_rec = []
        if patterns.get('full_table_scan'):
            index_rec.append("• CREATE selective indexes to eliminate table scans")
        if patterns.get('too_many_joins'):
            index_rec.append("• ADD composite indexes for multi-table JOIN operations")
        index_rec.append("• Run SQL Access Advisor for missing index analysis")
        
        # Query rewrite recommendations
        rewrite_rec = []
        if patterns.get('too_many_joins'):
            rewrite_rec.append("• OPTIMIZE JOIN order - put most selective conditions first")
        if patterns.get('correlated_subqueries'):
            rewrite_rec.append("• REPLACE correlated subqueries with EXISTS or JOIN operations")
        if not rewrite_rec:
            rewrite_rec.append("• REVIEW execution plan for I/O-intensive operations")
        
        # Risk assessment
        risk_level = "HIGH" if elapsed_time > 50 else "MEDIUM"
        risk_assessment = f"{risk_level}: SQL_ID {sql_id} I/O bottleneck • {pctio:.1f}% I/O wait • {elapsed_time:.1f}s elapsed\n• Missing or inefficient indexes causing excessive I/O\n• Query requires index optimization"
        
        # DBA recommendations
        dba_recs = [
            f"💾 Add / Optimize Indexes\n   • Focus on SQL_ID {sql_id} I/O reduction\n   • Run SQL Access Advisor: EXEC DBMS_ADVISOR.CREATE_TASK\n   • Create composite indexes for multi-column WHERE clauses",
            "📊 Update Optimizer Statistics\n   • EXEC DBMS_STATS.GATHER_SCHEMA_STATS for affected tables\n   • Ensure current statistics for optimizer decisions\n   • Set proper histogram collection"
        ]
        
        return {
            'condition': 'HIGH_IO_PATTERN',
            'risk_level': risk_level,
            'index_recommendations': "\n".join(index_rec),
            'query_rewrite': "\n".join(rewrite_rec),
            'risk_assessment': risk_assessment,
            'dba_recommendations': "\n\n".join(dba_recs),
            'patterns_detected': [k for k, v in patterns.items() if v]
        }

    def _handle_background_load(self, sql_id: str, sql_text: str, patterns: Dict, 
                              context: Dict, metrics: Dict) -> Dict[str, Any]:
        """Handle RMAN/System SQL condition"""
        
        elapsed_time = metrics.get('elapsed_time', 0)
        executions = metrics.get('executions', 0)
        
        # Index recommendations
        index_rec = "System/RMAN operations - index recommendations not applicable"
        
        # Query rewrite recommendations
        rewrite_rec = "System-generated SQL - query rewrite not recommended"
        
        # Risk assessment
        risk_assessment = f"HIGH: SQL_ID {sql_id} system/RMAN operation • {elapsed_time:.1f}s elapsed • {executions} executions\n• Background maintenance affecting production workload\n• Requires scheduling optimization"
        
        # DBA recommendations
        dba_recs = [
            f"⏰ Manage RMAN / Background Jobs\n   • Schedule RMAN backups during low activity periods\n   • SQL_ID {sql_id}: Review backup/maintenance timing\n   • Limit backup parallelism: CONFIGURE DEVICE TYPE DISK PARALLELISM 2",
            "🔍 Continuous Monitoring\n   • Set up alerts for long-running RMAN operations\n   • Monitor backup impact: V$BACKUP_ASYNC_IO\n   • Track maintenance job scheduling"
        ]
        
        return {
            'condition': 'RMAN_SYSTEM_SQL',
            'risk_level': 'HIGH',
            'index_recommendations': index_rec,
            'query_rewrite': rewrite_rec,
            'risk_assessment': risk_assessment,
            'dba_recommendations': "\n\n".join(dba_recs),
            'patterns_detected': [k for k, v in patterns.items() if v]
        }

    def _handle_stable_query(self, sql_id: str, sql_text: str, patterns: Dict, 
                           context: Dict, metrics: Dict) -> Dict[str, Any]:
        """Handle STABLE performance condition"""
        
        elapsed_time = metrics.get('elapsed_time', 0)
        executions = metrics.get('executions', 0)
        
        # Index recommendations
        index_rec = "Current index structure appears adequate for workload"
        
        # Query rewrite recommendations
        rewrite_rec = "No query rewrite needed - performance acceptable"
        
        # Risk assessment
        risk_assessment = f"LOW: SQL_ID {sql_id} performance within acceptable range • {elapsed_time:.1f}s elapsed • {executions} executions\n• Continue monitoring for performance changes"
        
        # DBA recommendations
        dba_recs = [
            f"🔍 Continuous Monitoring\n   • SQL_ID {sql_id}: Continue standard monitoring\n   • Set up alerts if performance degrades\n   • Track execution patterns over time"
        ]
        
        return {
            'condition': 'STABLE_PERFORMANCE',
            'risk_level': 'LOW',
            'index_recommendations': index_rec,
            'query_rewrite': rewrite_rec,
            'risk_assessment': risk_assessment,
            'dba_recommendations': "\n\n".join(dba_recs),
            'patterns_detected': [k for k, v in patterns.items() if v]
        }

    def _handle_general_optimization(self, sql_id: str, sql_text: str, patterns: Dict, 
                                   context: Dict, metrics: Dict) -> Dict[str, Any]:
        """Handle general optimization cases"""
        
        elapsed_time = metrics.get('elapsed_time', 0)
        executions = metrics.get('executions', 0)
        cpu_time = metrics.get('cpu_time', 0)
        
        # Build recommendations based on detected patterns
        index_rec = []
        if patterns.get('full_table_scan'):
            index_rec.append("• ADD indexes for WHERE clause columns")
        if patterns.get('too_many_joins'):
            index_rec.append("• OPTIMIZE join indexes")
        if not index_rec:
            index_rec.append("• Monitor index usage patterns")
            
        rewrite_rec = []
        if patterns.get('literal_values'):
            rewrite_rec.append("• USE bind variables instead of literals")
        if patterns.get('correlated_subqueries'):
            rewrite_rec.append("• Consider rewriting subqueries as JOINs")
        if not rewrite_rec:
            rewrite_rec.append("• Review execution plan for optimization opportunities")
        
        # Risk assessment
        risk_level = "HIGH" if elapsed_time > 30 else "MEDIUM" if elapsed_time > 10 else "LOW"
        risk_assessment = f"{risk_level}: SQL_ID {sql_id} • {elapsed_time:.1f}s elapsed • {cpu_time:.1f}s CPU • {executions} executions\n• Performance optimization opportunities identified"
        
        # DBA recommendations based on metrics
        dba_recs = []
        if elapsed_time > 20:
            dba_recs.append(f"🎯 Tune High-Elapsed SQL\n   • Focus on SQL_ID {sql_id}\n   • Use SQL Tuning Advisor\n   • Review execution plan")
        if cpu_time > 10:
            dba_recs.append("📊 Update Optimizer Statistics\n   • EXEC DBMS_STATS.GATHER_SCHEMA_STATS\n   • Refresh table statistics")
        if not dba_recs:
            dba_recs.append(f"🔍 Continue Monitoring\n   • SQL_ID {sql_id}: Track performance trends\n   • Set up performance alerts")
        
        return {
            'condition': 'GENERAL_OPTIMIZATION',
            'risk_level': risk_level,
            'index_recommendations': "\n".join(index_rec),
            'query_rewrite': "\n".join(rewrite_rec),
            'risk_assessment': risk_assessment,
            'dba_recommendations': "\n\n".join(dba_recs),
            'patterns_detected': [k for k, v in patterns.items() if v]
        }

    def _create_default_analysis(self, sql_id: str, metrics: Dict) -> Dict[str, Any]:
        """Create default analysis when SQL text is not available"""
        return {
            'condition': 'LIMITED_DATA',
            'risk_level': 'LOW',
            'index_recommendations': 'SQL text not available for pattern analysis',
            'query_rewrite': 'No query rewrite suggestions available',
            'risk_assessment': f'SQL_ID {sql_id}: Limited analysis due to unavailable SQL text',
            'dba_recommendations': 'Continue standard monitoring procedures',
            'patterns_detected': []
        }