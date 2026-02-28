"""
DBA Expert Engine - Senior Oracle DBA Analysis
Think like a real DBA: Identify ONLY problematic SQL/wait events with deep analysis

CRITICAL DATA INTEGRITY RULES:
1. Data is ALWAYS user-scoped (data/users/<username>/parsed_csv/)
2. New upload = New source of truth (invalidate all previous data)
3. Strict parsing → Strict analysis (required CSVs must exist)
4. UI & API consistency guarantee (accurate CSV counts)
5. Analysis scope is isolated (no cross-contamination)
6. DBA action plan rules (data-driven only)
7. Failure safety (stop on parsing failure)
8. Output contract (fresh analysis per upload)
"""

from typing import Dict, List, Any

from engine.fix_recommendation_formatter import FixRecommendationResult
from engine.load_reduction_engine import LoadReductionResult

from engine.decision_engine import ActionType, ActionType, DecisionResult

from engine.dynamic_sql_generator import GeneratedSQL

from engine.dynamic_sql_generator import DBAActionPlan



# Import DataIntegrityValidator for data integrity validation
try:
    from engine.data_integrity_validator import DataIntegrityValidator
except ImportError:
    # Fallback if DataIntegrityValidator is not available
    DataIntegrityValidator = None

# Import Decision Engine and Dynamic SQL Generator for evidence-driven recommendations
try:
    from engine.decision_engine import DecisionEngine, SignalNormalizer, NormalizedSignals
    from engine.dynamic_sql_generator import DynamicSQLGenerator
    DECISION_ENGINE_AVAILABLE = True
except ImportError:
    DECISION_ENGINE_AVAILABLE = False

# Import Load Reduction Engine for actionable DBA queries
try:
    from engine.load_reduction_engine import LoadReductionEngine, generate_load_reduction_for_finding
    LOAD_REDUCTION_AVAILABLE = True
except ImportError:
    LOAD_REDUCTION_AVAILABLE = False

# Import Fix Recommendation Formatter for UI-ready executable actions
try:
    from engine.fix_recommendation_formatter import (
        FixRecommendationFormatter, 
        generate_fix_recommendations_for_finding
    )
    FIX_FORMATTER_AVAILABLE = True
except ImportError:
    FIX_FORMATTER_AVAILABLE = False


class DBAExpertEngine:
    """
    Senior DBA Expert Analysis Engine
    - Identifies ONLY truly problematic SQL queries (1-3 max)
    - Provides deep DBA-level analysis
    - Gives confident, actionable recommendations
    """
    
    # DBA Thresholds - Adjusted to catch real problematic queries
    # HIGH SEVERITY thresholds
    CRITICAL_ELAPSED_TIME = 30.0       # > 30s is HIGH SEVERITY
    HIGH_ELAPSED_TIME = 10.0           # > 10s is MEDIUM SEVERITY
    
    CRITICAL_CPU_TIME = 20.0           # > 20s CPU is critical
    HIGH_CPU_TIME = 5.0                # > 5s CPU is high
    
    CRITICAL_EXECUTIONS = 500          # > 500 execs is high frequency
    MEDIUM_EXECUTIONS = 50             # > 50 execs
    
    CRITICAL_WORKLOAD_PCT = 15.0       # > 15% wait contribution
    HIGH_WORKLOAD_PCT = 5.0            # > 5% of total DB time
    
    CRITICAL_AVG_ELAPSED = 1.0         # > 1s per exec is HIGH
    MEDIUM_AVG_ELAPSED = 0.1           # > 0.1s per exec is concerning
    
    HIGH_CPU_PERCENTAGE = 50.0         # CPU ≥ 50%
    MEDIUM_CPU_PERCENTAGE = 30.0       # CPU > 30%
    
    def __init__(self, user_csv_dir: str = None, username: str = None) -> None:
        """
        Initialize DBA Expert Engine with CRITICAL DATA INTEGRITY ENFORCEMENT
        
        Args:
            user_csv_dir: User-specific CSV directory (REQUIRED for data integrity)
            username: Username for validation (REQUIRED for data integrity)
        """
        self.problematic_sql = []
        self.ignored_sql = []
        
        # CRITICAL: Data integrity validation
        self.user_csv_dir: str = user_csv_dir
        self.username: str = username
        self.data_integrity_validator = None
        self.last_validation_result = None
        
        # Initialize data integrity validator if required params provided
        if user_csv_dir and username and DataIntegrityValidator:
            self.data_integrity_validator = DataIntegrityValidator(user_csv_dir, username)
            print(f"🔒 DBA Expert Engine initialized with data integrity validation")
            print(f"   User: {username}")
            print(f"   CSV Directory: {user_csv_dir}")
        else:
            print("⚠️  DBA Expert Engine initialized WITHOUT data integrity validation")
            print("   This mode should only be used for testing")
        
    def analyze_workload(self, top_sql: List[Dict], raw_sql: List[Dict], 
                        wait_events: List[Dict], ash_analysis: Dict) -> Dict[str, Any]:
        """
        Main DBA analysis entry point - WITH CRITICAL DATA INTEGRITY ENFORCEMENT
        Returns ONLY truly problematic items with deep analysis
        
        CRITICAL: ALL 8 DATA INTEGRITY RULES MUST PASS BEFORE ANALYSIS
        """
        
        # ========================================================================
        # STEP 0: CRITICAL DATA INTEGRITY VALIDATION (NON-NEGOTIABLE)
        # ========================================================================
        if self.data_integrity_validator:
            print("🔒 ENFORCING CRITICAL DATA INTEGRITY RULES...")
            validation_result = self.data_integrity_validator.validate_data_integrity()
            self.last_validation_result = validation_result
            
            if not validation_result["is_valid"]:
                print("❌ DATA INTEGRITY VIOLATION DETECTED!")
                print("   Violations:")
                for violation in validation_result["violations"]:
                    print(f"   - {violation}")
                
                # RULE 7: FAILURE SAFETY - Stop processing immediately
                return {
                    "status": "INVALID",
                    "error": "CRITICAL DATA INTEGRITY VIOLATIONS DETECTED",
                    "error_type": "DATA_INTEGRITY_FAILURE",
                    "message": "No valid data available for analysis",
                    "violations": validation_result["violations"],
                    "validation_details": validation_result,
                    "workload_summary": {},
                    "problematic_count": 0,
                    "total_analyzed": 0,
                    "problematic_sql_findings": [],
                    "dba_conclusion": "ANALYSIS BLOCKED: Data integrity rules violated. Please upload fresh AWR/ASH files."
                }
            else:
                print("✅ ALL DATA INTEGRITY RULES VALIDATED SUCCESSFULLY")
                print(f"   Rules validated: {len(validation_result['rules_validated'])}")
                print(f"   CSV files found: {validation_result['csv_counts'].get('accurate_count', 0)}")
                print(f"   User: {validation_result['username']}")
                print(f"   Timestamp: {validation_result['timestamp']}")
        else:
            print("⚠️  WARNING: Running without data integrity validation (testing mode only)")
        
        # Step 1: Deep workload pattern analysis
        workload_analysis: Dict[str, Any] = self._analyze_workload_patterns(top_sql, wait_events, ash_analysis)
        
        # RULE 4: UI CONSISTENCY - Update workload analysis with correct SQL count from raw CSV data
        workload_analysis["sql_count"] = len(raw_sql)
        workload_analysis["sql_analyzed"] = len(raw_sql)
        
        # Add data integrity validation metadata to workload analysis
        if self.last_validation_result:
            workload_analysis["data_integrity"] = {
                "validation_timestamp": self.last_validation_result["timestamp"],
                "csv_counts": self.last_validation_result["csv_counts"],
                "rules_validated_count": len(self.last_validation_result["rules_validated"]),
                "username": self.last_validation_result["username"]
            }
        
        # Step 2: Apply strict DBA filtering - shortlist ONLY problematic SQL
        problematic_items = self._filter_problematic_sql(top_sql, raw_sql, workload_analysis)
        
        # Step 3: Deep DBA analysis for each problematic item
        dba_findings = []
        for item in problematic_items:
            deep_analysis: Dict[str, Any] = self._perform_deep_dba_analysis(item, workload_analysis, wait_events, ash_analysis)
            dba_findings.append(deep_analysis)
        
        # Step 4: Build DBA-level output with DATA INTEGRITY GUARANTEE
        analysis_result = {
            "status": "SUCCESS",
            "workload_summary": workload_analysis,
            "problematic_count": len(dba_findings),
            "total_analyzed": len(raw_sql),  # RULE 4: Count all SQL from CSV
            "problematic_sql_findings": dba_findings,
            "dba_conclusion": self._generate_dba_conclusion(dba_findings, workload_analysis)
        }
        
        # RULE 8: OUTPUT CONTRACT - Add metadata proving this is fresh analysis
        if self.last_validation_result:
            analysis_result["data_integrity_validation"] = {
                "timestamp": self.last_validation_result["timestamp"],
                "username": self.last_validation_result["username"],
                "csv_directory": self.last_validation_result["csv_directory"],
                "csv_count_verified": self.last_validation_result["csv_counts"].get("accurate_count", 0),
                "rules_validated": self.last_validation_result["rules_validated"],
                "integrity_status": "VALIDATED"
            }
        
        return analysis_result
    
    def _analyze_workload_patterns(self, top_sql: List[Dict], wait_events: List[Dict], 
                                   ash_analysis: Dict) -> Dict[str, Any]:
        """
        Step 1: Understand overall workload characteristics
        Think like a DBA examining the system
        """
        
        if not top_sql:
            return {
                "pattern": "NO_SIGNIFICANT_WORKLOAD",
                "total_elapsed": 0,
                "total_cpu": 0,
                "total_executions": 0,
                "dominant_wait": None
            }
        
        # Calculate total workload metrics
        total_elapsed: int = sum(sql.get('elapsed', 0) for sql in top_sql)
        total_cpu: int = sum(sql.get('cpu', 0) for sql in top_sql)
        total_executions: int = sum(sql.get('executions', 0) for sql in top_sql)
        avg_elapsed: float | int = total_elapsed / len(top_sql) if top_sql else 0
        
        # Identify dominant wait event
        dominant_wait = None
        if wait_events:
            dominant_wait = {
                "event": wait_events[0].get('statistic_name', 'Unknown'),
                "time_s": wait_events[0].get('time_s', 0),
                "pct_db_time": wait_events[0].get('pct_of_db_time', 0)
            }
        
        # Determine workload pattern
        pattern: str = self._classify_workload_pattern(total_elapsed, total_cpu, total_executions, 
                                                  dominant_wait, ash_analysis)
        
        return {
            "pattern": pattern,
            "total_elapsed": round(total_elapsed, 2),
            "total_cpu": round(total_cpu, 2),
            "total_executions": total_executions, 
            "avg_elapsed": round(avg_elapsed, 2),
            "dominant_wait": dominant_wait,
            "sql_count": len(top_sql),  # This will be updated with raw_sql count later
            "sql_analyzed": len(top_sql)  # This will be updated with raw_sql count later
        }
    
    def _classify_workload_pattern(self, total_elapsed: float, total_cpu: float, 
                                   total_executions: int, dominant_wait: Dict, 
                                   ash_analysis: Dict) -> str:
        """Classify the overall workload pattern"""
        
        if total_elapsed > 500:
            if total_cpu > 200:
                return "CPU_INTENSIVE_HEAVY_LOAD"
            else:
                return "IO_INTENSIVE_HEAVY_LOAD"
        elif total_executions > 10000:
            return "HIGH_FREQUENCY_WORKLOAD"
        elif dominant_wait and dominant_wait.get('pct_db_time', 0) > 30:
            return "WAIT_EVENT_DOMINATED"
        else:
            return "MODERATE_WORKLOAD"
    
    def _filter_problematic_sql(self, top_sql: List[Dict], raw_sql: List[Dict], 
                               workload_analysis: Dict) -> List[Dict]:
        """
        Step 2: Apply strict DBA filtering
        Return ONLY 1-3 most problematic SQL queries
        """
        
        problematic = []
        
        for idx, sql in enumerate(top_sql):
            # Extract metrics
            sql_id = sql.get('sql_id', f'SQL_{idx}')
            elapsed = sql.get('elapsed', 0)
            cpu = sql.get('cpu', 0)
            executions = sql.get('executions', 0)
            elapsed_per_exec = sql.get('elapsed_per_exec', 0)
            pcttotal = sql.get('pcttotal', 0)
            pctcpu = sql.get('pctcpu', 0)
            pctio = sql.get('pctio', 0)
            
            # Get SQL text if available
            sql_text = None
            if idx < len(raw_sql):
                sql_text = raw_sql[idx].get('sql_text', None)
            
            # DBA FILTERING LOGIC - Multiple criteria based on Oracle DBA standards
            is_problematic = False
            problem_reasons = []
            severity = "NONE"
            
            # ========== HIGH SEVERITY CHECKS (User Requirements) ==========
            
            # Criterion 1: Elapsed time > 50s = HIGH SEVERITY
            if elapsed >= self.CRITICAL_ELAPSED_TIME:
                is_problematic = True
                problem_reasons.append(f"HIGH_ELAPSED: {elapsed:.1f}s total elapsed time")
                severity = "HIGH"
            # Criterion 2: Elapsed time > 20s = MEDIUM SEVERITY
            elif elapsed >= self.HIGH_ELAPSED_TIME:
                is_problematic = True
                problem_reasons.append(f"MEDIUM_ELAPSED: {elapsed:.1f}s total elapsed time")
                severity = "MEDIUM"
            
            # Criterion 3: Executions > 100 = Concerning workload
            if executions >= self.CRITICAL_EXECUTIONS:
                is_problematic = True
                problem_reasons.append(f"HIGH_FREQUENCY: {executions} executions")
                if severity == "NONE":
                    severity = "HIGH"
            elif executions >= self.MEDIUM_EXECUTIONS and elapsed > 10:
                is_problematic = True
                problem_reasons.append(f"MEDIUM_FREQUENCY: {executions} executions causing {elapsed:.1f}s load")
                if severity == "NONE":
                    severity = "MEDIUM"
            
            # Criterion 4: Avg per exec > 1 sec = HIGH SEVERITY
            if elapsed_per_exec >= self.CRITICAL_AVG_ELAPSED:
                is_problematic = True
                problem_reasons.append(f"SLOW_EXECUTION: {elapsed_per_exec:.2f}s per execution")
                if severity != "HIGH":
                    severity = "HIGH"
            elif elapsed_per_exec >= self.MEDIUM_AVG_ELAPSED and executions > 50:
                is_problematic = True
                problem_reasons.append(f"SLOW_AVG_EXEC: {elapsed_per_exec:.2f}s per execution")
                if severity == "NONE":
                    severity = "MEDIUM"
            
            # Criterion 5: CPU ≥ 70% = HIGH SEVERITY
            if pctcpu >= self.HIGH_CPU_PERCENTAGE:
                is_problematic = True
                problem_reasons.append(f"HIGH_CPU_PCT: {pctcpu:.1f}% CPU utilization")
                if severity != "HIGH":
                    severity = "HIGH"
            # CPU > 50% = MEDIUM SEVERITY
            elif pctcpu >= self.MEDIUM_CPU_PERCENTAGE:
                is_problematic = True
                problem_reasons.append(f"MEDIUM_CPU_PCT: {pctcpu:.1f}% CPU utilization")
                if severity == "NONE":
                    severity = "MEDIUM"
            
            # Criterion 6: Wait contribution ≥ 25% of DB time = HIGH SEVERITY
            if pcttotal >= self.CRITICAL_WORKLOAD_PCT:
                is_problematic = True
                problem_reasons.append(f"DOMINANT_WORKLOAD: {pcttotal:.1f}% of total DB time")
                severity = "HIGH"
            elif pcttotal >= self.HIGH_WORKLOAD_PCT:
                is_problematic = True
                problem_reasons.append(f"HIGH_WORKLOAD_IMPACT: {pcttotal:.1f}% of total DB time")
                if severity == "NONE":
                    severity = "MEDIUM"
            
            # Criterion 7: High IO waits = MEDIUM/HIGH SEVERITY
            if pctio >= 40:
                is_problematic = True
                problem_reasons.append(f"HIGH_IO_WAIT: {pctio:.1f}% IO wait time")
                if severity == "NONE":
                    severity = "MEDIUM"
            
            # Criterion 8: Combined CPU time check
            if cpu >= self.CRITICAL_CPU_TIME:
                is_problematic = True
                problem_reasons.append(f"CRITICAL_CPU: {cpu:.1f}s CPU time")
                severity = "HIGH"
            elif cpu >= self.HIGH_CPU_TIME and elapsed > 30:
                is_problematic = True
                problem_reasons.append(f"HIGH_CPU: {cpu:.1f}s CPU time")
                if severity == "NONE":
                    severity = "MEDIUM"
            
            # Add to problematic list if criteria met
            if is_problematic:
                problematic.append({
                    'sql_id': sql_id,
                    'sql_text': sql_text,
                    'metrics': sql,
                    'problem_reasons': problem_reasons,
                    'severity': severity,
                    'dba_score': self._calculate_dba_score(elapsed, cpu, executions, pcttotal, elapsed_per_exec)
                })
        
        # Sort by DBA score (most critical first)
        problematic.sort(key=lambda x: x['dba_score'], reverse=True)
        
        # Return ONLY top 2-3 most problematic (user requirement: Max 2-3 SQL only)
        if len(problematic) == 0:
            self.problematic_sql = []
            self.ignored_sql = []
            return []
        
        # Always return max 3, but prefer 2 if the 3rd is much weaker
        max_return: int = min(3, len(problematic))
        
        # If we have more than 2, check if the 3rd is significantly weaker
        if max_return == 3 and problematic[2]['dba_score'] < problematic[0]['dba_score'] * 0.4:
            max_return = 2
        
        # But if only 1 is HIGH severity and rest are LOW, only return that 1
        if max_return > 1:
            high_severity_count: int = sum(1 for p in problematic[:max_return] if p['severity'] in ['HIGH', 'CRITICAL'])
            if high_severity_count == 1 and problematic[1]['severity'] not in ['HIGH', 'MEDIUM', 'CRITICAL']:
                max_return = 1
        
        self.problematic_sql = problematic[:max_return]
        self.ignored_sql = problematic[max_return:]
        
        return self.problematic_sql
    
    def _calculate_dba_score(self, elapsed: float, cpu: float, executions: int, 
                            pcttotal: float, elapsed_per_exec: float) -> float:
        """
        Calculate DBA priority score
        Higher score = more critical
        """
        score = 0.0
        
        # Elapsed time weight (highest priority)
        score += (elapsed / 100.0) * 40.0
        
        # CPU time weight
        score += (cpu / 50.0) * 25.0
        
        # Workload contribution weight
        score += (pcttotal / 20.0) * 20.0
        
        # Execution frequency weight
        score += min((executions / 5000.0) * 10.0, 10.0)
        
        # Per-execution performance weight
        score += min((elapsed_per_exec / 2.0) * 5.0, 5.0)
        
        return round(score, 2)
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        External method to validate data integrity without running full analysis
        Used by API endpoints to check data validity before processing
        
        Returns:
            Dict containing validation status and details
        """
        if not self.data_integrity_validator:
            return {
                "is_valid": False,
                "error": "Data integrity validator not initialized",
                "message": "DBA Expert Engine must be initialized with user_csv_dir and username"
            }
        
        validation_result = self.data_integrity_validator.validate_data_integrity()
        self.last_validation_result = validation_result
        
        return validation_result
    
    def get_csv_count_for_ui(self) -> Dict[str, Any]:
        """
        RULE 4: UI CONSISTENCY GUARANTEE
        Returns accurate CSV counts for UI display
        
        Returns:
            Dict with accurate CSV counts and metadata
        """
        if not self.last_validation_result:
            # Run validation to get current counts
            validation_result: Dict[str, Any] = self.validate_data_integrity()
            if not validation_result["is_valid"]:
                return {
                    "total_csv_files": 0,
                    "new_csv_files_generated": 0,
                    "error": "Data integrity validation failed",
                    "csv_file_list": []
                }
        else:
            validation_result = self.last_validation_result
        
        csv_counts = validation_result.get("csv_counts", {})
        
        return {
            "total_csv_files": csv_counts.get("accurate_count", 0),
            "new_csv_files_generated": csv_counts.get("accurate_count", 0),  # For new uploads, these are the same
            "csv_file_list": csv_counts.get("csv_file_list", []),
            "validation_timestamp": validation_result.get("timestamp"),
            "username": validation_result.get("username")
        }
    
    def enforce_failure_safety(self, error_context: str) -> Dict[str, Any]:
        """
        RULE 7: FAILURE SAFETY
        Called when errors are detected to ensure safe failure handling
        
        Args:
            error_context: Description of where the error occurred
        
        Returns:
            Safe failure response following Rule 7
        """
        return {
            "status": "FAILURE",
            "error_type": "ANALYSIS_FAILURE",
            "message": "No valid data available for analysis",
            "error_context": error_context,
            "workload_summary": {},
            "problematic_count": 0,
            "total_analyzed": 0,
            "problematic_sql_findings": [],
            "dba_conclusion": f"ANALYSIS FAILED: {error_context}. Please verify AWR/ASH files and re-upload.",
            "data_integrity_status": "FAILED"
        }
    
    def _perform_deep_dba_analysis(self, item: Dict, workload_analysis: Dict, 
                                   wait_events: List[Dict], ash_analysis: Dict) -> Dict[str, Any]:
        """
        Step 3: Perform deep DBA-level analysis for each problematic SQL
        This is where the real DBA expertise shows
        """
        
        sql_id = item['sql_id']
        sql_text = item.get('sql_text', '')
        metrics = item['metrics']
        problem_reasons = item['problem_reasons']
        severity = item['severity']
        dba_score = item['dba_score']
        
        # Extract metrics
        elapsed = metrics.get('elapsed', 0)
        cpu = metrics.get('cpu', 0)
        executions = metrics.get('executions', 0)
        elapsed_per_exec = metrics.get('elapsed_per_exec', 0)
        pcttotal = metrics.get('pcttotal', 0)
        pctcpu = metrics.get('pctcpu', 0)
        pctio = metrics.get('pctio', 0)
        
        # 1️⃣ SUMMARY - Why is this problematic?
        summary: str = self._generate_problem_summary(sql_id, problem_reasons, elapsed, cpu, 
                                                executions, pcttotal, severity, ash_analysis)
        
        # Extract I/O percentage from ASH analysis if available
        ash_io_percent = 0
        if ash_analysis and "workload_breakdown" in ash_analysis:
            workload_breakdown = ash_analysis["workload_breakdown"]
            if "IO" in workload_breakdown:
                ash_io_percent = workload_breakdown["IO"].get("total_percent", 0)
        
        # Use ASH I/O percentage if available, otherwise fall back to AWR pctio
        io_percentage = ash_io_percent if ash_io_percent > 0 else pctio
        
        # Calculate CPU percentage: if AWR pctcpu is 0/missing, use CPU time ratio
        # This matches the Quick Stats calculation: (cpu / elapsed * 100)
        calculated_cpu_pct = (cpu / elapsed * 100) if elapsed > 0 else 0
        effective_cpu_pct = pctcpu if pctcpu > 0 else calculated_cpu_pct
        
        # 2️⃣ TECHNICAL PERFORMANCE PARAMETERS (User-requested format)
        technical_params = {
            "sql_id": sql_id,
            "elapsed": round(elapsed, 2),
            "cpu": round(cpu, 2),
            "avg_time": round(elapsed_per_exec, 3),
            "executions": executions,
            "risk_level": severity,
            # Additional technical details
            "total_elapsed_time_s": round(elapsed, 2),
            "cpu_time_s": round(cpu, 2),
            "avg_elapsed_per_exec_s": round(elapsed_per_exec, 3),
            "contribution_to_db_time_pct": round(pcttotal, 2),
            "cpu_percentage": round(effective_cpu_pct, 2),
            "io_percentage": round(io_percentage, 2)  # Use ASH-based I/O percentage
        }
        
        # 3️⃣ EXECUTION PATTERN UNDERSTANDING
        execution_pattern: Dict[str, Any] = self._analyze_execution_pattern(executions, elapsed, 
                                                           elapsed_per_exec, pcttotal)
        
        # 4️⃣ POSSIBLE DBA INTERPRETATION (becomes "explanation" for user)
        dba_interpretation: str = self._generate_dba_interpretation(sql_text, metrics, 
                                                              workload_analysis, wait_events, ash_analysis)
        
        # 🛠️ FINAL DBA RECOMMENDATIONS
        dba_recommendations: Dict[str, Any] = self._generate_dba_recommendations(sql_id, sql_text, metrics, 
                                                                dba_interpretation, severity)
        
        # Generate DBA-style human reasoning explanation
        explanation: str = self._generate_dba_explanation(elapsed, cpu, executions, elapsed_per_exec, pctcpu, io_percentage, pcttotal)
        
        # 🔥 LOAD REDUCTION ENGINE - Generate actionable DBA queries
        load_reduction_actions = None
        if LOAD_REDUCTION_AVAILABLE:
            try:
                load_reduction_engine = LoadReductionEngine()
                
                # Determine if plan instability or full scan detected from interpretation
                interp_lower: str = dba_interpretation.lower()
                plan_instability: bool = 'plan' in interp_lower and ('unstable' in interp_lower or 'regression' in interp_lower or 'instability' in interp_lower)
                full_table_scan: bool = 'full scan' in interp_lower or 'table scan' in interp_lower or 'full table' in interp_lower
                
                load_reduction_result: LoadReductionResult = load_reduction_engine.analyze_and_generate_actions(
                    sql_id=sql_id,
                    io_wait_pct=io_percentage,
                    cpu_pct=pctcpu,
                    avg_exec_time=elapsed_per_exec,
                    executions=executions,
                    total_elapsed=elapsed,
                    plan_instability=plan_instability,
                    full_table_scan_detected=full_table_scan
                )
                
                load_reduction_actions: Dict[str, Any] = load_reduction_engine.to_dict(load_reduction_result)
            except Exception as e:
                print(f"Load reduction engine error: {e}")
                load_reduction_actions = None
        
        # ⚡ FIX RECOMMENDATIONS - UI-ready executable DBA actions (NEW)
        fix_recommendations = None
        if FIX_FORMATTER_AVAILABLE:
            try:
                fix_formatter = FixRecommendationFormatter()
                
                # Determine conditions from interpretation
                interp_lower: str = dba_interpretation.lower()
                plan_instability: bool = 'plan' in interp_lower and ('unstable' in interp_lower or 'regression' in interp_lower or 'instability' in interp_lower)
                full_table_scan: bool = 'full scan' in interp_lower or 'table scan' in interp_lower or 'full table' in interp_lower
                high_io_detected: bool = 'i/o' in interp_lower or 'io-heavy' in interp_lower or 'disk read' in interp_lower
                
                fix_result: FixRecommendationResult = fix_formatter.generate_fix_recommendations(
                    sql_id=sql_id,
                    io_wait_pct=io_percentage,
                    cpu_pct=pctcpu,
                    avg_exec_time=elapsed_per_exec,
                    executions=executions,
                    total_elapsed=elapsed,
                    plan_instability=plan_instability,
                    full_table_scan=full_table_scan,
                    high_io_detected=high_io_detected
                )
                
                fix_recommendations: Dict[str, Any] = fix_formatter.to_dict(fix_result)
            except Exception as e:
                print(f"Fix recommendation formatter error: {e}")
                fix_recommendations = None
        
        finding_result = {
            "sql_id": sql_id,
            "severity": severity,
            "dba_priority_score": dba_score,
            "risk_level": severity,  # User-requested field
            "explanation": explanation,  # User-requested field with DBA reasoning
            "problem_summary": summary,
            "technical_parameters": technical_params,
            "execution_pattern": execution_pattern,
            "dba_interpretation": dba_interpretation,
            "dba_recommendations": dba_recommendations,
            "sql_text_preview": sql_text[:200] if sql_text else "SQL text not available"
        }
        
        # Add load reduction actions if available
        if load_reduction_actions:
            finding_result["load_reduction_actions"] = load_reduction_actions
        
        # Add fix recommendations if available (NEW - UI-ready format)
        if fix_recommendations:
            finding_result["fix_recommendations"] = fix_recommendations
        
        return finding_result
    
    def _generate_problem_summary(self, sql_id: str, problem_reasons: List[str], 
                                 elapsed: float, cpu: float, executions: int, 
                                 pcttotal: float, severity: str, ash_analysis: Dict = None) -> str:
        """Generate DBA-level problem summary - conversational and analytical"""
        
        # 1️⃣ SUMMARY - Why is this SQL problematic?
        why_problematic: str = ""
        what_doing: str = ""
        impact_magnitude: str = ""
        
        # Analyze WHY it's problematic
        if pcttotal > 20:
            why_problematic: str = f"This SQL is **dominating your database workload**, consuming {pcttotal:.1f}% of total DB time. When a single query takes up this much share, it's choking other operations."
        elif pcttotal > 10:
            why_problematic: str = f"This SQL has **significant impact** on database performance, accounting for {pcttotal:.1f}% of total DB time. That's too high for a single query."
        elif elapsed > 100:
            why_problematic: str = f"This SQL is burning through **excessive elapsed time** ({elapsed:.1f}s total). Long-running queries like this block resources and impact user experience."
        elif cpu > 50:
            why_problematic: str = f"This SQL is **CPU-intensive**, consuming {cpu:.1f}s of CPU time. It's working the database engine hard - likely due to inefficient execution plan."
        else:
            why_problematic: str = f"This SQL shows **multiple performance red flags** that need immediate attention."
        
        # What is it doing to database?
        if executions > 1000:
            what_doing: str = f"It's hitting the database **{executions:,} times** during the analysis period. High frequency execution like this amplifies any inefficiency."
        elif executions > 100:
            what_doing: str = f"Running **{executions:,} times** - not extremely high frequency, but enough to matter when each execution is slow."
        elif executions < 50:
            what_doing: str = f"Only **{executions:,} executions**, but each one is expensive. Likely a batch job or complex report query."
        else:
            what_doing: str = f"Executed **{executions:,} times** with consistent performance impact."
        
        # How much impact?
        avg_per_exec: float | int = elapsed / executions if executions > 0 else 0
        if avg_per_exec > 1.0:
            impact_magnitude: str = f"**{avg_per_exec:.2f}s per execution** - that's way too slow. Even moderate frequency becomes problematic at this speed."
        elif avg_per_exec > 0.1:
            impact_magnitude: str = f"Averaging **{avg_per_exec:.3f}s per execution**. Combined with frequency, this creates sustained load."
        else:
            impact_magnitude: str = f"Individual executions are fast ({avg_per_exec:.4f}s), but the sheer volume creates cumulative impact."
        
        # Generate ASH correlation DBA summary if ASH data is available
        ash_dba_summary: str = ""
        if ash_analysis and "workload_breakdown" in ash_analysis:
            workload_breakdown = ash_analysis["workload_breakdown"]
            
            # Find the dominant ASH event type
            max_percent = 0
            dominant_type: str = ""
            dominant_data = {}
            
            for event_type, data in workload_breakdown.items():
                if event_type in ['CPU', 'IO', 'Concurrency'] and isinstance(data, dict):
                    total_percent = data.get('total_percent_impact', 0)
                    if total_percent > max_percent:
                        max_percent = total_percent
                        dominant_type = event_type
                        dominant_data = data
            
            # Generate DBA summary based on dominant ASH event
            if max_percent > 0 and dominant_type:
                if dominant_type == 'CPU':
                    ash_dba_summary = f"🔴 **This SQL is CPU-dominant**, consuming {max_percent:.1f}% CPU workload in ASH. Fixing it will significantly reduce CPU pressure."
                elif dominant_type == 'IO':
                    ash_dba_summary = f"📀 **This SQL is I/O-dominant**, responsible for {max_percent:.1f}% I/O workload in ASH. Disk bottlenecks are the primary concern."
                elif dominant_type == 'Concurrency':
                    ash_dba_summary = f"⏳ **This SQL suffers from concurrency issues**, accounting for {max_percent:.1f}% concurrency workload in ASH. Lock contention or resource waits are blocking performance."
        
        # Build the final summary with or without ASH correlation
        # Enhanced Quick Stats with all technical parameters
        avg_per_exec = elapsed / executions if executions > 0 else 0
        
        # Get technical parameters for display
        # Calculate CPU percentage from available data
        pctcpu: float | int = (cpu / elapsed * 100) if elapsed > 0 else 0
        cpu_pct: float | int = pctcpu
        
        # Get I/O percentage from ASH analysis if available
        io_percentage = 0
        if ash_analysis and "workload_breakdown" in ash_analysis:
            workload_breakdown = ash_analysis["workload_breakdown"]
            if "IO" in workload_breakdown:
                io_percentage = workload_breakdown["IO"].get("total_percent", 0)
        
        io_pct = io_percentage
        
        # Format I/O percentage display
        if io_pct > 0:
            io_display: str = f"{io_pct:.1f}% I/O"
        elif 'io_percentage' in locals() and io_percentage == 0:
            io_display = "0.0% I/O"  # Confirmed 0 from data
        else:
            io_display = "I/O: Data Not Available"  # No data available
        
        quick_stats: str = f"📊 **Quick Stats:** {elapsed:.1f}s elapsed | {cpu:.1f}s CPU | {executions:,} executions | {avg_per_exec:.2f}s avg exec | {pcttotal:.1f}% DB time | {cpu_pct:.1f}% CPU"
        
        if ash_dba_summary:
            return f"""
🔥 **{severity} PRIORITY** - SQL_ID: `{sql_id}`

**1️⃣ Why is this SQL problematic?**
{why_problematic}

**2️⃣ What is it doing to database?**
{what_doing}

**3️⃣ How much impact?**
{impact_magnitude}

{ash_dba_summary}

{quick_stats}
"""
        else:
            return f"""
🔥 **{severity} PRIORITY** - SQL_ID: `{sql_id}`

**1️⃣ Why is this SQL problematic?**
{why_problematic}

**2️⃣ What is it doing to database?**
{what_doing}

**3️⃣ How much impact?**
{impact_magnitude}

{quick_stats}
"""
    
    def _analyze_execution_pattern(self, executions: int, elapsed: float, 
                                  elapsed_per_exec: float, pcttotal: float) -> Dict[str, Any]:
        """3️⃣ Analyze and classify execution pattern with DBA insight"""
        
        pattern_type: str = ""
        pattern_description: str = ""
        is_high_frequency = False
        is_bursty = False
        is_sustained = False
        dba_assessment: str = ""
        
        # Determine pattern type and DBA assessment
        if executions > 5000:
            pattern_type = "EXTREME_HIGH_FREQUENCY"
            is_high_frequency = True
            pattern_description = f"**Extreme high-frequency pattern** - {executions:,} executions detected."
            dba_assessment = "This screams application-level issue. Likely a loop calling the same query repeatedly, or severe cache miss problem. The database is being hammered unnecessarily. Fix needs to happen in application code - implement caching, batch operations, or reduce call frequency."
        
        elif executions > 1000:
            pattern_type = "HIGH_FREQUENCY"
            is_high_frequency = True
            pattern_description = f"**High-frequency execution** - {executions:,} calls during analysis period."
            dba_assessment = "Consistent, high-volume pattern. This query is a workhorse but needs optimization. Even small improvements per execution will yield massive aggregate savings. Priority should be on making each execution faster through indexing or plan optimization."
        
        elif executions > 100 and elapsed_per_exec > 1.0:
            pattern_type = "SUSTAINED_SLOW_LOAD"
            is_sustained = True
            pattern_description = f"**Sustained load pattern** - {executions} executions, averaging {elapsed_per_exec:.2f}s each."
            dba_assessment = "This is creating continuous pressure on the system. Not a spike, but steady drain. Each execution is too slow. Likely full table scan or inefficient join. Need to fix the execution plan itself - indexes, statistics, or SQL rewrite."
        
        elif executions < 100 and elapsed > 50:
            pattern_type = "BURSTY_HIGH_IMPACT"
            is_bursty = True
            pattern_description = f"**Bursty/batch pattern** - only {executions} executions, but {elapsed:.1f}s total time."
            dba_assessment = "Low frequency but massive per-execution cost. Likely a report query, batch job, or data export. These don't run often but when they do, they lock up resources. Look for Cartesian joins, missing indexes on large tables, or unnecessary sorting operations."
        
        elif executions > 100 and elapsed_per_exec > 0.1:
            pattern_type = "MODERATE_SUSTAINED"
            is_sustained = True
            pattern_description = f"**Moderate sustained pattern** - {executions} executions averaging {elapsed_per_exec:.3f}s each."
            dba_assessment = "Consistent workload contributor. Not the worst, but definitely needs tuning. The combination of frequency and per-execution time creates cumulative impact. Optimize the plan and you'll see noticeable improvement."
        
        else:
            pattern_type = "FREQUENT_LIGHT_IMPACT"
            is_high_frequency: bool = executions > 500
            pattern_description = f"**Frequent but light impact** - {executions} fast executions creating aggregate load."
            dba_assessment = "Individual executions are fast, but volume creates cumulative impact. This is actually good news - query itself is efficient. The fix is reducing call frequency from application side, not SQL tuning."
        
        return {
            "pattern_type": pattern_type,
            "description": pattern_description,
            "dba_assessment": dba_assessment,
            "is_high_frequency": is_high_frequency,
            "is_bursty": is_bursty,
            "is_sustained": is_sustained,
            "executions": executions,
            "avg_time_per_exec": round(elapsed_per_exec, 4)
        }
    
    def _generate_dba_interpretation(self, sql_text: str, metrics: Dict, 
                                    workload_analysis: Dict, wait_events: List[Dict], 
                                    ash_analysis: Dict = None) -> str:
        """4️⃣ Generate DBA interpretation - explain like an experienced DBA"""
        
        interpretations = []
        
        elapsed = metrics.get('elapsed', 0)
        cpu = metrics.get('cpu', 0)
        executions = metrics.get('executions', 0)
        pctcpu = metrics.get('pctcpu', 0)
        pctio = metrics.get('pctio', 0)
        elapsed_per_exec = elapsed / executions if executions > 0 else 0
        
        # Get I/O percentage from ASH analysis if available
        ash_io_percent = 0
        if ash_analysis and "workload_breakdown" in ash_analysis:
            workload_breakdown = ash_analysis["workload_breakdown"]
            if "IO" in workload_breakdown:
                ash_io_percent = workload_breakdown["IO"].get("total_percent", 0)
        
        # Use ASH I/O percentage if available, otherwise fall back to AWR pctio
        io_percentage = ash_io_percent if ash_io_percent > 0 else pctio
        
        # CPU intensive analysis - DBA style
        if pctcpu > 85:
            interpretations.append("**🔴 CPU-Intensive SQL**")
            interpretations.append("")
            interpretations.append("High CPU usage means optimizer chose poor plan. Likely missing indexes causing full scans or stale stats misleading cardinality estimates. Check execution plan for inefficient operations.")
            interpretations.append("")
        elif cpu > 30:
            interpretations.append("**🟠 High CPU Consumption**")
            interpretations.append("")
            interpretations.append(f"Significant CPU consumption ({cpu:.1f}s) from intensive calculations. Full table scans, complex operations, or hash joins on non-indexed columns. Review execution plan for optimization opportunities.")
            interpretations.append("")
        
        # IO analysis - DBA style
        if io_percentage > 40:
            interpretations.append("**🟠 I/O-Heavy Operation**")
            interpretations.append("")
            interpretations.append(f"High I/O waits ({io_percentage:.1f}%) indicate excessive disk reads. Missing indexes forcing full table scans or inefficient data access patterns. Add proper indexes and review query structure.")
            interpretations.append("")
        
        # Execution pattern analysis - DBA style
        if executions > 2000 and elapsed_per_exec < 0.1:
            interpretations.append("**🔵 Fast Execution, High Frequency**")
            interpretations.append("")
            interpretations.append(f"Fast execution ({elapsed_per_exec:.4f}s) but called {executions:,} times. Application issue - inefficient loops, missing caching, or lack of batching. Review application logic to reduce call frequency.")
            interpretations.append("")
        elif executions < 50 and elapsed > 100:
            interpretations.append("**🔴 Slow Batch/Report Query**")
            interpretations.append("")
            interpretations.append(f"Heavyweight operation: {elapsed:.1f}s across {executions} executions. Structural issues like Cartesian joins, full scans on large tables, or suboptimal join algorithms. Review query design and indexing strategy.")
            interpretations.append("")
        
        # SQL text pattern analysis - DBA style
        if sql_text:
            sql_upper: str = sql_text.upper()
            
            if 'SELECT *' in sql_upper:
                interpretations.append("**⚠️ Selecting All Columns (SELECT *)**")
                interpretations.append("")
                interpretations.append("Retrieving all columns when you probably only need a few. This wastes I/O bandwidth and network overhead. Specify only needed columns.")
                interpretations.append("")
            
            if sql_upper.count('JOIN') >= 4:
                interpretations.append("**⚠️ Too Many Joins**")
                interpretations.append("")
                interpretations.append(f"Complex multi-join query ({sql_upper.count('JOIN')} joins) increases optimizer complexity. Verify all joins have proper indexes and consider breaking into simpler operations if performance degrades.")
                interpretations.append("")
            
            if 'WHERE' not in sql_upper and 'SELECT' in sql_upper and 'FROM' in sql_upper:
                interpretations.append("**🔴 No WHERE Clause**")
                interpretations.append("")
                interpretations.append("Query has no filtering conditions. This means FULL TABLE SCAN. If the table is large, you're reading millions of rows unnecessarily. Add WHERE clause to filter data.")
                interpretations.append("")
            
            if 'DISTINCT' in sql_upper and ('ORDER BY' in sql_upper or 'GROUP BY' in sql_upper):
                interpretations.append("**⚠️ Heavy DISTINCT with Sorting/Grouping**")
                interpretations.append("")
                interpretations.append("DISTINCT with ORDER BY or GROUP BY forces expensive sort operations. Check if DISTINCT is really needed - often it masks a bad join creating duplicates.")
                interpretations.append("")
        
        # Parallel / IO heavy check
        if elapsed > 60 and io_percentage < 20 and pctcpu > 60:
            interpretations.append("**🟡 Possible Parallel Query or Compute-Heavy**")
            interpretations.append("")
            interpretations.append("Long elapsed time with high CPU but low I/O suggests parallel processing or compute-heavy operations. Database performing in-memory computations, parallel execution, or large hash joins.")
            interpretations.append("")
        
        # Old stats / wrong cardinality
        if executions > 50 and (pctcpu > 50 or io_percentage > 30):
            interpretations.append("**🟡 Possible Stale Statistics**")
            interpretations.append("")
            interpretations.append("High resource usage with frequent execution suggests stale statistics misleading optimizer. Outdated row counts and data distribution causing poor execution plans. Run DBMS_STATS to refresh table statistics.")
            interpretations.append("")
        
        # Default interpretation
        if not interpretations:
            interpretations.append("**🟡 Performance Degradation Detected**")
            interpretations.append("")
            interpretations.append("Query requires execution plan review and optimization. Recommend running SQL Tuning Advisor and reviewing actual vs estimated rows in plan.")
            interpretations.append("")
        
        return "\n".join(interpretations)
    
    def _generate_dba_explanation(self, elapsed: float, cpu: float, executions: int, 
                                  elapsed_per_exec: float, pctcpu: float, io_percentage: float, 
                                  pcttotal: float) -> str:
        """
        Generate DBA-style human reasoning explanation
        User requirement: "High CPU + High Executions → consistent workload stressor"
        """
        
        explanations = []
        
        # Pattern 1: High CPU + High Executions
        if pctcpu >= 70 and executions >= 100:
            explanations.append("High CPU + High Executions → consistent workload stressor putting sustained pressure on system")
        elif pctcpu >= 50 and executions >= 100:
            explanations.append("Elevated CPU with frequent executions → ongoing performance drain on database resources")
        
        # Pattern 2: High Elapsed + Low execs  
        if elapsed >= 50 and executions < 100:
            explanations.append("High Elapsed + Low executions → few heavy queries causing significant database load")
        elif elapsed >= 20 and executions < 50:
            explanations.append("Long-running with few executions → batch/report query consuming excessive time")
        
        # Pattern 3: High IO wait (using corrected ASH-based percentage)
        if io_percentage >= 40:
            explanations.append("High IO wait → disk bound SQL likely due to missing indexes or full table scans")
        elif io_percentage >= 25:
            explanations.append("Elevated IO waits → inefficient data access pattern requiring index optimization")
        
        # Pattern 4: High frequency with good per-exec time
        if executions >= 1000 and elapsed_per_exec < 0.1:
            explanations.append("Very high frequency with fast execution → application-level optimization needed (caching/batching)")
        
        # Pattern 5: Workload domination
        if pcttotal >= 25:
            explanations.append(f"Dominant workload contribution ({pcttotal:.1f}% of DB time) → single SQL driving database load")
        elif pcttotal >= 10:
            explanations.append(f"Significant workload impact ({pcttotal:.1f}% of DB time) → major contributor to performance issues")
        
        # Default if no specific pattern
        if not explanations:
            if elapsed >= 20:
                explanations.append("Elevated elapsed time → requires execution plan review and SQL tuning")
            elif cpu >= 10:
                explanations.append("Notable CPU consumption → inefficient execution plan requiring optimization")
            else:
                explanations.append("Performance issue detected → requires DBA analysis and tuning")
        
        return " | ".join(explanations)
    
    def _link_to_wait_events(self, metrics: Dict, wait_events: List[Dict], 
                            ash_analysis: Dict) -> str:
        """5️⃣ Link SQL to wait events and ASH data - connect the dots"""
        
        pctcpu = metrics.get('pctcpu', 0)
        pctio = metrics.get('pctio', 0)
        elapsed = metrics.get('elapsed', 0)
        cpu = metrics.get('cpu', 0)
        
        # Get I/O percentage from ASH analysis if available
        ash_io_percent = 0
        if ash_analysis and "workload_breakdown" in ash_analysis:
            workload_breakdown = ash_analysis["workload_breakdown"]
            if "IO" in workload_breakdown:
                ash_io_percent = workload_breakdown["IO"].get("total_percent", 0)
        
        # Use ASH I/O percentage if available, otherwise fall back to AWR pctio
        io_percentage = ash_io_percent if ash_io_percent > 0 else pctio
        
        linkage_text = []
        linkage_text.append("**Wait Event / ASH Linkage:**")
        linkage_text.append("")
        
        # CPU linkage
        if pctcpu > 70:
            linkage_text.append("🔴 **Primary: CPU (ON CPU)**")
            linkage_text.append(f"   This SQL is spending {pctcpu:.1f}% of its time on CPU. In ASH, you'd see this session showing 'ON CPU' state.")
            linkage_text.append("   Translation: Database engine is actively working, not waiting for anything. Problem is execution plan efficiency.")
            linkage_text.append("")
        elif pctcpu > 40:
            linkage_text.append("🟠 **Significant: CPU Usage**")
            linkage_text.append(f"   Moderate CPU consumption ({pctcpu:.1f}%). ASH would show mix of ON CPU and wait events.")
            linkage_text.append("")
        
        # IO linkage (using corrected ASH-based percentage)
        if io_percentage > 40:
            linkage_text.append("🔴 **Primary: I/O Wait Events**")
            linkage_text.append(f"   High I/O wait percentage ({io_percentage:.1f}%). This SQL is waiting for data to be read from disk.")
            
            if wait_events:
                # Find relevant I/O wait events
                io_keywords: List[str] = ['read', 'write', 'i/o', 'db file', 'direct path']
                io_waits = [w for w in wait_events if any(keyword in w.get('statistic_name', '').lower() for keyword in io_keywords)]
                
                if io_waits:
                    top_io = io_waits[0]
                    linkage_text.append(f"   ASH Correlation: **{top_io.get('statistic_name')}** ({top_io.get('pct_of_db_time', 0):.1f}% of DB time)")
                    linkage_text.append("   This wait event indicates disk read bottleneck - missing indexes most likely cause.")
                else:
                    linkage_text.append("   ASH Correlation: I/O-related wait events (db file sequential read / scattered read)")
            else:
                linkage_text.append("   Expected ASH events: 'db file sequential read' (index) or 'db file scattered read' (full scan)")
            linkage_text.append("")
        elif io_percentage > 20:
            linkage_text.append("🟡 **Moderate: I/O Activity**")
            linkage_text.append(f"   Some I/O wait detected ({io_percentage:.1f}%). Not the primary bottleneck but contributing factor.")
            linkage_text.append("")
        
        # Concurrency / Latch linkage
        if wait_events:
            concurrency_keywords: List[str] = ['latch', 'enqueue', 'buffer busy', 'cursor', 'library cache']
            concurrency_waits = [w for w in wait_events if any(keyword in w.get('statistic_name', '').lower() for keyword in concurrency_keywords)]
            
            if concurrency_waits:
                linkage_text.append("🟠 **Concurrency Detected**")
                for cw in concurrency_waits[:2]:  # Top 2
                    linkage_text.append(f"   • {cw.get('statistic_name')} - contention for shared resources")
                linkage_text.append("   Indicates lock contention or buffer competition. Multiple sessions hitting same data.")
                linkage_text.append("")
        
        # Log file sync / commit
        if wait_events:
            commit_keywords: List[str] = ['log file sync', 'commit', 'log file parallel']
            commit_waits = [w for w in wait_events if any(keyword in w.get('statistic_name', '').lower() for keyword in commit_keywords)]
            
            if commit_waits:
                linkage_text.append("🟡 **Log File / Commit Activity**")
                linkage_text.append(f"   • {commit_waits[0].get('statistic_name')} detected")
                linkage_text.append("   Indicates commit frequency. If high, consider batching transactions or reducing commit frequency.")
                linkage_text.append("")
        
        # ASH dominant events correlation
        if ash_analysis and isinstance(ash_analysis, dict):
            dominant_events = ash_analysis.get('dominant_events', [])
            
            if dominant_events:
                linkage_text.append("📊 **ASH Analysis Correlation:**")
                
                for idx, event in enumerate(dominant_events[:3], 1):  # Top 3 events
                    event_name = event.get('event', 'Unknown')
                    # Fix: Use the correct key from ASH analyzer
                    pct_impact = event.get('total_percent_impact', 0.0)
                    linkage_text.append(f"   {idx}. **{event_name}** – {pct_impact:.2f}%")
                
                linkage_text.append("")
                linkage_text.append("   This SQL's behavior aligns with the dominant wait events seen in ASH data during the time window.")
                linkage_text.append("   Fixing this SQL will directly reduce the wait event pressure shown in ASH.")
                linkage_text.append("")
        
        # No specific wait events
        if len(linkage_text) <= 2:  # Only header
            linkage_text.append("ℹ️ **No Specific Wait Events Identified**")
            linkage_text.append("   Primary issue appears to be CPU/execution plan efficiency rather than external waits.")
            linkage_text.append("   Focus tuning efforts on query optimization, not infrastructure.")
            linkage_text.append("")
        
        return "\n".join(linkage_text)
    
    def _generate_dba_recommendations(self, sql_id: str, sql_text: str, metrics: Dict, 
                                     dba_interpretation: str, severity: str) -> Dict[str, Any]:
        """
        🛠️ Generate DBA-level recommendations - EVIDENCE-DRIVEN via Decision Engine
        
        This method now routes through the Decision Engine and Dynamic SQL Generator
        to produce recommendations that vary based on actual workload signals.
        
        CRITICAL: Same SQL_ID + different signals → different recommendations
        """
        
        # Use Decision Engine if available, otherwise fallback to legacy
        if DECISION_ENGINE_AVAILABLE:
            return self._generate_dynamic_recommendations(sql_id, sql_text, metrics, 
                                                          dba_interpretation, severity)
        else:
            return self._generate_legacy_recommendations(sql_id, sql_text, metrics, 
                                                         dba_interpretation, severity)
    
    def _generate_dynamic_recommendations(self, sql_id: str, sql_text: str, metrics: Dict,
                                          dba_interpretation: str, severity: str) -> Dict[str, Any]:
        """
        Evidence-driven recommendations using Decision Engine + Dynamic SQL Generator
        
        This method:
        1. Normalizes signals from metrics
        2. Evaluates through Decision Engine gates
        3. Generates dynamic SQL via DynamicSQLGenerator
        4. Formats output for UI contract
        """
        # Initialize engines
        decision_engine = DecisionEngine()
        sql_generator = DynamicSQLGenerator()
        
        # Extract and normalize signals
        elapsed = metrics.get('elapsed', 0)
        cpu = metrics.get('cpu', 0)
        executions = metrics.get('executions', 0)
        pctcpu = metrics.get('pctcpu', 0)
        pctio = metrics.get('pctio', 0)
        pcttotal = metrics.get('pcttotal', 0)
        elapsed_per_exec = elapsed / executions if executions > 0 else 0
        
        # Create normalized signal block
        signals = NormalizedSignals(
            sql_id=sql_id,
            executions=executions,
            total_elapsed=elapsed,
            avg_exec_time=elapsed_per_exec,
            cpu_time=cpu,
            cpu_pct=pctcpu,
            io_wait_pct=pctio,
            db_time_pct=pcttotal,
            sql_text=sql_text
        )
        
        # Evaluate through Decision Engine
        decision: DecisionResult = decision_engine.evaluate(signals)
        
        # Generate dynamic SQL commands
        generated_sql: List[GeneratedSQL] = sql_generator.generate_all(decision)
        
        # Generate dynamic action plan
        action_plan: DBAActionPlan = sql_generator.generate_action_plan(decision)
        
        # Map severity to priority (preserve UI contract)
        tuning_priority, priority_desc = self._map_severity_to_priority(severity)
        
        # Format "What DBA Should Do Next" from generated SQL
        next_steps: str = self._format_next_steps(decision, generated_sql, signals, dba_interpretation)
        
        # Format DBA Action Plan from dynamic generator
        formatted_action_plan: str = self._format_action_plan(action_plan, decision)
        
        # Calculate expected improvement based on category
        expected_improvement: str = self._calculate_expected_improvement(decision, severity)
        
        return {
            "tuning_priority": tuning_priority,
            "priority_description": priority_desc,
            "what_dba_should_do_next": next_steps,
            "dba_action_plan": formatted_action_plan,
            "expected_improvement": expected_improvement,
            # New evidence-driven fields
            "sql_category": decision.category.value,
            "allowed_actions": [a.value for a in decision.allowed_actions],
            "blocked_actions": [a.value for a in decision.blocked_actions],
            "why_shown": decision.why_shown,
            "why_hidden": decision.why_hidden
        }
    
    def _map_severity_to_priority(self, severity: str) -> tuple:
        """Map severity level to tuning priority with description"""
        if severity == "CRITICAL":
            return ("CRITICAL", "🔴 **CRITICAL** - Production impacting, requires immediate action")
        elif severity == "HIGH":
            return ("HIGH", "🟠 **HIGH** - Major performance drain, address within 24 hours")
        elif severity == "MEDIUM":
            return ("MEDIUM", "🟡 **MEDIUM** - Notable impact, schedule tuning this week")
        else:
            return ("LOW", "🟢 **LOW** - Minor optimization opportunity")
    
    def _format_next_steps(self, decision, generated_sql, signals, dba_interpretation: str) -> str:
        """
        Format 'What DBA Should Do Next' from dynamically generated SQL
        
        This replaces the hardcoded SQL templates with SQL generated
        based on the actual workload category and signals.
        """
        from engine.decision_engine import SQLCategory
        
        lines = []
        lines.append("**2️⃣ What DBA Should Do Next:**\n")
        
        # Add category-specific header
        category = decision.category
        if category == SQLCategory.BATCH_SQL:
            lines.append(f"**📊 Workload Category: BATCH/REPORT SQL**")
            lines.append(f"→ Detected slow per-execution ({signals.avg_exec_time:.2f}s avg) with low frequency ({signals.executions} execs)")
            lines.append(f"→ Focus: IO optimization and access path improvements")
            lines.append("")
        elif category == SQLCategory.CHATTY_SQL:
            lines.append(f"**⚡ Workload Category: CHATTY/OLTP SQL**")
            lines.append(f"→ Detected high frequency ({signals.executions:,} execs) with fast execution ({signals.avg_exec_time*1000:.1f}ms avg)")
            lines.append(f"→ Focus: Application-level optimization, NOT database tuning")
            lines.append("")
        elif category == SQLCategory.IO_BOUND_SQL:
            lines.append(f"**💾 Workload Category: IO-BOUND SQL**")
            lines.append(f"→ Detected high IO wait ({signals.io_wait_pct:.1f}%)")
            lines.append(f"→ Focus: Index optimization and access path analysis")
            lines.append("")
        elif category == SQLCategory.CPU_BOUND_SQL:
            lines.append(f"**🔥 Workload Category: CPU-BOUND SQL**")
            lines.append(f"→ Detected high CPU ({signals.cpu_pct:.1f}%) with low IO wait ({signals.io_wait_pct:.1f}%)")
            lines.append(f"→ Focus: Query complexity reduction and execution plan optimization")
            lines.append("")
        elif category == SQLCategory.MIXED_PROFILE_SQL:
            lines.append(f"**🔄 Workload Category: MIXED PROFILE SQL**")
            lines.append(f"→ Multiple concerning characteristics detected")
            lines.append(f"→ Focus: Comprehensive tuning approach")
            lines.append("")
        else:
            lines.append(f"**ℹ️ Workload Category: LOW PRIORITY**")
            lines.append(f"→ No critical tuning actions required at this time")
            lines.append("")
        
        # Add dynamically generated SQL commands
        for sql_cmd in generated_sql:
            lines.append(f"**{sql_cmd.action}:**")
            lines.append(f"*Intent: {sql_cmd.intent}*")
            lines.append(f"```sql")
            lines.append(sql_cmd.sql)
            lines.append(f"```")
            lines.append(f"→ {sql_cmd.explanation}")
            lines.append("")
        
        # Add blocked actions explanation (transparency)
        if decision.blocked_actions and decision.why_hidden:
            lines.append("**⚠️ Actions NOT Recommended for This Workload:**")
            for reason in decision.why_hidden:
                lines.append(f"• {reason}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_action_plan(self, action_plan, decision) -> str:
        """
        Format DBA Action Plan from dynamically generated plan
        
        This uses the DBAActionPlan from DynamicSQLGenerator which
        produces timeframe-based actions specific to the workload category.
        """
        lines = []
        lines.append("**3️⃣ DBA Action Plan:**\n")
        
        lines.append("**🔥 IMMEDIATE (Next 1 hour):**")
        for action in action_plan.immediate:
            lines.append(f"• {action}")
        lines.append("")
        
        lines.append("**⚡ SHORT-TERM (Today/Tomorrow):**")
        for action in action_plan.short_term:
            lines.append(f"• {action}")
        lines.append("")
        
        lines.append("**📅 MEDIUM-TERM (This Week):**")
        for action in action_plan.medium_term:
            lines.append(f"• {action}")
        lines.append("")
        
        lines.append("**🔄 LONG-TERM (Ongoing):**")
        for action in action_plan.long_term:
            lines.append(f"• {action}")
        lines.append("")
        
        # Add priority reasoning for transparency
        if action_plan.priority_reasoning:
            lines.append("**📋 Priority Reasoning:**")
            for reason in action_plan.priority_reasoning:
                lines.append(f"→ {reason}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _calculate_expected_improvement(self, decision, severity: str) -> str:
        """
        Calculate expected improvement based on category and severity
        
        This provides category-specific improvement estimates rather than
        generic percentages.
        """
        from engine.decision_engine import SQLCategory
        
        category = decision.category
        signals = decision.signals
        
        if category == SQLCategory.CHATTY_SQL:
            # Chatty SQL - improvements come from application, not DB
            return (f"Expected Improvement: Application-level caching could reduce "
                    f"database calls by 50-80% (currently {signals.executions:,} executions). "
                    f"Database tuning NOT recommended - query already executes in {signals.avg_exec_time*1000:.1f}ms.")
        
        elif category == SQLCategory.BATCH_SQL:
            # Batch SQL - IO improvements
            if signals.io_wait_pct > 80:
                return (f"Expected Improvement: Index optimization could reduce elapsed time by 60-80% "
                        f"(currently {signals.io_wait_pct:.1f}% IO wait indicating likely full table scans).")
            else:
                return (f"Expected Improvement: 30-50% reduction in elapsed time possible through "
                        f"execution plan optimization for batch workload pattern.")
        
        elif category == SQLCategory.IO_BOUND_SQL:
            # IO-bound - index focus
            return (f"Expected Improvement: Proper indexing could reduce IO wait from {signals.io_wait_pct:.1f}% "
                    f"to <20%, yielding 40-70% elapsed time reduction.")
        
        elif category == SQLCategory.CPU_BOUND_SQL:
            # CPU-bound - query complexity
            return (f"Expected Improvement: Query simplification or hints could reduce CPU consumption by 30-50%. "
                    f"Currently at {signals.cpu_pct:.1f}% CPU utilization.")
        
        else:
            # Default/Mixed
            if severity == "CRITICAL":
                return "Expected Improvement: 40-70% reduction in elapsed time with proper optimization strategy."
            elif severity == "HIGH":
                return "Expected Improvement: 30-50% reduction in elapsed time with targeted tuning."
            else:
                return "Expected Improvement: 20-40% performance improvement possible with optimization."
    
    def _generate_legacy_recommendations(self, sql_id: str, sql_text: str, metrics: Dict, 
                                         dba_interpretation: str, severity: str) -> Dict[str, Any]:
        """
        Legacy recommendation generation (fallback when Decision Engine unavailable)
        
        WARNING: This is the OLD templated approach. It should only be used
        if the Decision Engine cannot be imported.
        """
        
        elapsed = metrics.get('elapsed', 0)
        cpu = metrics.get('cpu', 0)
        executions = metrics.get('executions', 0)
        pctcpu = metrics.get('pctcpu', 0)
        pctio = metrics.get('pctio', 0)
        elapsed_per_exec = elapsed / executions if executions > 0 else 0
        
        # 1️⃣ Tuning Priority (Map severity to priority)
        if severity == "CRITICAL":
            tuning_priority = "CRITICAL"
            priority_desc = "🔴 **CRITICAL** - Production impacting, requires immediate action"
        elif severity == "HIGH":
            tuning_priority = "HIGH"
            priority_desc = "🟠 **HIGH** - Major performance drain, address within 24 hours"
        elif severity == "MEDIUM":
            tuning_priority = "MEDIUM"
            priority_desc = "🟡 **MEDIUM** - Notable impact, schedule tuning this week"
        else:
            tuning_priority = "LOW"
            priority_desc = "🟢 **LOW** - Minor optimization opportunity"
        
        # 2️⃣ What DBA should do next - specific, actionable recommendations
        next_steps = []
        next_steps.append("**2️⃣ What DBA Should Do Next:**\n")
        
        # Recommendation: Check indexes
        if pctio > 30 or 'missing index' in dba_interpretation.lower() or 'full table scan' in dba_interpretation.lower():
            next_steps.append("**🔍 Check for Missing Indexes:**")
            next_steps.append(f"```sql")
            next_steps.append(f"-- Run SQL Access Advisor")
            next_steps.append(f"EXEC DBMS_ADVISOR.QUICK_TUNE(DBMS_ADVISOR.SQLACCESS_ADVISOR, '{sql_id}');")
            next_steps.append(f"")
            next_steps.append(f"-- Or manually check execution plan")
            next_steps.append(f"SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('{sql_id}', NULL, 'ALLSTATS LAST'));")
            next_steps.append(f"-- Look for: TABLE ACCESS FULL, high A-Rows vs E-Rows")
            next_steps.append(f"```")
            next_steps.append("✅ **Action:** Create selective indexes on columns in WHERE/JOIN clauses")
            next_steps.append("")
        
        # Recommendation: SQL rewrite or hints
        if cpu > 20 or pctcpu > 70 or elapsed_per_exec > 1.0:
            next_steps.append("**✏️ Consider SQL Rewrite or Hints:**")
            next_steps.append("")
            
            suggestions = []
            if sql_text and 'SELECT *' in sql_text.upper():
                suggestions.append("change SELECT * to specific columns")
            if sql_text and sql_text.upper().count('JOIN') >= 4:
                suggestions.append("review join order placing smallest tables first")
                suggestions.append("consider USE_HASH or USE_NL hints if optimizer chooses wrong method")
            if executions > 1000:
                suggestions.append("add WHERE clause filters to reduce rows processed")
            
            if suggestions:
                next_steps.append("Key optimization opportunities include: " + ", ".join(suggestions) + ". These modifications can significantly improve query performance by reducing data processing overhead and guiding the optimizer toward more efficient execution paths.")
                next_steps.append("")
            
            next_steps.append(f"```sql")
            next_steps.append(f"-- Try different hint")
            next_steps.append(f"SELECT /*+ INDEX(table_name index_name) */ ...")
            next_steps.append(f"-- Or")
            next_steps.append(f"SELECT /*+ USE_HASH(t1 t2) */ ...")
            next_steps.append(f"```")
            next_steps.append("")
        
        # Recommendation: Check bind variables
        if executions > 500:
            next_steps.append("**🔗 Verify Bind Variable Usage:**")
            next_steps.append(f"```sql")
            next_steps.append(f"-- Check if SQL uses bind variables")
            next_steps.append(f"SELECT sql_id, sql_text, executions")
            next_steps.append(f"FROM v$sql")
            next_steps.append(f"WHERE sql_id = '{sql_id}';")
            next_steps.append(f"")
            next_steps.append(f"-- If you see many similar SQLs with literals, enforce bind variables")
            next_steps.append(f"```")
            next_steps.append("✅ **Action:** Use bind variables instead of literals to enable cursor sharing")
            next_steps.append("")
        
        # Recommendation: Gather stats
        if elapsed > 30 or 'stale statistics' in dba_interpretation.lower():
            next_steps.append("**📊 Gather Fresh Optimizer Statistics:**")
            next_steps.append(f"```sql")
            next_steps.append(f"-- Check when stats were last gathered")
            next_steps.append(f"SELECT table_name, last_analyzed, num_rows, stale_stats")
            next_steps.append(f"FROM dba_tab_statistics")
            next_steps.append(f"WHERE owner = '<SCHEMA>';")
            next_steps.append(f"")
            next_steps.append(f"-- Gather fresh stats")
            next_steps.append(f"EXEC DBMS_STATS.GATHER_SCHEMA_STATS(")
            next_steps.append(f"  ownname => '<SCHEMA>',")
            next_steps.append(f"  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,")
            next_steps.append(f"  method_opt => 'FOR ALL COLUMNS SIZE AUTO',")
            next_steps.append(f"  degree => 4")
            next_steps.append(f");")
            next_steps.append(f"```")
            next_steps.append("✅ **Action:** Schedule weekly stats gathering job")
            next_steps.append("")
        
        # Recommendation: Run SQL Tuning Advisor
        if severity in ['HIGH', 'CRITICAL']:
            next_steps.append("**🤖 Run SQL Tuning Advisor:**")
            next_steps.append(f"```sql")
            next_steps.append(f"-- Create tuning task")
            next_steps.append(f"DECLARE")
            next_steps.append(f"  l_task_name VARCHAR2(30) := 'TUNE_{sql_id}';")
            next_steps.append(f"BEGIN")
            next_steps.append(f"  DBMS_SQLTUNE.CREATE_TUNING_TASK(")
            next_steps.append(f"    sql_id      => '{sql_id}',")
            next_steps.append(f"    task_name   => l_task_name,")
            next_steps.append(f"    description => 'Tuning task for SQL {sql_id}'")
            next_steps.append(f"  );")
            next_steps.append(f"  ")
            next_steps.append(f"  -- Execute it")
            next_steps.append(f"  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(l_task_name);")
            next_steps.append(f"END;")
            next_steps.append(f"/")
            next_steps.append(f"")
            next_steps.append(f"-- View recommendations")
            next_steps.append(f"SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK('TUNE_{sql_id}') FROM DUAL;")
            next_steps.append(f"```")
            next_steps.append("✅ **Action:** Review and implement SQL Profile if recommended")
            next_steps.append("")
        
        # 3️⃣ Short "DBA Action Plan"
        action_plan = []
        action_plan.append("**3️⃣ DBA Action Plan:**\n")
        
        action_plan.append("**🔥 IMMEDIATE (Next 1 hour):**")
        action_plan.append(f"1. Capture current execution plan: `DBMS_XPLAN.DISPLAY_CURSOR('{sql_id}')`")
        action_plan.append(f"2. Check table statistics: `SELECT last_analyzed FROM dba_tables WHERE ...`")
        action_plan.append(f"3. Identify the bottleneck operation (full scan? bad join? sort?)")
        action_plan.append("")
        
        action_plan.append("**⚡ SHORT-TERM (Today/Tomorrow):**")
        if pctio > 30:
            action_plan.append("• Create missing indexes identified in execution plan")
        if executions > 2000:
            action_plan.append("• Meet with application team - discuss caching or batching options")
        if elapsed > 60:
            action_plan.append("• Run SQL Tuning Advisor and review recommendations")
        action_plan.append("• Test fixes in DEV environment first")
        action_plan.append("• Validate improvement with before/after AWR snapshots")
        action_plan.append("")
        
        action_plan.append("**📅 MEDIUM-TERM (This Week):**")
        action_plan.append("• Deploy validated fixes to PROD during maintenance window")
        action_plan.append("• Monitor SQL performance for 24-48 hours post-fix")
        action_plan.append("• Set up alert threshold if this SQL spikes again")
        if sql_text and sql_text.upper().count('JOIN') >= 4:
            action_plan.append("• Consider breaking complex query into smaller, optimized queries")
        action_plan.append("")
        
        action_plan.append("**🔄 LONG-TERM (Ongoing):**")
        action_plan.append("• Review AWR reports weekly - ensure this SQL stays below thresholds")
        action_plan.append("• Schedule automated statistics gathering (weekly)")
        action_plan.append("• Document the fix in knowledge base for future reference")
        if executions > 5000:
            action_plan.append("• Architectural review: Why is this query called so frequently?")
        action_plan.append("")
        
        # Expected improvement estimate
        expected_improvement = "Expected Improvement: "
        if severity == "CRITICAL":
            expected_improvement += "40-70% reduction in elapsed time with proper indexing/rewrite"
        elif severity == "HIGH":
            expected_improvement += "30-50% reduction in elapsed time with optimization"
        else:
            expected_improvement += "20-40% performance improvement possible"
        
        return {
            "tuning_priority": tuning_priority,
            "priority_description": priority_desc,
            "what_dba_should_do_next": "\n".join(next_steps),
            "dba_action_plan": "\n".join(action_plan),
            "expected_improvement": expected_improvement
        }
    
    def _generate_dba_conclusion(self, findings: List[Dict], workload_analysis: Dict) -> str:
        """Generate intelligent AI-based conclusion - short, smart, adaptive"""
        
        if not findings:
            return """
🟢 **NO HIGH-RISK SQL IDENTIFIED**

AI Analysis Complete: System workload appears healthy.
• All query patterns within acceptable thresholds
• No immediate tuning targets detected
• Continue standard monitoring protocols
"""
        
        # Extract key metrics for intelligent analysis
        critical_count: int = sum(1 for f in findings if f['severity'] == 'CRITICAL')
        high_count: int = sum(1 for f in findings if f['severity'] == 'HIGH')
        medium_count: int = sum(1 for f in findings if f['severity'] == 'MEDIUM')
        total_count: int = len(findings)
        
        # Analyze dominant issue types from findings
        cpu_dominant: int = sum(1 for f in findings if 'CPU' in str(f.get('explanation', '')))
        workload_dominant: int = sum(1 for f in findings if 'workload' in str(f.get('explanation', '')).lower())
        frequency_issues: int = sum(1 for f in findings if 'frequency' in str(f.get('explanation', '')).lower())
        
        # Calculate expected impact
        total_db_impact: int = sum(f.get('technical_parameters', {}).get('contribution_to_db_time_pct', 0) for f in findings)
        avg_cpu_pct: float = sum(f.get('technical_parameters', {}).get('cpu_percentage', 0) for f in findings) / total_count
        
        # Generate intelligent conclusions
        conclusion_parts = []
        
        # AI-driven severity assessment
        if critical_count > 0:
            conclusion_parts.append(f"🔴 **Found {critical_count} CRITICAL issue{'s' if critical_count > 1 else ''}.** Production-impacting queries need immediate action.")
        elif high_count > 0:
            conclusion_parts.append(f"🟠 **Found {high_count} HIGH priority issue{'s' if high_count > 1 else ''}.** These queries are causing notable performance degradation.")
        else:
            conclusion_parts.append(f"🟡 **Identified {medium_count} MEDIUM priority issue{'s' if medium_count > 1 else ''}.** Performance optimization opportunities detected.")
        
        # Intelligent pattern recognition
        if total_db_impact > 50:
            conclusion_parts.append(f"• **High Impact Pattern**: {total_db_impact:.1f}% DB time consumption - major workload contributor")
        elif workload_dominant >= total_count * 0.6:
            conclusion_parts.append("• **Workload Concentration**: Issues clustered in high-impact queries")
        
        if cpu_dominant >= total_count * 0.5 and avg_cpu_pct > 70:
            conclusion_parts.append(f"• **CPU-Bound System**: Average {avg_cpu_pct:.0f}% CPU usage - execution plan optimization needed")
        elif frequency_issues >= total_count * 0.5:
            conclusion_parts.append("• **Frequency Pattern**: High-execution queries detected - application-level optimization required")
        
        # Smart RCA insight
        if total_count == 1:
            conclusion_parts.append("• **Focused Problem**: Single SQL root cause identified - targeted fix will yield significant improvement")
        elif total_count == 2:
            conclusion_parts.append("• **Dual Bottleneck**: Two primary performance drivers - systematic approach recommended")
        else:
            conclusion_parts.append(f"• **Multiple Targets**: {total_count} bottlenecks identified - prioritize by severity score")
        
        # Expected outcome prediction
        if critical_count > 0 or total_db_impact > 40:
            conclusion_parts.append("• **Expected Results**: 40-60% performance improvement achievable with proper tuning")
        elif high_count > 0:
            conclusion_parts.append("• **Expected Results**: 25-40% performance gains expected from optimization")
        else:
            conclusion_parts.append("• **Expected Results**: 15-25% improvement potential through tuning")
        
        # AI recommendation priority
        if critical_count > 0:
            conclusion_parts.append("• **AI Recommendation**: Deploy fixes in production maintenance window within 24 hours")
        elif high_count > 0:
            conclusion_parts.append("• **AI Recommendation**: Schedule optimization work this week - measurable user impact")
        else:
            conclusion_parts.append("• **AI Recommendation**: Include in next performance tuning cycle")
        
        conclusion_parts.append("")
        conclusion_parts.append("— *AI DBA Expert Analysis Complete* 🤖")
        
        return "\n".join(conclusion_parts)

