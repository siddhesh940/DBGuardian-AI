import json
import os
from typing import Any, Dict, List, Optional, Tuple

from engine.awr_analyzer import AWRAnalyzer
from engine.ash_analyzer import ASHAnalyzer
from engine.time_window_detector import TimeWindowDetector
from engine.dba_expert_engine import DBAExpertEngine


# ---------------------------------------------------------
# RCA LABELING HELPER (NO NEW MATH - LABELS ONLY)
# ---------------------------------------------------------
def derive_primary_rca(cpu_pct: float, io_pct: float) -> Tuple[str, str]:
    """
    Derive the primary root cause label from already-computed metrics.
    
    This function does NOT recompute any metrics.
    It only labels existing results for explicit RCA presentation.
    
    Args:
        cpu_pct: Already computed CPU percentage (0-100)
        io_pct: Already computed IO percentage (0-100)
    
    Returns:
        Tuple of (primary_rca_label, reason_string)
    """
    if cpu_pct >= 80 and io_pct <= 5:
        return "CPU_BOUND_SQL", "Single SQL consumed majority DB Time with minimal IO wait"
    if io_pct >= 40:
        return "IO_BOUND_WORKLOAD", "High IO wait dominated DB Time"
    return "MIXED_WORKLOAD", "CPU and IO jointly contributed to load"


class RCAEngine:
    def __init__(self, csv_dir, time_filter=None, result_dir=None, username=None, 
                 html_file_path: Optional[str] = None) -> None:
        """
        csv_dir  → user specific parsed CSV path
        result_dir → user specific results folder (optional)
        username → username for data integrity validation (CRITICAL)
        html_file_path → path to the AWR HTML file for authoritative time extraction (optional)
        """

        self.csv_dir = csv_dir
        self.time_filter = time_filter
        self.username = username
        self.html_file_path: str | None = html_file_path
        self.snapshot_metadata = None

        # Debug: Verify correct path
        print(f"🔍 RCA Engine initialized with CSV directory: {csv_dir}")
        if username:
            print(f"👤 Username: {username} (Data integrity validation ENABLED)")
        else:
            print("⚠️  Username not provided (Data integrity validation DISABLED)")
            
        if os.path.exists(csv_dir):
            csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
            print(f"📂 Found {len(csv_files)} CSV files: {csv_files}")
        else:
            print(f"⚠️  CSV directory does not exist: {csv_dir}")

        # If result_dir not passed, fallback to csv_dir/results
        self.result_dir = result_dir or os.path.join(csv_dir, "..", "results")

        self.awr_analyzer = AWRAnalyzer(csv_dir, time_filter=time_filter)
        self.ash_analyzer: ASHAnalyzer = ASHAnalyzer(csv_dir, time_filter=time_filter)
        self.window_detector: TimeWindowDetector = TimeWindowDetector(csv_dir, html_file_path)  # Pass HTML for CPU
        
        # Parse snapshot metadata from HTML file if provided
        if html_file_path and os.path.exists(html_file_path):
            self._parse_snapshot_metadata(html_file_path)
        
        # Initialize DBA Expert Engine with DATA INTEGRITY VALIDATION
        if username:
            self.dba_expert = DBAExpertEngine(user_csv_dir=csv_dir, username=username)
        else:
            # Fallback for testing (not recommended for production)
            self.dba_expert = DBAExpertEngine()

    # ---------------------------------------------------------
    # SNAPSHOT METADATA PARSING (AUTHORITATIVE TIME SOURCE)
    # ---------------------------------------------------------
    def _parse_snapshot_metadata(self, html_file_path: str) -> None:
        """
        Parse authoritative snapshot metadata from AWR HTML file.
        This provides the single source of truth for time windows.
        """
        try:
            from parsers.snapshot_metadata_parser import SnapshotMetadataParser
            parser = SnapshotMetadataParser(html_file_path)
            self.snapshot_metadata: Dict[str, Any] = parser.parse()
            
            if self.snapshot_metadata and self.snapshot_metadata.get("parse_success"):
                print(f"✅ Snapshot metadata parsed successfully from HTML")
                print(f"   Begin: {self.snapshot_metadata.get('begin_time')}")
                print(f"   End: {self.snapshot_metadata.get('end_time')}")
            else:
                print(f"⚠️  Could not parse snapshot metadata from HTML file")
        except Exception:
            import sys
            e: BaseException | None = sys.exc_info()[1]
            print(f"❌ Error parsing snapshot metadata: {e}")
            self.snapshot_metadata = None
    
    def _get_authoritative_analysis_window(self) -> str:
        """
        Get the authoritative analysis window display string.
        Uses snapshot metadata if available, falls back to time_filter.
        """
        # Priority 1: Use snapshot metadata if available
        if self.snapshot_metadata and self.snapshot_metadata.get("parse_success"):
            window_data: Dict[str, Any] = self.window_detector.get_analysis_window_from_metadata(
                self.snapshot_metadata
            )
            if window_data.get("display_window") and window_data["display_window"] != "--":
                return window_data["display_window"]
        
        # Priority 2: Use time_filter if provided
        if self.time_filter and self.time_filter.get("time_window"):
            return self.time_filter["time_window"]
        
        # Fallback
        return "--"
    
    def _get_cpu_percentage(self) -> Optional[float]:
        """
        Get CPU percentage from authoritative AWR source.
        
        CRITICAL PRIORITY ORDER:
        1. PRIMARY: Host CPU → 100 - %Idle (from AWR HTML)
        2. SECONDARY: Instance CPU → %Busy CPU (only if Host CPU missing)
        
        Returns None if data is not available.
        """
        if not self.snapshot_metadata or not self.snapshot_metadata.get("parse_success"):
            return None
        
        # PRIMARY SOURCE: Host CPU → 100 - %Idle
        if self.snapshot_metadata.get("host_cpu_idle_pct") is not None:
            calculated_cpu: float = 100.0 - float(self.snapshot_metadata["host_cpu_idle_pct"])
            return min(100.0, max(0.0, round(calculated_cpu, 1)))
        
        # SECONDARY SOURCE: Instance CPU → %Busy CPU (only if Host CPU missing)
        if self.snapshot_metadata.get("instance_cpu_busy_pct") is not None:
            cpu_pct = float(self.snapshot_metadata["instance_cpu_busy_pct"])
            return min(100.0, round(cpu_pct, 1))
        
        return None

    # ---------------------------------------------------------
    # MAIN RCA EXECUTION
    # ---------------------------------------------------------
    def run(self):
        # ASH analysis
        ash_results = self._analyze_ash_data()

        # Detect high load periods
        high_load_periods = self.window_detector.detect_high_load_periods()

        # If user selected time window → filter output summary
        if self.time_filter:
            filtered_periods = self._filter_periods_by_time(high_load_periods)
        else:
            filtered_periods = high_load_periods

        # AWR Top SQL + Wait Events
        top_sql, raw_sql = self.awr_analyzer.top_sql()
        awr_waits = self.awr_analyzer.top_wait_events()
        
        # ========================================================
        # 🎯 DBA EXPERT ANALYSIS - ONLY PROBLEMATIC SQL
        # ========================================================
        dba_analysis: Dict[str, Any] = self.dba_expert.analyze_workload(
            top_sql=top_sql,
            raw_sql=raw_sql,
            wait_events=awr_waits,
            ash_analysis=ash_results.get("analysis", {})
        )

        # Extract problematic SQL from DBA expert analysis for UI display
        problematic_sql = []
        if dba_analysis and "findings" in dba_analysis:
            for finding in dba_analysis["findings"]:
                # Extract the key fields UI needs from technical_parameters
                tech_params = finding.get("technical_parameters", {})
                problematic_sql.append({
                    "sql_id": tech_params.get("sql_id", finding.get("sql_id", "Unknown")),
                    "elapsed": tech_params.get("elapsed", 0),
                    "cpu": tech_params.get("cpu", 0),
                    "executions": tech_params.get("executions", 0),
                    "elapsed_per_exec": tech_params.get("avg_time", 0),
                    "risk": tech_params.get("risk_level", finding.get("severity", "MEDIUM"))
                })
        
        result = {
            "summary": {
                "high_load_periods": filtered_periods,
                "detected_peak": filtered_periods[0] if filtered_periods else None,
                "ash_summary": ash_results.get("summary", {})
            },
            "ash_analysis": ash_results.get("analysis", {}),
            
            # Problematic SQL for UI (maps to DBA expert findings)
            "top_sql": problematic_sql,
            
            # DBA Expert Analysis (Primary Output)
            "dba_expert_analysis": dba_analysis,
            
            # Keep raw data for reference (but hidden from main view)
            "_raw_data": {
                "top_sql_all": top_sql,
                "top_sql_raw": raw_sql,
                "top_wait_events": awr_waits
            }
        }

        # Metadata
        if self.time_filter:
            result["time_filter_applied"] = True
            result["primary_analysis"] = "ASH" if ash_results.get("has_data", False) else "AWR"
        else:
            result["time_filter_applied"] = False
            result["primary_analysis"] = "AWR"

        # ===== AUTHORITATIVE ANALYSIS WINDOW (Single Source of Truth) =====
        result["analysis_window"] = self._get_authoritative_analysis_window()
        
        # Add CPU percentage if available (capped at 100%)
        cpu_pct: float | None = self._get_cpu_percentage()
        if cpu_pct is not None:
            result["cpu_percentage"] = cpu_pct

        # ===== EXPLICIT ROOT CAUSE ANALYSIS (PRESENTATION ONLY) =====
        # This derives RCA labels from already-computed values.
        # NO new calculations - only labeling for explicit RCA presentation.
        
        # Get CPU and IO percentages from existing breakdown
        breakdown = ash_results.get("analysis", {}).get("workload_breakdown", {})
        rca_cpu_pct: float = breakdown.get("CPU", {}).get("total_percent", 0.0)
        rca_io_pct: float = breakdown.get("IO", {}).get("total_percent", 0.0)
        
        # Derive primary RCA label (no new math)
        primary_rca, primary_reason = derive_primary_rca(rca_cpu_pct, rca_io_pct)
        
        # Derive secondary causes (label only - no new detection logic)
        secondary_rcas: List[str] = []
        
        # Check for missing index from DBA expert findings
        if dba_analysis and "findings" in dba_analysis:
            for finding in dba_analysis["findings"]:
                finding_type = finding.get("type", "").upper()
                if "INDEX" in finding_type or "MISSING_INDEX" in finding_type:
                    if "MISSING_INDEX" not in secondary_rcas:
                        secondary_rcas.append("MISSING_INDEX")
                if "FULL_TABLE_SCAN" in finding_type or "FTS" in finding_type:
                    if "FULL_TABLE_SCAN" not in secondary_rcas:
                        secondary_rcas.append("FULL_TABLE_SCAN")
        
        # Check for concurrency issues from breakdown
        conc_pct: float = breakdown.get("Concurrency", {}).get("total_percent", 0.0)
        if conc_pct > 20 and "CONCURRENCY_CONTENTION" not in secondary_rcas:
            secondary_rcas.append("CONCURRENCY_CONTENTION")
        
        # Add explicit root_cause to result (new key only - no existing keys modified)
        result["root_cause"] = {
            "primary": primary_rca,
            "secondary": secondary_rcas,
            "confidence": "HIGH" if (rca_cpu_pct > 0 or rca_io_pct > 0) else "LOW",
            "reason": primary_reason
        }

        return result

    # ---------------------------------------------------------
    # ASH ANALYSIS LOGIC
    # ---------------------------------------------------------
    def _analyze_ash_data(self):
        try:
            summary = self.ash_analyzer.get_time_window_summary()
            dominant_events = self.ash_analyzer.analyze_dominant_events(limit=10)
            spikes = self.ash_analyzer.detect_activity_spikes(threshold_percent=3.0)
            breakdown = self.ash_analyzer.get_cpu_vs_io_breakdown()

            has_data = (
                len(dominant_events) > 0 or
                summary.get("filtered_data_points", 0) > 0
            )

            return {
                "has_data": has_data,
                "summary": summary,
                "analysis": {
                    "dominant_events": dominant_events,
                    "activity_spikes": spikes,
                    "workload_breakdown": breakdown,
                    "recommendations": self._generate_ash_recommendations(
                        dominant_events, spikes, breakdown
                    )
                }
            }
        except Exception:
            import sys
            e: BaseException | None = sys.exc_info()[1]
            print("Error analyzing ASH data:", str(e))
            return {
                "has_data": False,
                "summary": {},
                "analysis": {
                    "error": str(e),
                    "dominant_events": [],
                    "activity_spikes": [],
                    "workload_breakdown": {},
                    "recommendations": []
                }
            }

    # ---------------------------------------------------------
    # ASH RECOMMENDATION ENGINE
    # ---------------------------------------------------------
    def _generate_ash_recommendations(self, dominant_events, spikes, breakdown):
        recommendations = []

        if not dominant_events:
            recommendations.append({
                "type": "info",
                "title": "No significant activity detected",
                "description": "The selected time window shows minimal database activity.",
                "priority": "low"
            })
            return recommendations

        top_event = dominant_events[0]

        # CPU Load
        if top_event.get("event_class") == "CPU" and top_event.get("total_percent_impact", 0) > 20:
            recommendations.append({
                "type": "performance",
                "title": "High CPU Utilization Detected",
                "description": f"CPU activity accounts for {top_event.get('total_percent_impact')}% workload.",
                "priority": "high",
                "event": top_event.get("event")
            })

        # IO Load
        elif top_event.get("event_class") == "IO" and top_event.get("total_percent_impact", 0) > 15:
            recommendations.append({
                "type": "performance",
                "title": "I/O Bottleneck Detected",
                "description": f"I/O workload: {top_event.get('total_percent_impact')}%.",
                "priority": "high",
                "event": top_event.get("event")
            })

        # Concurrency
        elif top_event.get("event_class") == "Concurrency":
            recommendations.append({
                "type": "concurrency",
                "title": "Concurrency Issues Detected",
                "description": f"Lock/Latch contention detected: {top_event.get('event')}",
                "priority": "medium"
            })

        # Spikes
        if spikes:
            high_spikes = [s for s in spikes if s.get("percent_impact", 0) > 10]
            if high_spikes:
                recommendations.append({
                    "type": "spike",
                    "title": "Activity Spikes Detected",
                    "description": f"Found {len(high_spikes)} high impact spikes.",
                    "priority": "medium",
                    "spikes": high_spikes[:3]
                })

        # Workload Breakdown
        if breakdown:
            cpu = breakdown.get("CPU", {}).get("total_percent", 0)
            io = breakdown.get("IO", {}).get("total_percent", 0)
            conc = breakdown.get("Concurrency", {}).get("total_percent", 0)

            if cpu > 50:
                recommendations.append({
                    "type": "workload",
                    "title": "CPU-Intensive Workload",
                    "description": f"{cpu}% CPU workload.",
                    "priority": "medium"
                })
            elif io > 40:
                recommendations.append({
                    "type": "workload",
                    "title": "IO-Intensive Workload",
                    "description": f"{io}% IO workload.",
                    "priority": "medium"
                })
            elif conc > 30:
                recommendations.append({
                    "type": "workload",
                    "title": "Concurrency Issues",
                    "description": f"{conc}% concurrency load.",
                    "priority": "high"
                })

        return recommendations

    # ---------------------------------------------------------
    # APPLY TIME FILTER LABEL
    # ---------------------------------------------------------
    def _filter_periods_by_time(self, periods):
        if not self.time_filter:
            return periods

        filtered = []
        for p in periods:
            obj = dict(p)
            obj["filtered_by_time"] = True
            filtered.append(obj)

        return filtered

    # ---------------------------------------------------------
    # SAVE RESULT → USER SAFE
    # ---------------------------------------------------------
    def save(self, filename="rca_result.json") -> str:
        os.makedirs(self.result_dir, exist_ok=True)

        path: str = os.path.join(self.result_dir, filename)

        result = self.run()

        # Write result to file (avoid type annotation that formatter corrupts)
        fh: os.TextIOWrapper[_WrappedBuffer] = open(path, "w") # type: ignore
        json.dump(result, fh, indent=2)
        fh.close()

        return path

