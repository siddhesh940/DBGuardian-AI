"""
Unified Metrics Module - SINGLE SOURCE OF TRUTH for all metrics
=================================================================

This module provides THE ONLY authoritative source for:
- Total elapsed time
- Total executions  
- CPU usage %
- IO wait %

CRITICAL RULES:
1. ALL metrics come from the SAME CSV files generated from ONE HTML report
2. Metrics are computed ONCE and cached
3. CPU% is ALWAYS capped at 100.0
4. IO wait % is calculated from wait events CSV (never fake 0%)
5. The same values are used EVERYWHERE (UI, High Load Detection, DBA Analysis)

DO NOT compute these metrics anywhere else in the codebase.
"""

import os
import pandas as pd
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class AWRMetrics:
    """
    Unified AWR metrics container.
    These values are computed ONCE from CSV files and reused everywhere.
    """
    # From SQL Stats CSV
    total_elapsed_time_s: float = 0.0
    total_executions: int = 0
    total_cpu_time_s: float = 0.0
    
    # From Wait Events CSV  
    db_time_s: float = 0.0
    db_cpu_time_s: float = 0.0
    io_wait_time_s: float = 0.0
    
    # From Snapshot Metadata (HTML)
    snapshot_elapsed_s: float = 0.0
    cpu_cores: int = 8  # Default if not found
    
    # Instance CPU %Busy CPU - PRIMARY source for workload-level CPU Usage
    # This represents overall database CPU utilization, not per-SQL CPU
    instance_cpu_busy_pct: Optional[float] = None
    
    # Host CPU %Idle - Used for workload-level CPU calculation (100 - %Idle)
    host_cpu_idle_pct: Optional[float] = None
    
    # Computed metrics (derived from above)
    # CPU Usage = Workload-level CPU utilization from Host/Instance metrics
    cpu_percentage: float = 0.0  # ALWAYS <= 100.0
    io_wait_percentage: float = 0.0
    
    # Time window
    time_window_display: str = "--"
    
    # Metadata
    is_valid: bool = False
    source_csv_files: list = field(default_factory=list)


class UnifiedMetricsCalculator:
    """
    Calculate and store unified metrics from CSV files.
    
    USAGE:
        calculator = UnifiedMetricsCalculator(csv_dir, html_file_path)
        metrics = calculator.compute_metrics()
        
        # Use metrics.total_elapsed_time_s, metrics.cpu_percentage, etc.
    """
    
    def __init__(self, csv_dir: str, html_file_path: Optional[str] = None) -> None:
        """
        Initialize with paths to data sources.
        
        Args:
            csv_dir: Directory containing parsed CSV files
            html_file_path: Optional path to AWR HTML for metadata extraction
        """
        self.csv_dir: str = csv_dir
        self.html_file_path: str | None = html_file_path
        self._cached_metrics: Optional[AWRMetrics] = None
    
    def compute_metrics(self, force_refresh: bool = False) -> AWRMetrics:
        """
        Compute all metrics from CSV files.
        Results are cached; use force_refresh=True to recompute.
        
        Returns:
            AWRMetrics object with all computed values
        """
        if self._cached_metrics and not force_refresh:
            return self._cached_metrics
        
        metrics = AWRMetrics()
        
        try:
            # Find CSV files
            csv_files: Dict[str, str] = self._find_csv_files()
            if not csv_files:
                return metrics
            
            metrics.source_csv_files = list(csv_files.keys())
            
            # 1. Extract SQL Stats metrics
            if 'sql_stats' in csv_files:
                self._extract_sql_stats_metrics(csv_files['sql_stats'], metrics)
            
            # 2. Extract Wait Events metrics (for IO wait %)
            if 'wait_events' in csv_files:
                self._extract_wait_events_metrics(csv_files['wait_events'], metrics)
            
            # 3. Extract Instance Stats metrics
            if 'instance_stats' in csv_files:
                self._extract_instance_stats_metrics(csv_files['instance_stats'], metrics)
            
            # 4. Extract Snapshot Metadata from HTML (for accurate CPU calculation)
            if self.html_file_path and os.path.exists(self.html_file_path):
                self._extract_html_metadata(metrics)
            
            # 5. Compute derived metrics
            self._compute_derived_metrics(metrics)
            
            metrics.is_valid = True
            
        except Exception as e:
            print(f"âŒ Error computing unified metrics: {e}")
            metrics.is_valid = False
        
        self._cached_metrics = metrics
        return metrics
    
    def _find_csv_files(self) -> Dict[str, str]:
        """Find AWR CSV files in the directory."""
        csv_files = {}
        
        if not os.path.exists(self.csv_dir):
            return csv_files
        
        for filename in os.listdir(self.csv_dir):
            if not filename.endswith('.csv'):
                continue
            
            filepath: str = os.path.join(self.csv_dir, filename)
            filename_lower: str = filename.lower()
            
            if 'awr_sql_stats' in filename_lower:
                csv_files['sql_stats'] = filepath
            elif 'awr_wait_events' in filename_lower:
                csv_files['wait_events'] = filepath
            elif 'awr_instance_stats' in filename_lower:
                csv_files['instance_stats'] = filepath
        
        return csv_files
    
    def _extract_sql_stats_metrics(self, filepath: str, metrics: AWRMetrics) -> None:
        """Extract metrics from awr_sql_stats CSV."""
        try:
            df: pd.DataFrame = pd.read_csv(filepath)
            
            # Convert to numeric
            for col in ['elapsed__time_s', 'executions', 'cpu_time_s']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Total elapsed time (sum of all SQL elapsed times)
            if 'elapsed__time_s' in df.columns:
                metrics.total_elapsed_time_s = float(df['elapsed__time_s'].sum())
            
            # Total executions
            if 'executions' in df.columns:
                metrics.total_executions = int(df['executions'].sum())
            
            # Total CPU time from SQL
            if 'cpu_time_s' in df.columns:
                metrics.total_cpu_time_s = float(df['cpu_time_s'].sum())
            
            print(f"âœ… SQL Stats: elapsed={metrics.total_elapsed_time_s:.1f}s, "
                  f"executions={metrics.total_executions}, cpu={metrics.total_cpu_time_s:.1f}s")
                  
        except Exception as e:
            print(f"âš ï¸ Error reading SQL stats CSV: {e}")
    
    def _extract_wait_events_metrics(self, filepath: str, metrics: AWRMetrics) -> None:
        """Extract metrics from awr_wait_events CSV."""
        try:
            df: pd.DataFrame = pd.read_csv(filepath)
            
            # Convert time column to numeric
            if 'time_s' in df.columns:
                df['time_s'] = pd.to_numeric(df['time_s'], errors='coerce').fillna(0)
            
            db_cpu_time = 0.0
            db_time = 0.0
            io_wait_time = 0.0
            
            # Extract key metrics
            for _, row in df.iterrows():
                event_name: str = str(row.get('event', row.get('statistic_name', ''))).lower()
                time_val = float(row.get('time_s', 0))
                
                if 'db cpu' in event_name:
                    db_cpu_time: float = time_val
                elif 'db time' in event_name:
                    db_time: float = time_val
                elif any(io_event in event_name for io_event in [
                    'db file sequential read', 
                    'db file scattered read',
                    'direct path read',
                    'direct path write',
                    'log file sync',
                    'log file parallel write'
                ]):
                    io_wait_time += time_val
            
            metrics.db_cpu_time_s = db_cpu_time
            metrics.db_time_s = db_time
            metrics.io_wait_time_s = io_wait_time
            
            print(f"âœ… Wait Events: db_cpu={db_cpu_time:.1f}s, db_time={db_time:.1f}s, io_wait={io_wait_time:.1f}s")
            
        except Exception as e:
            print(f"âš ï¸ Error reading wait events CSV: {e}")
    
    def _extract_instance_stats_metrics(self, filepath: str, metrics: AWRMetrics) -> None:
        """Extract metrics from awr_instance_stats CSV."""
        try:
            df: pd.DataFrame = pd.read_csv(filepath)
            # Instance stats can provide additional context
            # Currently not used for primary metrics but available for extension
            
        except Exception as e:
            print(f"âš ï¸ Error reading instance stats CSV: {e}")
    
    def _extract_html_metadata(self, metrics: AWRMetrics) -> None:
        """Extract authoritative metadata from AWR HTML file."""
        try:
            from parsers.snapshot_metadata_parser import SnapshotMetadataParser
            
            parser = SnapshotMetadataParser(self.html_file_path)
            metadata: Dict[str, Any] = parser.parse()
            
            if metadata and metadata.get("parse_success"):
                # Get snapshot elapsed time
                if metadata.get("elapsed_seconds"):
                    metrics.snapshot_elapsed_s = float(metadata["elapsed_seconds"])
                
                # Get CPU cores
                if metadata.get("cpu_cores"):
                    metrics.cpu_cores = int(metadata["cpu_cores"])
                
                # Get DB CPU from HTML (more authoritative than CSV sometimes)
                if metadata.get("db_cpu_seconds"):
                    # Use HTML value if CSV value is 0 or HTML has more accurate data
                    html_db_cpu = float(metadata["db_cpu_seconds"])
                    if metrics.db_cpu_time_s == 0 or html_db_cpu > metrics.db_cpu_time_s:
                        metrics.db_cpu_time_s = html_db_cpu
                
                # ===== Extract Instance CPU %Busy CPU (for metadata/reference only) =====
                # Note: CPU Usage is now calculated from DB CPU Time / DB Time
                if metadata.get("instance_cpu_busy_pct") is not None:
                    metrics.instance_cpu_busy_pct = float(metadata["instance_cpu_busy_pct"])
                    print(f"âœ… Instance CPU %Busy CPU (metadata): {metrics.instance_cpu_busy_pct}%")
                
                # Extract Host CPU %Idle (for metadata/reference only)
                if metadata.get("host_cpu_idle_pct") is not None:
                    metrics.host_cpu_idle_pct = float(metadata["host_cpu_idle_pct"])
                    print(f"âœ… Host CPU %Idle (metadata): {metrics.host_cpu_idle_pct}%")
                
                # Format time window
                if metadata.get("begin_time") and metadata.get("end_time"):
                    begin = metadata["begin_time"]
                    end = metadata["end_time"]
                    begin_str = begin.strftime('%I:%M %p').lstrip('0')
                    end_str = end.strftime('%I:%M %p').lstrip('0')
                    metrics.time_window_display = f"{begin_str} - {end_str}"
                
                print(f"âœ… HTML Metadata: elapsed={metrics.snapshot_elapsed_s:.1f}s, "
                      f"cores={metrics.cpu_cores}, window={metrics.time_window_display}")
                      
        except Exception as e:
            print(f"âš ï¸ Error extracting HTML metadata: {e}")
    
    def _compute_derived_metrics(self, metrics: AWRMetrics) -> None:
        """
        Compute derived metrics (CPU %, IO wait %).
        
        WORKLOAD-LEVEL CPU CALCULATION:
        CPU Usage = Overall database CPU utilization during the analysis window
        
        This uses Instance CPU %Busy or Host CPU utilization from AWR metadata.
        This is different from SQL-level CPU (which can be 90%+ for a single SQL).
        
        For High Load Time Detection, we show workload-level CPU to answer:
        'Was the database CPU saturated during this window?'
        """
        # ===== WORKLOAD-LEVEL CPU PERCENTAGE CALCULATION =====
        # Use Instance CPU %Busy or Host CPU (100 - %Idle) from AWR metadata
        # This represents overall database CPU utilization, NOT per-SQL CPU
        
        # PRIMARY: Use Instance CPU %Busy (Oracle's CPU consumption)
        if metrics.instance_cpu_busy_pct is not None:
            metrics.cpu_percentage = min(100.0, max(0.0, round(metrics.instance_cpu_busy_pct, 1)))
            print(f"CPU Usage (Instance %Busy): {metrics.cpu_percentage}%")
        # SECONDARY: Use Host CPU utilization (100 - %Idle)
        elif metrics.host_cpu_idle_pct is not None:
            cpu_pct = 100.0 - metrics.host_cpu_idle_pct
            metrics.cpu_percentage = min(100.0, max(0.0, round(cpu_pct, 1)))
            print(f"CPU Usage (Host 100-Idle): 100 - {metrics.host_cpu_idle_pct}% = {metrics.cpu_percentage}%")
        # FALLBACK: Calculate from DB CPU / snapshot elapsed time / cores
        elif metrics.db_cpu_time_s > 0 and metrics.snapshot_elapsed_s > 0:
            cpu_pct = (metrics.db_cpu_time_s / (metrics.snapshot_elapsed_s * metrics.cpu_cores)) * 100
            metrics.cpu_percentage = min(100.0, max(0.0, round(cpu_pct, 1)))
            print(f"CPU Usage (DB CPU/cores): {metrics.db_cpu_time_s:.1f}s / ({metrics.snapshot_elapsed_s:.1f}s * {metrics.cpu_cores}) = {metrics.cpu_percentage}%")
        else:
            metrics.cpu_percentage = 0.0
            print(f"CPU Usage: No workload CPU data available, defaulting to 0%")
        
        # ===== IO WAIT PERCENTAGE CALCULATION =====
        # IO% = (IO_WAIT_TIME / DB_TIME) Ã— 100
        
        if metrics.db_time_s > 0:
            io_pct: float = (metrics.io_wait_time_s / metrics.db_time_s) * 100
            metrics.io_wait_percentage = min(100.0, round(io_pct, 1))
        elif metrics.total_elapsed_time_s > 0 and metrics.io_wait_time_s > 0:
            # Fallback: use total elapsed time
            io_pct: float = (metrics.io_wait_time_s / metrics.total_elapsed_time_s) * 100
            metrics.io_wait_percentage = min(100.0, round(io_pct, 1))
        else:
            metrics.io_wait_percentage = 0.0
        
        print(f"âœ… Derived: CPU%={metrics.cpu_percentage}%, IO%={metrics.io_wait_percentage}%")
    
    def get_high_load_details_string(self) -> str:
        """
        Generate the details string for High Load Time Detection.
        
        ALWAYS includes all 4 metrics:
        - Total elapsed time
        - Total executions
        - CPU Usage (= DB CPU Time / DB Time Ã— 100)
        - IO wait
        """
        metrics: AWRMetrics = self.compute_metrics()
        
        details = []
        
        # Always include Total elapsed time
        details.append(f"Total elapsed time: {metrics.total_elapsed_time_s:.1f}s")
        
        # Always include Total executions
        details.append(f"Total executions: {metrics.total_executions:,}")
        
        # CPU Usage = (DB CPU Time / DB Time) Ã— 100 - same definition as RCA
        details.append(f"CPU Usage: {metrics.cpu_percentage}%")
        
        # Always include IO wait %
        details.append(f"IO wait: {metrics.io_wait_percentage}%")
        
        return "; ".join(details)
    
    def get_metrics_dict(self) -> Dict[str, Any]:
        """
        Get all metrics as a dictionary for API responses.
        This ensures consistent data is returned everywhere.
        """
        metrics: AWRMetrics = self.compute_metrics()
        
        return {
            "total_elapsed_time_s": round(metrics.total_elapsed_time_s, 1),
            "total_executions": metrics.total_executions,
            "cpu_percentage": metrics.cpu_percentage,
            "io_wait_percentage": metrics.io_wait_percentage,
            "time_window": metrics.time_window_display,
            "db_time_s": round(metrics.db_time_s, 1),
            "db_cpu_time_s": round(metrics.db_cpu_time_s, 1),
            "cpu_cores": metrics.cpu_cores,
            "is_valid": metrics.is_valid
        }


# Global cache to store metrics per user/session
_metrics_cache: Dict[str, UnifiedMetricsCalculator] = {}


def get_unified_metrics(csv_dir: str, html_file_path: Optional[str] = None, 
                        force_refresh: bool = False) -> AWRMetrics:
    """
    Get unified metrics for a given CSV directory.
    Uses caching to avoid recomputation.
    
    Args:
        csv_dir: Directory containing parsed CSV files
        html_file_path: Optional path to AWR HTML file
        force_refresh: Force recomputation of metrics
    
    Returns:
        AWRMetrics object with all computed values
    """
    cache_key: str = csv_dir
    
    if cache_key not in _metrics_cache or force_refresh:
        _metrics_cache[cache_key] = UnifiedMetricsCalculator(csv_dir, html_file_path)
    
    return _metrics_cache[cache_key].compute_metrics(force_refresh)


def clear_metrics_cache(csv_dir: Optional[str] = None) -> None:
    """
    Clear cached metrics.
    
    Args:
        csv_dir: Clear specific cache entry, or all if None
    """
    global _metrics_cache
    
    if csv_dir:
        _metrics_cache.pop(csv_dir, None)
    else:
        _metrics_cache.clear()

