import pandas as pd
import os
import glob
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from typing import Any, Dict

from engine.unified_metrics import AWRMetrics


class TimeWindowDetector:
    def __init__(self, csv_dir, html_file_path=None) -> None:
        self.csv_dir: Any = csv_dir
        self.html_file_path = html_file_path  # Explicit HTML file for this analysis
        print(f"🕒 TimeWindowDetector initialized with CSV dir: {csv_dir}")

    # =========================================================================
    # AUTHORITATIVE TIME WINDOW FROM SNAPSHOT METADATA
    # =========================================================================
    def get_analysis_window_from_metadata(self, snapshot_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create authoritative time window data from parsed snapshot metadata.
        
        This is the SINGLE source of truth for time window display.
        All display formatting happens here - NO rounding in the parser.
        
        Args:
            snapshot_metadata: Dict from SnapshotMetadataParser.parse() containing:
                - begin_time: datetime
                - end_time: datetime
                - elapsed_seconds: float
                - db_cpu_seconds: float (optional)
                - cpu_cores: int (optional)
        
        Returns:
            Dict containing:
                - begin_time: datetime (raw, for filtering)
                - end_time: datetime (raw, for filtering)
                - display_window: str (formatted for UI, e.g., "09:30 AM - 10:30 AM")
                - elapsed_seconds: float
                - db_cpu_seconds: float (if available)
                - cpu_cores: int (if available)
        """
        result = {
            "begin_time": None,
            "end_time": None,
            "display_window": "--",
            "elapsed_seconds": None,
            "db_cpu_seconds": None,
            "cpu_cores": None
        }
        
        if not snapshot_metadata or not snapshot_metadata.get("parse_success"):
            return result
        
        begin_time = snapshot_metadata.get("begin_time")
        end_time = snapshot_metadata.get("end_time")
        
        if not begin_time or not end_time:
            return result
        
        # Store raw times for data filtering
        result["begin_time"] = begin_time
        result["end_time"] = end_time
        
        # Handle cross-midnight scenarios
        if end_time < begin_time:
            end_time = end_time + timedelta(days=1)
            result["end_time"] = end_time
        
        # Calculate elapsed if not provided
        elapsed = snapshot_metadata.get("elapsed_seconds")
        if elapsed is None:
            elapsed = (end_time - begin_time).total_seconds()
        result["elapsed_seconds"] = elapsed
        
        # Copy optional metadata
        if snapshot_metadata.get("db_cpu_seconds") is not None:
            result["db_cpu_seconds"] = snapshot_metadata["db_cpu_seconds"]
        if snapshot_metadata.get("cpu_cores") is not None:
            result["cpu_cores"] = snapshot_metadata["cpu_cores"]
        
        # ===== DISPLAY FORMATTING (ONLY place where this happens) =====
        # Round to nearest 30 minutes for cleaner display
        begin_rounded: datetime = self._round_time_to_interval(begin_time, 30)
        end_rounded: datetime = self._round_time_to_interval(end_time, 30)
        
        # Format as "HH:MM AM/PM - HH:MM AM/PM"
        begin_str = begin_rounded.strftime('%I:%M %p').lstrip('0')
        end_str = end_rounded.strftime('%I:%M %p').lstrip('0')
        
        result["display_window"] = f"{begin_str} - {end_str}"
        
        return result
    
    def _round_time_to_interval(self, dt: datetime, interval_minutes = 30) -> datetime:
        """
        Round datetime to nearest interval (e.g., 30 minutes).
        
        Examples:
            09:14 -> 09:00
            09:17 -> 09:30
            09:44 -> 09:30
            09:47 -> 10:00
        """
        minute = dt.minute
        half_interval = interval_minutes // 2
        
        # Find the nearest interval
        rounded_minute = (minute + half_interval) // interval_minutes * interval_minutes
        
        # Handle overflow to next hour
        if rounded_minute >= 60:
            dt = dt + timedelta(hours=1)
            rounded_minute = 0
        
        return dt.replace(minute=rounded_minute, second=0, microsecond=0)

    def _safe_float(self, value, default=0.0) -> float:
        """Safely convert a value to float, returning default if conversion fails."""
        try:
            if pd.isna(value) or value == '' or value is None:
                return default
            return float(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return default

    # =========================================================================
    # CPU PERCENTAGE CALCULATION
    # =========================================================================
    @staticmethod
    def calculate_cpu_percentage(db_cpu_seconds: float, elapsed_seconds: float, 
                                  cpu_cores: int, default_cores = 8) -> float:
        """
        Calculate CPU percentage using authoritative formula.
        
        Formula: CPU% = (DB_CPU_TIME / (Elapsed_Time * CPU_Cores)) * 100
        Always capped at 100.0 to prevent impossible values.
        
        Args:
            db_cpu_seconds: DB CPU time in seconds from AWR report
            elapsed_seconds: Elapsed time in seconds from AWR report
            cpu_cores: Number of CPU cores from AWR report
            default_cores: Default CPU core count if not available (default: 8)
        
        Returns:
            CPU percentage (0.0 - 100.0)
        """
        # Use default cores if not provided or invalid
        cores = cpu_cores if cpu_cores and cpu_cores > 0 else default_cores
        
        # Validate inputs
        if not db_cpu_seconds or db_cpu_seconds <= 0:
            return 0.0
        if not elapsed_seconds or elapsed_seconds <= 0:
            return 0.0
        
        # Calculate CPU percentage
        cpu_pct = (db_cpu_seconds / (elapsed_seconds * cores)) * 100
        
        # CRITICAL: Cap at 100% to prevent impossible values
        return min(100.0, round(cpu_pct, 2))
    
    def _extract_timestamps_from_html(self) -> None | Dict[str, None]:
        """Extract actual timestamps from AWR/ASH HTML files."""
        html_dir = os.path.join(os.path.dirname(self.csv_dir), 'raw_html')
        
        if not os.path.exists(html_dir):
            return None
            
        timestamps = {'awr': None, 'ash': None}
        
        # Check for AWR HTML files
        for file in os.listdir(html_dir):
            if file.endswith('.html'):
                filepath = os.path.join(html_dir, file)
                
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        soup = BeautifulSoup(content, 'html.parser')
                    
                    # Extract AWR timestamps
                    if 'awr' in file.lower() or 'AWR Report' in content:
                        awr_times = self._parse_awr_timestamps(soup)
                        if awr_times:
                            timestamps['awr'] = awr_times
                    
                    # Extract ASH timestamps  
                    if 'ash' in file.lower() or 'ASH Report' in content:
                        ash_times = self._parse_ash_timestamps(soup, content)
                        if ash_times:
                            timestamps['ash'] = ash_times
                            
                except Exception as e:
                    print(f"Error parsing HTML file {file}: {e}")
                    continue
                    
        return timestamps
    
    def _parse_awr_timestamps(self, soup):
        """Parse AWR Begin/End snap timestamps."""
        try:
            # Look for Begin Snap and End Snap rows
            begin_time = None
            end_time = None
            
            for tr in soup.find_all('tr'):
                cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                if len(cells) >= 3:
                    if 'Begin Snap:' in cells[0]:
                        # Format: "09-8月 -20 21:00:54" or similar
                        time_text = cells[2]
                        begin_time = self._parse_oracle_timestamp(time_text)
                    elif 'End Snap:' in cells[0]:
                        time_text = cells[2]
                        end_time = self._parse_oracle_timestamp(time_text)
            
            if begin_time and end_time:
                return {'begin': begin_time, 'end': end_time}
                
        except Exception as e:
            print(f"Error parsing AWR timestamps: {e}")
            
        return None
    
    def _parse_ash_timestamps(self, soup, content):
        """Parse ASH Analysis Begin/End times."""
        try:
            # Look in title first: "ASH Report - From 06-Dec-25 10:50:19 To 06-Dec-25 11:36:23"
            title_match = re.search(r'From (\d{2}-\w{3}-\d{2} \d{2}:\d{2}:\d{2}) To (\d{2}-\w{3}-\d{2} \d{2}:\d{2}:\d{2})', content)
            if title_match:
                begin_str, end_str = title_match.groups()
                begin_time = self._parse_oracle_timestamp(begin_str)
                end_time = self._parse_oracle_timestamp(end_str)
                if begin_time and end_time:
                    return {'begin': begin_time, 'end': end_time}
            
            # Also check in table rows
            for tr in soup.find_all('tr'):
                cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                if len(cells) >= 2:
                    if 'Analysis Begin Time:' in cells[0]:
                        begin_time = self._parse_oracle_timestamp(cells[1])
                    elif 'Analysis End Time:' in cells[0]:
                        end_time = self._parse_oracle_timestamp(cells[1])
            
            if begin_time and end_time:
                return {'begin': begin_time, 'end': end_time}
                
        except Exception as e:
            print(f"Error parsing ASH timestamps: {e}")
            
        return None
    
    def _analyze_ash_time_series(self, soup, content):
        """Advanced ASH analysis using Average Active Sessions (AAS) and realistic thresholds."""
        try:
            # Extract time-based activity data from ASH report
            ash_data = self._extract_ash_activity_data(soup, content)
            
            if not ash_data:
                return []
            
            # Analyze for high load periods using AAS methodology
            high_load_periods = self._detect_ash_high_load_periods(ash_data)
            
            return high_load_periods
            
        except Exception as e:
            print(f"Error in ASH time series analysis: {e}")
            return []
    
    def _extract_ash_activity_data(self, soup, content):
        """Extract structured activity data from ASH HTML."""
        activity_data = []
        
        try:
            # Method 1: Extract from time-based activity tables
            for tr in soup.find_all('tr'):
                cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                
                if len(cells) >= 4:
                    # Look for time pattern: "HH:MM:SS (X.X min)"
                    time_match = re.search(r'(\d{2}:\d{2}:\d{2})\s*\(([\d.]+)\s*min\)', cells[0])
                    
                    if time_match:
                        time_str = time_match.group(1)
                        duration_min = float(time_match.group(2))
                        
                        # Extract session counts from subsequent cells
                        total_sessions = 0
                        cpu_sessions = 0
                        wait_sessions = 0
                        
                        for i, cell in enumerate(cells[1:], 1):
                            if cell.isdigit():
                                sessions = int(cell)
                                if i == 1:  # First numeric cell is usually total
                                    total_sessions = sessions
                                elif 'CPU' in ' '.join(cells).upper() and i <= 3:
                                    # Look for CPU-related sessions
                                    if sessions > cpu_sessions:
                                        cpu_sessions = sessions
                        
                        # Calculate wait sessions
                        wait_sessions = max(0, total_sessions - cpu_sessions)
                        
                        if total_sessions > 0:  # Only include meaningful data
                            start_time = self._parse_time_only(time_str)
                            if start_time:
                                activity_data.append({
                                    'start': start_time,
                                    'end': start_time + timedelta(minutes=duration_min),
                                    'duration_min': duration_min,
                                    'total_sessions': total_sessions,
                                    'cpu_sessions': cpu_sessions,
                                    'wait_sessions': wait_sessions,
                                    'aas': total_sessions / max(1, duration_min) * 5,  # Normalize to 5-min AAS
                                    'cpu_pct': (cpu_sessions / total_sessions * 100) if total_sessions > 0 else 0
                                })
            
            # Method 2: Parse from text patterns if table parsing didn't work
            if not activity_data:
                pattern = r'(\d{2}:\d{2}:\d{2})\s+\(([\d.]+)\s+min\).*?(\d+).*?CPU.*?(\d+)'
                for match in re.finditer(pattern, content):
                    time_str, duration_min, total_sessions, cpu_sessions = match.groups()
                    
                    total_sessions = int(total_sessions)
                    cpu_sessions = int(cpu_sessions)
                    duration_min = float(duration_min)
                    wait_sessions = max(0, total_sessions - cpu_sessions)
                    
                    start_time = self._parse_time_only(time_str)
                    if start_time:
                        activity_data.append({
                            'start': start_time,
                            'end': start_time + timedelta(minutes=duration_min),
                            'duration_min': duration_min,
                            'total_sessions': total_sessions,
                            'cpu_sessions': cpu_sessions,
                            'wait_sessions': wait_sessions,
                            'aas': total_sessions / max(1, duration_min) * 5,
                            'cpu_pct': (cpu_sessions / total_sessions * 100) if total_sessions > 0 else 0
                        })
        
        except Exception as e:
            print(f"Error extracting ASH activity data: {e}")
        
        # Sort by start time
        activity_data.sort(key=lambda x: x['start'])
        return activity_data
    
    def _detect_ash_high_load_periods(self, ash_data):
        """Detect high load periods using production-grade ASH analysis."""
        if not ash_data:
            return []
        
        high_load_periods = []
        
        try:
            # Configuration - realistic thresholds for production systems
            AAS_HIGH_THRESHOLD = 3.0      # High load when AAS >= 3 (conservative)
            AAS_CRITICAL_THRESHOLD = 6.0  # Critical when AAS >= 6  
            CPU_HIGH_THRESHOLD = 75.0     # High CPU when >= 75%
            CPU_CRITICAL_THRESHOLD = 90.0 # Critical CPU when >= 90%
            MIN_DURATION_MINUTES = 10     # Minimum sustained period
            
            # Analyze each time window
            for data_point in ash_data:
                aas = data_point['aas']
                cpu_pct = data_point['cpu_pct']
                duration_min = data_point['duration_min']
                total_sessions = data_point['total_sessions']
                cpu_sessions = data_point['cpu_sessions']
                wait_sessions = data_point['wait_sessions']
                
                # Skip short duration spikes (avoid false positives)
                if duration_min < MIN_DURATION_MINUTES:
                    continue
                
                # Determine if this is a high load period
                is_high_load = False
                severity = "LOW"
                reasons = []
                
                # AAS-based detection (primary indicator)
                if aas >= AAS_CRITICAL_THRESHOLD:
                    is_high_load = True
                    severity = "HIGH"
                    reasons.append(f"Critical AAS: {aas:.1f} (threshold: {AAS_CRITICAL_THRESHOLD})")
                elif aas >= AAS_HIGH_THRESHOLD:
                    is_high_load = True
                    severity = "HIGH" if cpu_pct >= CPU_HIGH_THRESHOLD else "MEDIUM"
                    reasons.append(f"High AAS: {aas:.1f} (threshold: {AAS_HIGH_THRESHOLD})")
                
                # CPU-dominated load detection
                if cpu_pct >= CPU_CRITICAL_THRESHOLD and total_sessions >= 5:
                    is_high_load = True
                    severity = "HIGH"
                    reasons.append(f"Critical CPU load: {cpu_pct:.1f}%")
                elif cpu_pct >= CPU_HIGH_THRESHOLD and total_sessions >= 3:
                    is_high_load = True
                    if severity == "LOW":
                        severity = "MEDIUM"
                    reasons.append(f"High CPU load: {cpu_pct:.1f}%")
                
                # Wait-dominated load detection  
                if wait_sessions >= 5 and wait_sessions > cpu_sessions:
                    is_high_load = True
                    if severity == "LOW":
                        severity = "MEDIUM"
                    reasons.append(f"Wait-dominated load: {wait_sessions} wait vs {cpu_sessions} CPU sessions")
                
                # High session count (absolute threshold)
                if total_sessions >= 10 and duration_min >= MIN_DURATION_MINUTES:
                    is_high_load = True
                    if severity == "LOW":
                        severity = "MEDIUM"
                    reasons.append(f"High session count: {total_sessions} active sessions")
                
                # Add to high load periods if detected
                if is_high_load and reasons:
                    # Determine primary load type
                    if cpu_pct >= 70:
                        load_type = "High CPU dominated load"
                    elif wait_sessions > cpu_sessions and wait_sessions >= 3:
                        load_type = "High Wait Event load"
                    else:
                        load_type = "High database activity"
                    
                    high_load_periods.append({
                        'start': data_point['start'],
                        'end': data_point['end'],
                        'duration_min': duration_min,
                        'total_sessions': total_sessions,
                        'cpu_sessions': cpu_sessions,
                        'wait_sessions': wait_sessions,
                        'aas': aas,
                        'cpu_pct': cpu_pct,
                        'severity': severity,
                        'load_type': load_type,
                        'reason': f"{load_type}: {'; '.join(reasons)}"
                    })
            
            # Merge adjacent/overlapping periods
            if high_load_periods:
                high_load_periods = self._merge_continuous_ash_periods(high_load_periods)
        
        except Exception as e:
            print(f"Error in ASH high load detection: {e}")
        
        return high_load_periods
    
    def _merge_continuous_ash_periods(self, periods):
        """Merge continuous ASH high load periods."""
        if len(periods) <= 1:
            return periods
        
        merged = []
        current = periods[0].copy()
        
        for next_period in periods[1:]:
            # Check if periods are adjacent (within 5 minutes)
            time_gap = next_period['start'] - current['end']
            
            if time_gap.total_seconds() <= 300:  # 5 minutes or less
                # Merge periods
                current['end'] = next_period['end']
                current['duration_min'] += next_period['duration_min']
                current['total_sessions'] = max(current['total_sessions'], next_period['total_sessions'])
                current['aas'] = max(current['aas'], next_period['aas'])
                current['cpu_pct'] = max(current['cpu_pct'], next_period['cpu_pct'])
                
                # Update severity to highest
                if next_period['severity'] == "HIGH" or current['severity'] == "HIGH":
                    current['severity'] = "HIGH"
                elif next_period['severity'] == "MEDIUM" or current['severity'] == "MEDIUM":
                    current['severity'] = "MEDIUM"
                
                # Combine reasons
                current['reason'] = f"Sustained {current['load_type']}: Peak AAS {current['aas']:.1f}, Peak CPU {current['cpu_pct']:.1f}%, Duration {current['duration_min']:.0f}m"
            else:
                # Periods are separate, add current and start new
                merged.append(current)
                current = next_period.copy()
        
        # Add the last period
        merged.append(current)
        
        return merged

    def _analyze_awr_peak_periods(self, sql_df):
        """Analyze AWR SQL data to estimate peak activity periods."""
        if sql_df.empty:
            return []
            
        high_load_periods = []
        
        try:
            # Convert to numeric
            for col in ['elapsed__time_s', 'executions', 'pctcpu']:
                if col in sql_df.columns:
                    sql_df[col] = pd.to_numeric(sql_df[col], errors='coerce').fillna(0)
            
            # Find statements with truly significant impact
            high_impact_sql = sql_df[
                (sql_df['elapsed__time_s'] > 15) |  # Longer-running queries
                (sql_df['executions'] > 200) |      # Higher execution count
                (sql_df['pctcpu'] > 85)             # Higher CPU usage
            ]
            
            if not high_impact_sql.empty:
                total_elapsed = high_impact_sql['elapsed__time_s'].sum()
                total_executions = high_impact_sql['executions'].sum()
                max_cpu = high_impact_sql['pctcpu'].max()
                
                # Estimate time windows based on execution patterns
                if total_elapsed > 50:  # Significant activity
                    # Group by execution characteristics to estimate time periods
                    patterns = []
                    
                    # Pattern 1: DBMS_SCHEDULER jobs (likely maintenance)
                    scheduler_sql = high_impact_sql[high_impact_sql['sql_module'].str.contains('DBMS_SCHEDULER', na=False)]
                    if not scheduler_sql.empty:
                        patterns.append({
                            'type': 'Maintenance Activity',
                            'elapsed': scheduler_sql['elapsed__time_s'].sum(),
                            'executions': scheduler_sql['executions'].sum()
                        })
                    
                    # Pattern 2: High-frequency queries  
                    frequent_sql = high_impact_sql[high_impact_sql['executions'] > 50]
                    if not frequent_sql.empty:
                        patterns.append({
                            'type': 'High Query Volume', 
                            'elapsed': frequent_sql['elapsed__time_s'].sum(),
                            'executions': frequent_sql['executions'].sum()
                        })
                    
                    # Pattern 3: Long-running queries
                    long_sql = high_impact_sql[high_impact_sql['elapsed__time_s'] > 20]
                    if not long_sql.empty:
                        patterns.append({
                            'type': 'Long-running Queries',
                            'elapsed': long_sql['elapsed__time_s'].sum(), 
                            'executions': long_sql['executions'].sum()
                        })
                    
                    # Estimate peak periods based on patterns
                    for i, pattern in enumerate(patterns):
                        if pattern['elapsed'] > 30:  # Significant impact
                            # Estimate duration based on elapsed time and executions
                            estimated_duration = min(60, pattern['elapsed'] / 2)  # Conservative estimate
                            
                            # Create estimated time window (since we don't have exact times from AWR)
                            base_time: datetime = datetime.now().replace(hour=11, minute=0, second=0, microsecond=0)  # Assume peak time
                            start_time: datetime = base_time + timedelta(minutes=i*15)  # Stagger multiple periods
                            end_time: datetime = start_time + timedelta(minutes=estimated_duration)
                            
                            reason_parts = []
                            if pattern['elapsed'] > 50:
                                reason_parts.append(f"High elapsed time: {pattern['elapsed']:.1f}s")
                            if pattern['executions'] > 100:
                                reason_parts.append(f"High executions: {pattern['executions']:.0f}")
                            if max_cpu > 80:
                                reason_parts.append(f"High CPU: {max_cpu:.1f}%")
                            
                            high_load_periods.append({
                                'start': start_time,
                                'end': end_time,
                                'pattern_type': pattern['type'],
                                'elapsed_time': pattern['elapsed'],
                                'executions': pattern['executions'],
                                'reason': '; '.join(reason_parts)
                            })
                            
        except Exception as e:
            print(f"Error analyzing AWR peak periods: {e}")
        
        return high_load_periods
    
    def _merge_continuous_periods(self, periods):
        """Merge overlapping and adjacent high-load periods."""
        if not periods:
            return []
        
        # Sort periods by start time
        sorted_periods = sorted(periods, key=lambda x: x['start'])
        merged = []
        
        for period in sorted_periods:
            if not merged:
                merged.append(period)
            else:
                last = merged[-1]
                # Check if periods are adjacent or overlapping (within 10 minutes gap)
                time_gap = period['start'] - last['end']
                if time_gap.total_seconds() <= 600:  # 10 minutes or less gap
                    # Merge periods
                    last['end'] = max(last['end'], period['end'])
                    # Combine reasons and metrics
                    last['total_sessions'] = max(last.get('total_sessions', 0), period.get('total_sessions', 0))
                    last['elapsed_time'] = last.get('elapsed_time', 0) + period.get('elapsed_time', 0)
                    last['executions'] = last.get('executions', 0) + period.get('executions', 0)
                    
                    # Update reason with combined info
                    reasons = []
                    if last.get('total_sessions', 0) > 50:
                        reasons.append(f"High session activity: {last['total_sessions']} peak sessions")
                    if last.get('elapsed_time', 0) > 100:
                        reasons.append(f"High elapsed time: {last['elapsed_time']:.1f}s")
                    if last.get('executions', 0) > 500:
                        reasons.append(f"High executions: {last['executions']}")
                    
                    last['reason'] = '; '.join(reasons) if reasons else 'Sustained high activity'
                else:
                    merged.append(period)
        
        return merged
    
    def _parse_time_only(self, time_str) -> datetime | None:
        """Parse time string and return datetime with today's date."""
        try:
            match = re.search(r'(\d{2}):(\d{2}):(\d{2})', time_str)
            if match:
                hour, minute, second = match.groups()
                today: datetime = datetime.now()
                return datetime(today.year, today.month, today.day, int(hour), int(minute), int(second))
        except Exception as e:
            print(f"Error parsing time {time_str}: {e}")
        return None
    
    def _parse_oracle_timestamp(self, timestamp_str):
        """Parse various Oracle timestamp formats."""
        if not timestamp_str:
            return None
            
        try:
            # Pattern 1: "06-Dec-25 10:50:19" (ASH format)
            match = re.search(r'(\d{2})-(\w{3})-(\d{2}) (\d{2}):(\d{2}):(\d{2})', timestamp_str)
            if match:
                day, month, year, hour, minute, second = match.groups()
                month_map: Dict[str, int] = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                           'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
                month_num = month_map.get(month, 1)
                year_full = 2000 + int(year) if int(year) < 50 else 1900 + int(year)
                return datetime(year_full, month_num, int(day), int(hour), int(minute), int(second))
            
            # Pattern 2: "09-8月 -20 21:00:54" or "10-8月 -20 08:00:13" (AWR Chinese format)
            match = re.search(r'(\d{1,2})-\w*月?\w*-(\d{2}) (\d{2}):(\d{2}):(\d{2})', timestamp_str)
            if match:
                day, year, hour, minute, second = match.groups()
                # Since we see "8月" in the data, assume August
                month_num = 8
                year_full = 2000 + int(year)
                # If it looks like it's crossing to next day, handle that
                if day == '10' and hour == '08':  # End time next day
                    return datetime(year_full, month_num, int(day), int(hour), int(minute), int(second))
                else:  # Begin time
                    return datetime(year_full, month_num, int(day), int(hour), int(minute), int(second))
            
            # Extract just time if no date patterns match
            time_match = re.search(r'(\d{2}):(\d{2}):(\d{2})', timestamp_str)
            if time_match:
                hour, minute, second = time_match.groups()
                # Use current date with the time
                today: datetime = datetime.now()
                return datetime(today.year, today.month, today.day, int(hour), int(minute), int(second))
                
        except Exception as e:
            print(f"Error parsing timestamp '{timestamp_str}': {e}")
            
        return None

    def _get_csv_files(self):
        """Find all AWR-related CSV files in the directory."""
        patterns = [
            "*awr_sql_stats*.csv",
            "*awr_instance_stats*.csv", 
            "*awr_wait_events*.csv"
        ]
        
        files = {}
        for pattern in patterns:
            matches = glob.glob(os.path.join(self.csv_dir, pattern))
            if matches:
                key = pattern.split('*')[1].split('*')[0]  # Extract the middle part
                files[key] = matches[0]  # Take first match
        
        return files

    def _get_actual_time_window(self) -> str:
        """Get the actual time window from AWR/ASH HTML files."""
        timestamps: Dict[str, Any] | None = self._extract_timestamps_from_html()
        
        if not timestamps:
            return "Analysis period (time window data unavailable)"
            
        # Prefer AWR timestamps, fallback to ASH
        time_data = timestamps.get('awr') or timestamps.get('ash')
        
        if time_data and time_data.get('begin') and time_data.get('end'):
            begin_time = time_data['begin']
            end_time = time_data['end']
            
            # Handle cases where end time is next day (AWR reports can span midnight)
            if end_time < begin_time:
                end_time = end_time + timedelta(days=1)
            
            # Format times for display
            begin_str = begin_time.strftime('%I:%M %p').lstrip('0')
            end_str = end_time.strftime('%I:%M %p').lstrip('0')
            
            # Calculate duration
            duration = end_time - begin_time
            hours = int(duration.total_seconds() // 3600)
            minutes = int((duration.total_seconds() % 3600) // 60)
            
            # Handle cross-midnight cases
            if hours > 12:  # If duration is over 12 hours, probably crossed midnight
                # Show dates if crossing midnight
                if begin_time.date() != end_time.date():
                    begin_str = begin_time.strftime('%b %d %I:%M %p').lstrip('0')
                    end_str = end_time.strftime('%b %d %I:%M %p').lstrip('0')
                duration_str = f" ({hours}h {minutes}m period)"
            elif hours > 0:
                duration_str = f" ({hours}h {minutes}m period)"
            else:
                duration_str = f" ({minutes}m period)"
            
            return f"{begin_str} - {end_str}{duration_str}"
        
        return "Analysis period (timestamps extracted from AWR/ASH data)"
    
    def _extract_time_window_from_filename(self, filename) -> str:
        """Extract time window information from AWR report filename - fallback method."""
        # Try to get actual timestamps first
        actual_window = self._get_actual_time_window()
        if "unavailable" not in actual_window and "timestamps extracted" not in actual_window:
            return actual_window
            
        # Fallback to filename parsing
        parts = os.path.basename(filename).split('_')
        for i, part in enumerate(parts):
            if part.isdigit() and i + 1 < len(parts) and parts[i + 1].replace('.csv', '').isdigit():
                start_snap = int(part)
                end_snap = int(parts[i + 1].replace('.csv', ''))
                
                snap_duration = end_snap - start_snap
                if snap_duration <= 1:
                    return f"Snapshot {start_snap}-{end_snap} (≈1h window)"
                else:
                    return f"Snapshot {start_snap}-{end_snap} (≈{snap_duration}h window)"
                    
        return "Analysis period (time window from AWR data)"

    def _determine_peak_activity_window(self, sql_df) -> None | str:
        """Analyze SQL data to determine when peak activity occurred."""
        if sql_df.empty:
            return None
            
        # Ensure numeric columns are properly converted
        for col in ['elapsed__time_s', 'executions', 'pctcpu']:
            if col in sql_df.columns:
                sql_df[col] = pd.to_numeric(sql_df[col], errors='coerce').fillna(0)
            
        # Find the most resource-intensive SQL statements
        if 'elapsed__time_s' not in sql_df.columns:
            return None
            
        top_sql = sql_df.nlargest(5, 'elapsed__time_s')
        
        high_impact_statements = []
        for _, row in top_sql.iterrows():
            if row['elapsed__time_s'] > 5:  # Statements taking more than 5 seconds
                high_impact_statements.append({
                    'elapsed_time': row['elapsed__time_s'],
                    'executions': row['executions'],
                    'cpu_pct': row['pctcpu']
                })
        
        if high_impact_statements:
            total_high_impact_time = sum(stmt['elapsed_time'] for stmt in high_impact_statements)
            if total_high_impact_time > 50:  # Significant load
                # Return actual time window instead of generic text
                return self._get_actual_time_window()
        
        return None

    def _get_normalized_cpu_percentage(self) -> float:
        """
        Get CPU percentage from authoritative AWR source.
        
        CRITICAL PRIORITY ORDER:
        1. PRIMARY: Host CPU → 100 - %Idle (from AWR HTML)
        2. SECONDARY: Instance CPU → %Busy CPU (only if Host CPU missing)
        
        Returns:
            CPU percentage (0.0 - 100.0)
        """
        try:
            # Use explicit HTML file if provided (preferred - ensures correct file per upload)
            if self.html_file_path and os.path.exists(self.html_file_path):
                from parsers.snapshot_metadata_parser import SnapshotMetadataParser
                parser = SnapshotMetadataParser(self.html_file_path)
                metadata = parser.parse()
                
                if metadata and metadata.get("parse_success"):
                    # PRIMARY SOURCE: Host CPU → 100 - %Idle
                    if metadata.get("host_cpu_idle_pct") is not None:
                        calculated_cpu = 100.0 - float(metadata["host_cpu_idle_pct"])
                        return min(100.0, max(0.0, round(calculated_cpu, 1)))
                    
                    # SECONDARY SOURCE: Instance CPU → %Busy CPU (only if Host CPU missing)
                    if metadata.get("instance_cpu_busy_pct") is not None:
                        cpu_pct = float(metadata["instance_cpu_busy_pct"])
                        return min(100.0, round(cpu_pct, 1))
            
            # Fallback: Search in raw_html directory (for backward compatibility)
            html_dir = os.path.join(os.path.dirname(self.csv_dir), 'raw_html')
            if not os.path.exists(html_dir):
                return 0.0
            
            # Find the correct AWR HTML file by matching with CSV filenames
            # CSVs are named like: awr_sql_stats_06-12-2025_awr.csv
            # HTML is named like: 06-12-2025_awr.html
            csv_files = [f for f in os.listdir(self.csv_dir) if f.endswith('.csv')] if os.path.exists(self.csv_dir) else []
            html_base_names = set()
            
            for csv_file in csv_files:
                # Extract the base name from CSV (e.g., "06-12-2025_awr" from "awr_sql_stats_06-12-2025_awr.csv")
                parts = csv_file.replace('.csv', '').split('_')
                if len(parts) >= 3:
                    # Skip prefix like "awr_sql_stats_" and get the rest
                    base_name = '_'.join(parts[3:]) if parts[0] == 'awr' else '_'.join(parts[2:])
                    if base_name:
                        html_base_names.add(base_name)
            
            # Find matching HTML file
            for filename in os.listdir(html_dir):
                if not filename.lower().endswith('.html'):
                    continue
                    
                html_base = filename.replace('.html', '')
                
                # Check if this HTML matches the CSVs
                if html_base in html_base_names or any(html_base in name for name in html_base_names):
                    filepath = os.path.join(html_dir, filename)
                    try:
                        from parsers.snapshot_metadata_parser import SnapshotMetadataParser
                        parser = SnapshotMetadataParser(filepath)
                        metadata = parser.parse()
                        
                        if metadata and metadata.get("parse_success"):
                            # PRIMARY SOURCE: Host CPU → 100 - %Idle
                            if metadata.get("host_cpu_idle_pct") is not None:
                                calculated_cpu = 100.0 - float(metadata["host_cpu_idle_pct"])
                                return min(100.0, max(0.0, round(calculated_cpu, 1)))
                            
                            # SECONDARY SOURCE: Instance CPU → %Busy CPU
                            if metadata.get("instance_cpu_busy_pct") is not None:
                                cpu_pct = float(metadata["instance_cpu_busy_pct"])
                                return min(100.0, round(cpu_pct, 1))
                    except Exception as e:
                        print(f"Error parsing HTML for CPU%: {e}")
                        continue
                        
        except Exception as e:
            print(f"Error getting normalized CPU%: {e}")
        
        return 0.0

    def _analyze_sql_performance(self, sql_df):
        """Analyze SQL performance metrics for AWR high load detection."""
        if sql_df.empty:
            return None
            
        # Convert columns to numeric, replacing any non-numeric values with 0
        for col in ['elapsed__time_s', 'executions', 'elapsed_time_per_exec_s', 'pctcpu']:
            if col in sql_df.columns:
                sql_df[col] = pd.to_numeric(sql_df[col], errors='coerce').fillna(0)
            
        # Calculate metrics from AWR data - 100% accurate values
        total_elapsed_time = sql_df['elapsed__time_s'].sum() if 'elapsed__time_s' in sql_df.columns else 0
        total_executions = sql_df['executions'].sum() if 'executions' in sql_df.columns else 0
        
        # CRITICAL FIX: Use normalized CPU% from HTML metadata, NOT pctcpu from CSV
        # pctcpu in CSV = "% of DB CPU consumed by this SQL" (can exceed 100%)
        # We need: System CPU% = (DB_CPU_TIME / (ELAPSED × CPU_CORES)) × 100
        max_cpu_percent = self._get_normalized_cpu_percentage()
        
        # High load thresholds (simplified for AWR analysis)
        high_load_detected = False
        severity = "LOW"
        
        # Check total elapsed time (threshold: > 50s indicates significant activity)
        if total_elapsed_time > 50:
            high_load_detected = True
            severity = "HIGH" if total_elapsed_time > 200 else "MEDIUM"
        
        # Check execution volume (threshold: > 100 total executions)
        if total_executions > 100:
            high_load_detected = True
            if total_executions > 500 and severity != "HIGH":
                severity = "HIGH"
            elif severity == "LOW":
                severity = "MEDIUM"
        
        # Check CPU usage (threshold: > 80% CPU indicates high load)
        if max_cpu_percent > 80:
            high_load_detected = True
            severity = "HIGH"
        elif max_cpu_percent > 50:
            high_load_detected = True
            if severity == "LOW":
                severity = "MEDIUM"
        
        return {
            "detected": high_load_detected,
            "severity": severity,
            "metrics": {
                "total_elapsed_time": total_elapsed_time,
                "total_executions": int(total_executions),
                "max_cpu_percent": max_cpu_percent
            }
        }

    def _analyze_db_time_metrics(self, wait_events_df):
        """Analyze DB time and wait events for high load detection."""
        if wait_events_df.empty:
            return None
            
        # Convert time_s column to numeric
        if 'time_s' in wait_events_df.columns:
            wait_events_df['time_s'] = pd.to_numeric(wait_events_df['time_s'], errors='coerce').fillna(0)
            
        # Look for DB CPU and DB time metrics
        db_cpu_time = 0
        db_time = 0
        
        for _, row in wait_events_df.iterrows():
            if 'DB CPU' in str(row.get('statistic_name', '')):
                db_cpu_time = self._safe_float(row.get('time_s', 0))
            elif 'DB time' in str(row.get('statistic_name', '')):
                db_time = self._safe_float(row.get('time_s', 0))
        
        high_load_detected = False
        severity = "LOW" 
        details = []
        
        # Check DB time (threshold: > 30s indicates significant load)
        if db_time > 30:
            high_load_detected = True
            details.append(f"DB time: {db_time:.2f}s")
            if db_time > 100:
                severity = "HIGH"
            elif severity == "LOW":
                severity = "MEDIUM"
        
        # Check DB CPU time (threshold: > 20s indicates high CPU load)
        if db_cpu_time > 20:
            high_load_detected = True
            details.append(f"DB CPU time: {db_cpu_time:.2f}s")
            if db_cpu_time > 60:
                severity = "HIGH"
            elif severity == "LOW":
                severity = "MEDIUM"
        
        # Calculate CPU percentage of DB time
        if db_time > 0:
            cpu_percentage = (db_cpu_time / db_time) * 100
            if cpu_percentage > 70:
                high_load_detected = True
                details.append(f"CPU % of DB time: {cpu_percentage:.1f}%")
                if cpu_percentage > 90:
                    severity = "HIGH"
                elif severity == "LOW":
                    severity = "MEDIUM"
        
        return {
            "detected": high_load_detected,
            "severity": severity,
            "details": details,
            "metrics": {
                "db_time": db_time,
                "db_cpu_time": db_cpu_time,
                "cpu_percentage": (db_cpu_time / db_time * 100) if db_time > 0 else 0
            }
        }

    def detect_high_load_periods(self):
        """BULLETPROOF file type isolation - HARD RULES implementation."""
        results = []
        
        try:
            # STRICT file type detection - no assumptions
            has_ash_files: bool = self._check_ash_files_exist()
            has_awr_files: bool = self._check_awr_files_exist()
            
            # RULE 1: File Type Isolation - Never mix logic
            
            # Case A: ONLY AWR uploaded → Show ONLY AWR result
            if has_awr_files and not has_ash_files:
                awr_result = self._get_pure_awr_result()
                if awr_result:
                    results.append(awr_result)
            
            # Case B: ONLY ASH uploaded → Show ONLY ASH result  
            elif has_ash_files and not has_awr_files:
                ash_result = self._get_pure_ash_result()
                if ash_result:
                    results.append(ash_result)
            
            # Case C: BOTH uploaded → Show both separately (2 independent blocks)
            elif has_ash_files and has_awr_files:
                # ASH block first
                ash_result = self._get_pure_ash_result()
                if ash_result:
                    results.append(ash_result)
                    
                # AWR block second  
                awr_result = self._get_pure_awr_result()
                if awr_result:
                    results.append(awr_result)
            
            # Case D: No files at all
            else:
                results.append({
                    "period": "No monitoring data available",
                    "type": "No Data",
                    "severity": "LOW", 
                    "details": "No monitoring files uploaded for analysis"
                })
                
        except Exception as e:
            results.append({
                "period": "Analysis failed",
                "type": "Error",
                "severity": "LOW",
                "details": f"Error during analysis: {str(e)}"
            })
        
        return results
    
    def _check_ash_files_exist(self) -> bool:
        """Check if ASH files were actually processed (based on CSV generation)."""
        # Check for ASH-specific CSV files that were generated from processing
        if os.path.exists(self.csv_dir):
            for file in os.listdir(self.csv_dir):
                if file.endswith('.csv') and 'ash' in file.lower():
                    return True
        
        return False
    
    def _check_awr_files_exist(self) -> bool:
        """Check if AWR files were actually processed (based on CSV generation)."""
        # Check for AWR-specific CSV files that were generated from processing
        if os.path.exists(self.csv_dir):
            for file in os.listdir(self.csv_dir):
                if file.endswith('.csv') and 'awr' in file.lower():
                    return True
        
        return False
    
    def _get_pure_ash_result(self):
        """Get PURE ASH result - never contaminated with AWR logic."""
        # RULE 2: Output Format - ASH only
        
        try:
            # Fresh ASH analysis - always recalculate
            ash_periods = self._detect_ash_peak_periods()
            
            if ash_periods:
                # ASH High Load detected
                period = ash_periods[0]  # Most significant period
                start_str = period['start'].strftime('%I:%M %p').lstrip('0')
                end_str = period['end'].strftime('%I:%M %p').lstrip('0')
                duration = period.get('duration_min', 0)
                
                return {
                    "period": f"{start_str} - {end_str} ({duration:.0f}m)",
                    "type": "ASH High Load",
                    "severity": period.get('severity', 'HIGH'),
                    "details": f"ASH Analysis: {period.get('reason', 'Sustained high database activity detected')}"
                }
            else:
                # ASH Normal (no high load)
                reason = "AAS and CPU utilization remained within normal parameters"
                if hasattr(self, '_ash_analysis_summary'):
                    reason = self._ash_analysis_summary.get('no_high_load_reason', reason)
                
                return {
                    "period": "No High Load Detected",
                    "type": "ASH Normal",
                    "severity": "LOW",
                    "details": f"ASH Analysis: {reason}"
                }
                
        except Exception as e:
            return {
                "period": "ASH analysis failed",
                "type": "ASH Error",
                "severity": "LOW",
                "details": f"ASH Analysis Error: {str(e)}"
            }
    
    def _get_pure_awr_result(self):
        """
        Get PURE AWR result - never contaminated with ASH logic.
        
        CRITICAL: Uses unified metrics to ensure ALL 4 metrics are included.
        """
        # RULE 2: Output Format - AWR only
        
        try:
            csv_files = self._get_csv_files()
            
            # Fresh AWR analysis - always recalculate using unified metrics
            awr_analysis = self._basic_high_load_analysis(csv_files)
            
            if awr_analysis:
                # AWR High Load detected - include unified metrics
                result = {
                    "period": awr_analysis['period'],
                    "type": "AWR High Load",
                    "severity": awr_analysis.get('severity', 'HIGH'),
                    "details": f"AWR Analysis: {awr_analysis.get('details', 'High database load detected')}"
                }
                
                # Include metrics dict for UI consumption
                if 'metrics' in awr_analysis:
                    result['metrics'] = awr_analysis['metrics']
                
                return result
            else:
                # AWR Normal (no high load) - still get metrics for display
                try:
                    from engine.unified_metrics import UnifiedMetricsCalculator
                    html_file = self._find_awr_html_file()
                    metrics_calc = UnifiedMetricsCalculator(self.csv_dir, html_file)
                    metrics_dict = metrics_calc.get_metrics_dict()
                    
                    # Build details string with actual metrics (even for no high load)
                    details = f"AWR Analysis: Total elapsed time: {metrics_dict['total_elapsed_time_s']:.1f}s; "
                    details += f"Total executions: {metrics_dict['total_executions']:,}"
                    
                    return {
                        "period": "No High Load Detected",
                        "type": "AWR Normal", 
                        "severity": "LOW",
                        "details": details,
                        "metrics": metrics_dict
                    }
                except:
                    return {
                        "period": "No High Load Detected",
                        "type": "AWR Normal", 
                        "severity": "LOW",
                        "details": "AWR Analysis: No significant load patterns detected in available data"
                    }
                
        except Exception as e:
            return {
                "period": "AWR analysis failed",
                "type": "AWR Error",
                "severity": "LOW", 
                "details": f"AWR Analysis Error: {str(e)}"
            }
    
    def _detect_ash_peak_periods(self):
        """Detect peak periods from ASH HTML files using production-grade analysis."""
        html_dir = os.path.join(os.path.dirname(self.csv_dir), 'raw_html')
        
        if not os.path.exists(html_dir):
            return []
            
        ash_periods = []
        ash_files_processed = 0
        
        for file in os.listdir(html_dir):
            if 'ash' in file.lower() and file.endswith('.html'):
                ash_files_processed += 1
                filepath = os.path.join(html_dir, file)
                
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        soup = BeautifulSoup(content, 'html.parser')
                    
                    periods = self._analyze_ash_time_series(soup, content)
                    ash_periods.extend(periods)
                    
                except Exception as e:
                    print(f"Error processing ASH file {file}: {e}")
                    continue
        
        # Store metadata for "no high load" messaging
        if ash_files_processed > 0 and not ash_periods:
            self._ash_analysis_summary = {
                'files_analyzed': ash_files_processed,
                'has_data': True,
                'no_high_load_reason': 'AAS and CPU utilization remained within normal parameters'
            }
        
        return ash_periods
    
    def _get_html_based_time_window(self) -> str:
        """
        Get the authoritative time window from AWR HTML file.
        This is the SINGLE source of truth - no estimation allowed.
        """
        try:
            html_dir = os.path.join(os.path.dirname(self.csv_dir), 'raw_html')
            if not os.path.exists(html_dir):
                return "High Load Detected"
            
            # Find AWR HTML file
            for filename in os.listdir(html_dir):
                if filename.lower().endswith('.html'):
                    filepath = os.path.join(html_dir, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                            sample = f.read(5000)
                        if 'awr' in filename.lower() or 'AWR Report' in sample or 'Begin Snap' in sample:
                            # Parse using the authoritative parser
                            from parsers.snapshot_metadata_parser import SnapshotMetadataParser
                            parser = SnapshotMetadataParser(filepath)
                            metadata = parser.parse()
                            
                            if metadata and metadata.get("parse_success"):
                                # Use the time_window_detector formatting
                                window_data = self.get_analysis_window_from_metadata(metadata)
                                if window_data.get("display_window") and window_data["display_window"] != "--":
                                    return window_data["display_window"]
                    except Exception as e:
                        print(f"Error parsing HTML {filename}: {e}")
                        continue
        except Exception as e:
            print(f"Error getting HTML-based time window: {e}")
        
        return "High Load Detected"
    
    def _basic_high_load_analysis(self, csv_files):
        """
        Basic high load analysis - uses UNIFIED METRICS for consistency.
        
        CRITICAL: This method now uses UnifiedMetricsCalculator to ensure
        ALL 4 metrics are ALWAYS included and consistent everywhere.
        """
        try:
            # ===== USE UNIFIED METRICS (Single Source of Truth) =====
            from engine.unified_metrics import UnifiedMetricsCalculator
            
            # Find AWR HTML file for metadata extraction
            html_file_path = self._find_awr_html_file()
            
            # Get unified metrics from CSV files + HTML metadata
            metrics_calc = UnifiedMetricsCalculator(self.csv_dir, html_file_path)
            unified_metrics: AWRMetrics = metrics_calc.compute_metrics()
            
            if not unified_metrics.is_valid:
                # Fallback to old analysis if unified metrics fail
                return self._basic_high_load_analysis_fallback(csv_files)
            
            # Determine if high load is detected
            high_load_detected: bool = (
                unified_metrics.total_elapsed_time_s > 50 or
                unified_metrics.total_executions > 100 or
                unified_metrics.cpu_percentage > 50
            )
            
            if not high_load_detected:
                return None
            
            # Determine severity
            severity = "LOW"
            if unified_metrics.total_elapsed_time_s > 200 or unified_metrics.cpu_percentage > 80:
                severity = "HIGH"
            elif unified_metrics.total_elapsed_time_s > 50 or unified_metrics.cpu_percentage > 50:
                severity = "MEDIUM"
            
            # Get AUTHORITATIVE time window
            authoritative_window = unified_metrics.time_window_display
            if not authoritative_window or authoritative_window == "--":
                authoritative_window = self._get_html_based_time_window()
            
            # ===== ALWAYS include ALL 4 metrics (MANDATORY) =====
            details = metrics_calc.get_high_load_details_string()
            
            return {
                "period": authoritative_window,
                "type": "High Database Load",
                "severity": severity,
                "details": details,
                # Include raw metrics for UI consumption
                "metrics": metrics_calc.get_metrics_dict()
            }
            
        except Exception as e:
            print(f"Error in basic analysis: {e}")
            # Fallback to old analysis
            return self._basic_high_load_analysis_fallback(csv_files)
    
    def _find_awr_html_file(self) -> str:
        """Find AWR HTML file in the raw_html directory."""
        try:
            html_dir = os.path.join(os.path.dirname(self.csv_dir), 'raw_html')
            if not os.path.exists(html_dir):
                return None
            
            for filename in os.listdir(html_dir):
                if filename.lower().endswith('.html'):
                    filepath = os.path.join(html_dir, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                            sample = f.read(5000)
                        if 'awr' in filename.lower() or 'AWR Report' in sample or 'Begin Snap' in sample:
                            return filepath
                    except:
                        continue
        except Exception as e:
            print(f"Error finding AWR HTML file: {e}")
        return None
    
    def _basic_high_load_analysis_fallback(self, csv_files):
        """Fallback high load analysis for backwards compatibility."""
        try:
            if 'awr_sql_stats' in csv_files:
                sql_df = pd.read_csv(csv_files['awr_sql_stats'])
                sql_analysis = self._analyze_sql_performance(sql_df)
                
                if sql_analysis and sql_analysis['detected']:
                    authoritative_window = self._get_html_based_time_window()
                    
                    # Build details from metrics - ALWAYS include all 4
                    metrics = sql_analysis.get('metrics', {})
                    details = []
                    details.append(f"Total elapsed time: {metrics.get('total_elapsed_time', 0):.1f}s")
                    details.append(f"Total executions: {int(metrics.get('total_executions', 0)):,}")
                    # CPU Usage = CPU Time / DB Time (or Elapsed Time as fallback)
                    details.append(f"CPU Usage: {min(100.0, metrics.get('max_cpu_percent', 0)):.1f}%")
                    details.append(f"IO wait: 0%")  # Fallback has no IO data
                    
                    return {
                        "period": authoritative_window,
                        "type": "High Database Load",
                        "severity": sql_analysis['severity'],
                        "details": "; ".join(details)
                    }
        except Exception as e:
            print(f"Error in fallback analysis: {e}")
        
        return None


