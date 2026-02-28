"""
Snapshot Metadata Parser - Extract authoritative snapshot metadata from AWR HTML files

This parser extracts:
- Begin snapshot time (datetime)
- End snapshot time (datetime)  
- Elapsed seconds
- DB CPU time (seconds)
- CPU core count (if available)

IMPORTANT: This parser returns RAW metadata with NO rounding or UI formatting.
All display formatting is handled by engine/time_window_detector.py
"""

from io import TextIOWrapper
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup, Tag, Tag, Tag, Tag, Tag, Tag, Tag, Tag, Tag, Tag, Tag, ResultSet
from bs4.element import Tag, ResultSet
from bs4.element import Tag, ResultSet
from bs4.element import Tag, Tag, Tag, ResultSet
from bs4.element import Tag, ResultSet
from bs4.element import Tag, ResultSet
from bs4.element import Tag, Tag


class SnapshotMetadataParser:
    """
    Parse AWR HTML files to extract authoritative snapshot metadata.
    This is the single source of truth for time windows and CPU metrics.
    """
    
    def __init__(self, html_file_path: str) -> None:
        """
        Initialize with path to AWR HTML file.
        
        Args:
            html_file_path: Full path to the AWR HTML file to parse
        """
        self.html_file_path: str = html_file_path
        self.soup = None
        self.content = None
        
    def parse(self) -> Dict[str, Any]:
        """
        Parse the AWR HTML file and extract snapshot metadata.
        
        Returns:
            Dict containing:
                - begin_time: datetime of snapshot begin
                - end_time: datetime of snapshot end
                - elapsed_seconds: float, elapsed time in seconds
                - db_cpu_seconds: float, DB CPU time in seconds
                - cpu_cores: int, number of CPU cores (if available)
                - parse_success: bool, whether parsing was successful
                - parse_errors: list of any errors encountered
        """
        result = {
            "begin_time": None,
            "end_time": None,
            "elapsed_seconds": None,
            "db_cpu_seconds": None,
            "cpu_cores": None,
            "instance_cpu_busy_pct": None,  # Instance CPU → %Busy CPU (PRIMARY source)
            "host_cpu_idle_pct": None,       # Host CPU → %Idle (for fallback)
            "parse_success": False,
            "parse_errors": []
        }
        
        try:
            with open(self.html_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                self.content = f.read()
            self.soup = BeautifulSoup(self.content, 'html.parser')
            
            # Extract snapshot times (AUTHORITATIVE source)
            snapshot_times: Dict[str, datetime] | None = self._extract_snapshot_times()
            if snapshot_times:
                result["begin_time"] = snapshot_times.get("begin")
                result["end_time"] = snapshot_times.get("end")
            else:
                result["parse_errors"].append("Could not extract snapshot times")
            
            # Extract elapsed time
            elapsed: float | None = self._extract_elapsed_time()
            if elapsed is not None:
                result["elapsed_seconds"] = elapsed
            else:
                # Try to calculate from begin/end times
                if result["begin_time"] and result["end_time"]:
                    delta = result["end_time"] - result["begin_time"]
                    # Handle cross-midnight scenarios
                    if delta.total_seconds() < 0:
                        delta = delta + timedelta(days=1)
                    result["elapsed_seconds"] = delta.total_seconds()
                else:
                    result["parse_errors"].append("Could not extract elapsed time")
            
            # Extract DB CPU time
            db_cpu: float | None = self._extract_db_cpu_time()
            if db_cpu is not None:
                result["db_cpu_seconds"] = db_cpu
            else:
                result["parse_errors"].append("Could not extract DB CPU time")
            
            # Extract CPU core count
            cpu_cores: int | None = self._extract_cpu_cores()
            if cpu_cores is not None:
                result["cpu_cores"] = cpu_cores
            
            # Extract Instance CPU %Busy CPU (PRIMARY source for Max CPU Usage)
            instance_cpu: float | None = self._extract_instance_cpu_busy_pct()
            if instance_cpu is not None:
                result["instance_cpu_busy_pct"] = instance_cpu
            
            # Extract Host CPU %Idle (for fallback calculation)
            host_cpu_idle: float | None = self._extract_host_cpu_idle_pct()
            if host_cpu_idle is not None:
                result["host_cpu_idle_pct"] = host_cpu_idle
            
            # Determine success
            result["parse_success"] = (
                result["begin_time"] is not None and 
                result["end_time"] is not None
            )
            
        except Exception as e:
            result["parse_errors"].append(f"Failed to parse HTML file: {str(e)}")
            
        return result
    
    def _extract_snapshot_times(self) -> Optional[Dict[str, datetime]]:
        """
        Extract Begin Snap and End Snap timestamps from AWR report.
        
        Returns:
            Dict with 'begin' and 'end' datetime values, or None if not found
        """
        try:
            begin_time = None
            end_time = None
            
            # Method 1: Look for Begin Snap: / End Snap: in table rows
            for tr in self.soup.find_all('tr'):
                cells: list[str] = [td.get_text(strip=True) for td in tr.find_all('td')]
                if len(cells) >= 3:
                    cell_text: str = cells[0].lower()
                    if 'begin snap' in cell_text:
                        # Timestamp is usually in the 3rd cell (index 2)
                        timestamp_text: str = cells[2] if len(cells) > 2 else cells[-1]
                        begin_time: datetime | None = self._parse_oracle_timestamp(timestamp_text)
                    elif 'end snap' in cell_text:
                        timestamp_text: str = cells[2] if len(cells) > 2 else cells[-1]
                        end_time: datetime | None = self._parse_oracle_timestamp(timestamp_text)
            
            # Method 2: Search in text content if table parsing failed
            if not begin_time or not end_time:
                # Pattern: "Begin Snap: ... 09-Aug-20 21:00:54"
                begin_match: re.Match[str] | None = re.search(
                    r'Begin\s+Snap[:\s]+\d+\s+(\d{1,2}-\w{3}-\d{2}\s+\d{2}:\d{2}:\d{2})',
                    self.content, re.IGNORECASE
                )
                end_match: re.Match[str] | None = re.search(
                    r'End\s+Snap[:\s]+\d+\s+(\d{1,2}-\w{3}-\d{2}\s+\d{2}:\d{2}:\d{2})',
                    self.content, re.IGNORECASE
                )
                
                if begin_match and not begin_time:
                    begin_time: datetime | None = self._parse_oracle_timestamp(begin_match.group(1))
                if end_match and not end_time:
                    end_time: datetime | None = self._parse_oracle_timestamp(end_match.group(1))
            
            # Method 3: Try ASH-style format "From ... To ..."
            if not begin_time or not end_time:
                ash_match: re.Match[str] | None = re.search(
                    r'From\s+(\d{2}-\w{3}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+To\s+(\d{2}-\w{3}-\d{2}\s+\d{2}:\d{2}:\d{2})',
                    self.content, re.IGNORECASE
                )
                if ash_match:
                    begin_time: datetime | None = self._parse_oracle_timestamp(ash_match.group(1))
                    end_time: datetime | None = self._parse_oracle_timestamp(ash_match.group(2))
            
            if begin_time and end_time:
                return {"begin": begin_time, "end": end_time}
                
        except Exception as e:
            print(f"Error extracting snapshot times: {e}")
            
        return None
    
    def _extract_elapsed_time(self) -> Optional[float]:
        """
        Extract elapsed time from AWR report in seconds.
        
        Returns:
            Elapsed time in seconds, or None if not found
        """
        try:
            # Method 1: Look for "Elapsed:" or "Elapsed Time:" in tables
            for tr in self.soup.find_all('tr'):
                cells: list[str] = [td.get_text(strip=True) for td in tr.find_all('td')]
                for i, cell in enumerate(cells):
                    cell_lower: str = cell.lower()
                    if 'elapsed' in cell_lower and 'time' not in cell_lower:
                        # Next cell(s) might have the value
                        for j in range(i+1, min(i+3, len(cells))):
                            elapsed: float | None = self._parse_time_value(cells[j])
                            if elapsed is not None:
                                return elapsed
            
            # Method 2: Search for "Elapsed: X.XX (mins)" pattern
            elapsed_match: re.Match[str] | None = re.search(
                r'Elapsed[:\s]+([\d,.]+)\s*\(?(?:mins?|minutes?)\)?',
                self.content, re.IGNORECASE
            )
            if elapsed_match:
                minutes = float(elapsed_match.group(1).replace(',', ''))
                return minutes * 60  # Convert to seconds
            
            # Method 3: Look for explicit seconds
            elapsed_sec_match: re.Match[str] | None = re.search(
                r'Elapsed[:\s]+([\d,.]+)\s*(?:sec(?:ond)?s?)',
                self.content, re.IGNORECASE
            )
            if elapsed_sec_match:
                return float(elapsed_sec_match.group(1).replace(',', ''))
            
        except Exception as e:
            print(f"Error extracting elapsed time: {e}")
            
        return None
    
    def _extract_db_cpu_time(self) -> Optional[float]:
        """
        Extract DB CPU time from AWR report in seconds.
        
        Returns:
            DB CPU time in seconds, or None if not found
        """
        try:
            # Method 1: Look for "DB CPU" in wait events table (total seconds)
            # IMPORTANT: Skip rows with "(s)" which indicate per-second rates from Load Profile
            for tr in self.soup.find_all('tr'):
                cells: list[str] = [td.get_text(strip=True) for td in tr.find_all('td')]
                for i, cell in enumerate(cells):
                    if 'DB CPU' in cell or 'db cpu' in cell.lower():
                        # Skip per-second metrics (e.g., "DB CPU(s):" from Load Profile)
                        if '(s)' in cell or '/s' in cell.lower():
                            continue
                        # Look for time value in subsequent cells
                        for j in range(i+1, min(i+4, len(cells))):
                            cpu_time: float | None = self._parse_time_value(cells[j])
                            if cpu_time is not None and cpu_time > 0:
                                return cpu_time
            
            # Method 2: Look in Load Profile section
            for tr in self.soup.find_all('tr'):
                cells: list[str] = [td.get_text(strip=True) for td in tr.find_all('td')]
                if len(cells) >= 2:
                    if 'DB CPU' in cells[0] and '(s)' in cells[0]:
                        cpu_per_sec: float | None = self._parse_numeric_value(cells[1])
                        if cpu_per_sec is not None:
                            # This is per-second, need to multiply by elapsed
                            # Will be handled by caller if needed
                            return cpu_per_sec
            
            # Method 3: Search for pattern in text
            cpu_match: re.Match[str] | None = re.search(
                r'DB\s+CPU[:\s]+([\d,.]+)\s*(?:s|sec|seconds?)?',
                self.content, re.IGNORECASE
            )
            if cpu_match:
                return float(cpu_match.group(1).replace(',', ''))
                
        except Exception as e:
            print(f"Error extracting DB CPU time: {e}")
            
        return None
    
    def _extract_cpu_cores(self) -> Optional[int]:
        """
        Extract CPU core count from AWR report.
        
        Returns:
            Number of CPU cores, or None if not found
        """
        try:
            # Method 1: Look for "CPUs" in the report header/metadata
            for tr in self.soup.find_all('tr'):
                cells: list[str] = [td.get_text(strip=True) for td in tr.find_all('td')]
                for i, cell in enumerate(cells):
                    cell_lower: str = cell.lower()
                    if 'cpu' in cell_lower and ('core' in cell_lower or 'count' in cell_lower or cell_lower.endswith('cpus')):
                        for j in range(i+1, min(i+3, len(cells))):
                            cores: int | None = self._parse_integer_value(cells[j])
                            if cores is not None and 0 < cores <= 1024:
                                return cores
            
            # Method 2: Search for patterns in text
            cpu_patterns: list[str] = [
                r'(\d+)\s*CPU[s]?(?:\s+cores?)?',
                r'CPU[s]?[:\s]+(\d+)',
                r'Num\s+CPU[s]?[:\s]+(\d+)',
            ]
            for pattern in cpu_patterns:
                match: re.Match[str] | None = re.search(pattern, self.content, re.IGNORECASE)
                if match:
                    cores = int(match.group(1))
                    if 0 < cores <= 1024:  # Sanity check
                        return cores
            
            # Method 3: Look in report summary section
            summary_match: re.Match[str] | None = re.search(r'CPUs[:\s]+(\d+)', self.content)
            if summary_match:
                return int(summary_match.group(1))
                
        except Exception as e:
            print(f"Error extracting CPU cores: {e}")
            
        return None
    
    def _extract_instance_cpu_busy_pct(self) -> Optional[float]:
        """
        Extract Instance CPU → %Busy CPU from AWR report.
        
        This is the PRIMARY source for Max CPU Usage.
        The value represents actual Oracle DB CPU usage as a percentage.
        
        Returns:
            %Busy CPU value (0.0 - 100.0), or None if not found
        """
        try:
            from bs4 import NavigableString
            
            # Method 1: Search for "Instance CPU" as a text node (NavigableString)
            # In AWR HTML, "Instance CPU" appears as plain text between <p /> tags
            for element in self.soup.body.descendants if self.soup.body else []:
                if isinstance(element, NavigableString):
                    text: str = element.strip()
                    if text == 'Instance CPU':
                        # Find the next table after this text
                        table: Tag | None = element.find_next('table')
                        if table:
                            rows: ResultSet[Tag] = table.find_all('tr')
                            if len(rows) >= 2:
                                # Get header row to find %Busy CPU column index
                                header_cells: ResultSet[Tag] = rows[0].find_all(['th', 'td'])
                                busy_cpu_idx = None
                                
                                for idx, cell in enumerate(header_cells):
                                    cell_text: str = cell.get_text(strip=True).lower()
                                    if '%busy cpu' in cell_text or 'busy cpu' in cell_text:
                                        busy_cpu_idx: int = idx
                                        break
                                
                                if busy_cpu_idx is not None:
                                    # Get value from data row
                                    data_cells: ResultSet[Tag] = rows[1].find_all(['th', 'td'])
                                    if len(data_cells) > busy_cpu_idx:
                                        value_text: str = data_cells[busy_cpu_idx].get_text(strip=True)
                                        busy_cpu: float | None = self._parse_numeric_value(value_text)
                                        if busy_cpu is not None:
                                            return min(100.0, busy_cpu)
            
            # Method 2: Also try looking in tags (for different HTML structures)
            for tag in self.soup.find_all(['p', 'h2', 'h3', 'b', 'font']):
                text: str = tag.get_text(strip=True)
                if text == 'Instance CPU' or text.lower() == 'instance cpu':
                    table: Tag | None = tag.find_next('table')
                    if table:
                        rows: ResultSet[Tag] = table.find_all('tr')
                        if len(rows) >= 2:
                            header_cells: ResultSet[Tag] = rows[0].find_all(['th', 'td'])
                            busy_cpu_idx = None
                            
                            for idx, cell in enumerate(header_cells):
                                cell_text: str = cell.get_text(strip=True).lower()
                                if '%busy cpu' in cell_text or 'busy cpu' in cell_text:
                                    busy_cpu_idx: int = idx
                                    break
                            
                            if busy_cpu_idx is not None:
                                data_cells: ResultSet[Tag] = rows[1].find_all(['th', 'td'])
                                if len(data_cells) > busy_cpu_idx:
                                    value_text: str = data_cells[busy_cpu_idx].get_text(strip=True)
                                    busy_cpu: float | None = self._parse_numeric_value(value_text)
                                    if busy_cpu is not None:
                                        return min(100.0, busy_cpu)
                
        except Exception as e:
            print(f"Error extracting Instance CPU %Busy CPU: {e}")
            
        return None
    
    def _extract_host_cpu_idle_pct(self) -> Optional[float]:
        """
        Extract Host CPU → %Idle from AWR report.
        
        This is the PRIMARY source for Max CPU Usage calculation.
        Max CPU = 100 - %Idle
        
        Returns:
            %Idle value (0.0 - 100.0), or None if not found
        """
        try:
            from bs4 import NavigableString
            
            # Method 1: Search for "Host CPU" as a NavigableString (most reliable)
            # In AWR HTML, "Host CPU" appears as plain text between tags
            for element in self.soup.body.descendants if self.soup.body else []:
                if isinstance(element, NavigableString):
                    text = element.strip()
                    if text == 'Host CPU':
                        # Find the next table after this text
                        table = element.find_next('table')
                        if table:
                            rows = table.find_all('tr')
                            if len(rows) >= 2:
                                # Get header row to find %Idle column index
                                header_cells = rows[0].find_all(['th', 'td'])
                                idle_idx = None
                                
                                for idx, cell in enumerate(header_cells):
                                    cell_text = cell.get_text(strip=True).lower()
                                    if '%idle' in cell_text or cell_text == 'idle':
                                        idle_idx = idx
                                        break
                                
                                if idle_idx is not None:
                                    # Get value from data row
                                    data_cells = rows[1].find_all(['th', 'td'])
                                    if len(data_cells) > idle_idx:
                                        value_text = data_cells[idle_idx].get_text(strip=True)
                                        idle_pct = self._parse_numeric_value(value_text)
                                        if idle_pct is not None:
                                            return idle_pct
            
            # Method 2: Look for "Host CPU" in tags (fallback)
            for tag in self.soup.find_all(['p', 'h2', 'h3', 'b', 'font']):
                text = tag.get_text(strip=True)
                if text == 'Host CPU' or text.lower() == 'host cpu':
                    table = tag.find_next('table')
                    if table:
                        rows = table.find_all('tr')
                        if len(rows) >= 2:
                            header_cells = rows[0].find_all(['th', 'td'])
                            idle_idx = None
                            
                            for idx, cell in enumerate(header_cells):
                                cell_text = cell.get_text(strip=True).lower()
                                if '%idle' in cell_text or cell_text == 'idle':
                                    idle_idx = idx
                                    break
                            
                            if idle_idx is not None:
                                data_cells = rows[1].find_all(['th', 'td'])
                                if len(data_cells) > idle_idx:
                                    value_text = data_cells[idle_idx].get_text(strip=True)
                                    idle_pct = self._parse_numeric_value(value_text)
                                    if idle_pct is not None:
                                        return idle_pct
                
        except Exception as e:
            print(f"Error extracting Host CPU %Idle: {e}")
            
        return None
    
    def _parse_oracle_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse Oracle timestamp formats into datetime.
        
        Supported formats:
        - "06-Dec-25 10:50:19"
        - "09-Aug-20 21:00:54"
        - "09-8月 -20 21:00:54" (Chinese month names)
        """
        if not timestamp_str:
            return None
            
        try:
            # Pattern 1: Standard English format "06-Dec-25 10:50:19"
            match: re.Match[str] | None = re.search(r'(\d{1,2})-(\w{3})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})', timestamp_str)
            if match:
                day, month, year, hour, minute, second = match.groups()
                month_map: Dict[str, int] = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month_num: int = month_map.get(month.lower(), 1)
                year_full: int = 2000 + int(year) if int(year) < 50 else 1900 + int(year)
                return datetime(year_full, month_num, int(day), int(hour), int(minute), int(second))
            
            # Pattern 2: Chinese month format "09-8月 -20 21:00:54"
            match: re.Match[str] | None = re.search(r'(\d{1,2})-(\d{1,2})月\s*-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})', timestamp_str)
            if match:
                day, month, year, hour, minute, second = match.groups()
                year_full: int = 2000 + int(year) if int(year) < 50 else 1900 + int(year)
                return datetime(year_full, int(month), int(day), int(hour), int(minute), int(second))
            
            # Pattern 3: Just extract time if no full pattern matches
            time_match: re.Match[str] | None = re.search(r'(\d{2}):(\d{2}):(\d{2})', timestamp_str)
            if time_match:
                hour, minute, second = time_match.groups()
                today: datetime = datetime.now()
                return datetime(today.year, today.month, today.day, int(hour), int(minute), int(second))
                
        except Exception as e:
            print(f"Error parsing timestamp '{timestamp_str}': {e}")
            
        return None
    
    def _parse_time_value(self, value_str: str) -> Optional[float]:
        """Parse a time value that might be in seconds, minutes, or with units."""
        if not value_str:
            return None
        try:
            # Remove common noise
            clean: str = value_str.strip().replace(',', '')
            
            # Check for explicit units
            if 'min' in clean.lower():
                num_match: re.Match[str] | None = re.search(r'([\d.]+)', clean)
                if num_match:
                    return float(num_match.group(1)) * 60
            elif 'sec' in clean.lower() or clean.endswith('s'):
                num_match: re.Match[str] | None = re.search(r'([\d.]+)', clean)
                if num_match:
                    return float(num_match.group(1))
            else:
                # Try to extract numeric value
                num_match: re.Match[str] | None = re.search(r'^([\d.]+)$', clean)
                if num_match:
                    return float(num_match.group(1))
        except:
            pass
        return None
    
    def _parse_numeric_value(self, value_str: str) -> Optional[float]:
        """Parse a numeric value from string."""
        if not value_str:
            return None
        try:
            clean: str = value_str.strip().replace(',', '')
            num_match: re.Match[str] | None = re.search(r'([\d.]+)', clean)
            if num_match:
                return float(num_match.group(1))
        except:
            pass
        return None
    
    def _parse_integer_value(self, value_str: str) -> Optional[int]:
        """Parse an integer value from string."""
        if not value_str:
            return None
        try:
            clean: str = value_str.strip().replace(',', '')
            num_match: re.Match[str] | None = re.search(r'(\d+)', clean)
            if num_match:
                return int(num_match.group(1))
        except:
            pass
        return None


def parse_snapshot_metadata(html_file_path: str) -> Dict[str, Any]:
    """
    Convenience function to parse snapshot metadata from an AWR HTML file.
    
    Args:
        html_file_path: Full path to the AWR HTML file
        
    Returns:
        Dict containing snapshot metadata (see SnapshotMetadataParser.parse())
    """
    parser = SnapshotMetadataParser(html_file_path)
    return parser.parse()







