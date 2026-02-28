import os
import pandas as pd
import glob
from datetime import date, datetime, time
from typing import Any, Optional, Tuple


class ASHAnalyzer:
    def __init__(self, csv_dir, time_filter=None) -> None:
        self.csv_dir = csv_dir
        self.time_filter = time_filter

    # -----------------------------
    # Find CSV File
    # -----------------------------
    def _find_csv_file(self, pattern):
        search_pattern = os.path.join(self.csv_dir, "*{}*.csv".format(pattern))
        matches = glob.glob(search_pattern)

        if matches:
            print("✅ Found {} ASH CSV: {}".format(pattern, matches[0]))
            return matches[0]

        print("❌ No {} ASH CSV found in {}".format(pattern, self.csv_dir))
        return None

    # -----------------------------
    # Parse Time
    # -----------------------------
    def _parse_time_from_slot(self, slot_time_str):
        try:
            text = str(slot_time_str).strip()
            time_part = text.split()[0]        # HH:MM:SS
            t = datetime.strptime(time_part, "%H:%M:%S").time()
            return datetime.combine(datetime.now().date(), t)

        except Exception as e:
            print("Error parsing time:", slot_time_str, e)
            return None

    # -----------------------------
    # Time Range Builder
    # -----------------------------
    def _create_time_filter_range(self):
        if not self.time_filter:
            return None, None

        try:
            start_hour = self.time_filter.get("start_hour", 0)
            start_minute = self.time_filter.get("start_minute", 0)
            end_hour = self.time_filter.get("end_hour", 23)
            end_minute = self.time_filter.get("end_minute", 59)

            today = datetime.now().date()

            start_time = datetime.combine(today, time(start_hour, start_minute))
            end_time = datetime.combine(today, time(end_hour, end_minute))

            return start_time, end_time

        except Exception as e:
            print("Error creating time filter:", str(e))
            return None, None

    # -----------------------------
    # Filter ASH
    # -----------------------------
    def _filter_ash_data_by_time(self, df):
        if not self.time_filter:
            return df

        start_time, end_time = self._create_time_filter_range()
        if not start_time or not end_time:
            return df

        df = df.copy()
        df["parsed_time"] = df["slot_time_(duration)"].apply(self._parse_time_from_slot)
        df = df.dropna(subset=["parsed_time"])

        mask = (df["parsed_time"] >= start_time) & (df["parsed_time"] <= end_time)
        filtered_df = df[mask].copy()

        print("ASH time filter applied: {} -> {} rows".format(len(df), len(filtered_df)))
        print("Time range: {} to {}".format(start_time.strftime("%H:%M:%S"),
                                            end_time.strftime("%H:%M:%S")))

        filtered_df = filtered_df.drop("parsed_time", axis=1)
        return filtered_df

    # -----------------------------
    # Dominant Events
    # -----------------------------
    def analyze_dominant_events(self, limit=10):
        ash_file = self._find_csv_file("ash_activity_over_time")
        if not ash_file:
            return []

        df = pd.read_csv(ash_file)
        df = self._filter_ash_data_by_time(df)

        if df.empty:
            return []

        df["event_count"] = pd.to_numeric(df["event_count"], errors="coerce").fillna(0)
        df["%_event"] = pd.to_numeric(df["%_event"], errors="coerce").fillna(0)
        df["slot_count"] = pd.to_numeric(df["slot_count"], errors="coerce").fillna(0)

        event_stats = df.groupby("event").agg({
            "event_count": "sum",
            "%_event": "mean",
            "slot_count": "count"
        }).reset_index()

        totals = df.groupby("event")["%_event"].sum().reset_index()
        totals.columns = ["event", "total_percent_impact"]

        event_stats = event_stats.merge(totals, on="event")
        event_stats = event_stats.sort_values("total_percent_impact", ascending=False)

        def classify(event_name):
            name = str(event_name).lower()

            if "cpu" in name:
                return "CPU"
            if any(x in name for x in ["read", "write", "io", "disk", "file"]):
                return "IO"
            if any(x in name for x in ["latch", "lock", "enq", "buffer"]):
                return "Concurrency"
            if "sql*net" in name or "network" in name:
                return "Network"
            return "Other"

        event_stats["event_class"] = event_stats["event"].apply(classify)

        results = []
        for _, row in event_stats.head(limit).iterrows():
            results.append({
                "event": row["event"],
                "event_class": row["event_class"],
                "total_event_count": int(row["event_count"]),
                "average_percent": float(round(row["%_event"], 2)),
                "total_percent_impact": float(round(row["total_percent_impact"], 2)),
                "time_slots_affected": int(row["slot_count"])
            })

        return results

    # -----------------------------
    # Spikes
    # -----------------------------
    def detect_activity_spikes(self, threshold_percent=5.0):
        ash_file = self._find_csv_file("ash_activity_over_time")
        if not ash_file:
            return []

        df = pd.read_csv(ash_file)
        df = self._filter_ash_data_by_time(df)

        if df.empty:
            return []

        df["%_event"] = pd.to_numeric(df["%_event"], errors="coerce").fillna(0)
        spikes = df[df["%_event"] >= threshold_percent].copy()
        spikes = spikes.sort_values("%_event", ascending=False)

        results = []
        for _, row in spikes.iterrows():
            results.append({
                "time_slot": row["slot_time_(duration)"],
                "event": row["event"],
                "percent_impact": float(row["%_event"]),
                "event_count": int(row["event_count"]),
                "total_sessions": int(row["slot_count"])
            })

        return results

    # -----------------------------
    # Summary
    # -----------------------------
    def get_time_window_summary(self):
        ash_file = self._find_csv_file("ash_activity_over_time")
        if not ash_file:
            return {}

        df = pd.read_csv(ash_file)
        original = len(df)

        df = self._filter_ash_data_by_time(df)
        filtered = len(df)

        if df.empty:
            start, end = self._create_time_filter_range()
            return {
                "time_window_applied": True,
                "original_data_points": original,
                "filtered_data_points": 0,
                "time_slots_analyzed": 0,
                "total_activity": 0,
                "dominant_event": None
            }

        df["event_count"] = pd.to_numeric(df["event_count"], errors="coerce").fillna(0)
        df["%_event"] = pd.to_numeric(df["%_event"], errors="coerce").fillna(0)

        slots = df["slot_time_(duration)"].nunique()
        total_activity = int(df["event_count"].sum())

        event_impact = df.groupby("event")["%_event"].sum().sort_values(ascending=False)

        dominant_event = None
        if len(event_impact) > 0:
            dominant_event = {
                "event": event_impact.index[0],
                "total_impact": float(round(event_impact.iloc[0], 2))
            }

        start, end = self._create_time_filter_range()

        return {
            "time_window_applied": True,
            "time_range": {
                "start": start.strftime("%H:%M:%S") if start else "N/A",
                "end": end.strftime("%H:%M:%S") if end else "N/A"
            },
            "original_data_points": original,
            "filtered_data_points": filtered,
            "time_slots_analyzed": slots,
            "total_activity": total_activity,
            "dominant_event": dominant_event
        }

    # -----------------------------
    # CPU vs IO Breakdown
    # -----------------------------
    def get_cpu_vs_io_breakdown(self):
        events = self.analyze_dominant_events()
        if not events:
            return {}

        breakdown = {
            "CPU": {"count": 0, "total_percent": 0, "events": []},
            "IO": {"count": 0, "total_percent": 0, "events": []},
            "Concurrency": {"count": 0, "total_percent": 0, "events": []},
            "Network": {"count": 0, "total_percent": 0, "events": []},
            "Other": {"count": 0, "total_percent": 0, "events": []},
        }

        for e in events:
            cls = e.get("event_class", "Other")
            breakdown[cls]["count"] += 1
            breakdown[cls]["total_percent"] += e.get("total_percent_impact", 0)
            breakdown[cls]["events"].append({
                "event": e.get("event"),
                "impact": e.get("total_percent_impact", 0)
            })

        for k in breakdown:
            breakdown[k]["total_percent"] = float(round(breakdown[k]["total_percent"], 2))

        return breakdown

