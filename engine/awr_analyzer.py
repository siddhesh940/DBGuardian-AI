import os
import pandas as pd
import numpy as np
import glob
from typing import Optional, List


class AWRAnalyzer:
    def __init__(self, csv_dir: str, time_filter=None) -> None:
        self.csv_dir: str = csv_dir
        self.time_filter = time_filter  # For future enhancement

    # -----------------------------
    # helper: find CSV files dynamically
    # -----------------------------
    def _find_csv_file(self, pattern) -> Optional[str]:
        """Find CSV file matching the pattern (e.g., 'awr_sql_stats')"""
        search_pattern = os.path.join(self.csv_dir, "*{}*.csv".format(pattern))
        matches = glob.glob(search_pattern)

        if matches:
            print("✅ Found {} CSV: {}".format(pattern, matches[0]))
            return matches[0]

        print("❌ No {} CSV found in {}".format(pattern, self.csv_dir))
        return None

    # -----------------------------
    # helper: find column safely
    # -----------------------------
    def _find_col(self, df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    # -----------------------------
    # TOP SQL ANALYSIS
    # -----------------------------
    def top_sql(self, limit: int = 5):
        sql_stats_file = self._find_csv_file("awr_sql_stats")
        if not sql_stats_file:
            print("No SQL stats CSV file found in {}".format(self.csv_dir))
            return [], []

        df = pd.read_csv(sql_stats_file)
        df.columns = df.columns.str.strip().str.lower()

        col_sql_id = self._find_col(df, ["sql_id"])
        col_elapsed = self._find_col(df, ["elapsed__time_s", "elapsed_time_s"])
        col_pctcpu = self._find_col(df, ["pctcpu"])
        col_executions = self._find_col(df, ["executions"])
        col_elapsed_per_exec = self._find_col(df,
                                              ["elapsed_time_per_exec_s",
                                               "elapsed_per_exec_s"])
        col_pcttotal = self._find_col(df, ["pcttotal"])
        col_pctio = self._find_col(df, ["pctio"])

        if not col_sql_id or not col_elapsed:
            return [], []

        df[col_elapsed] = pd.to_numeric(df[col_elapsed], errors="coerce").fillna(0)

        if col_pctcpu:
            df[col_pctcpu] = pd.to_numeric(df[col_pctcpu], errors="coerce").fillna(0)
        else:
            df["pctcpu"] = 0.0
            col_pctcpu = "pctcpu"

        if col_executions:
            df[col_executions] = pd.to_numeric(df[col_executions],
                                               errors="coerce").fillna(0)
        else:
            df["executions"] = 0
            col_executions = "executions"

        if col_elapsed_per_exec:
            df[col_elapsed_per_exec] = pd.to_numeric(df[col_elapsed_per_exec],
                                                     errors="coerce").fillna(0)
        else:
            df["elapsed_per_exec"] = 0.0
            col_elapsed_per_exec = "elapsed_per_exec"

        if col_pcttotal:
            df[col_pcttotal] = pd.to_numeric(df[col_pcttotal],
                                             errors="coerce").fillna(0)
        else:
            df["pcttotal"] = 0.0
            col_pcttotal = "pcttotal"

        if col_pctio:
            df[col_pctio] = pd.to_numeric(df[col_pctio],
                                          errors="coerce").fillna(0)
        else:
            df["pctio"] = 0.0
            col_pctio = "pctio"

        df_filtered = df[df[col_elapsed] > 0]

        raw_sql = []
        for _, r in df_filtered.iterrows():
            raw_sql.append({
                "sql_id": str(r[col_sql_id]),
                "elapsed": float(r[col_elapsed])
            })

        df_top = df_filtered.sort_values(col_elapsed, ascending=False).head(limit)

        top_sql = []

        for _, r in df_top.iterrows():
            elapsed = float(r[col_elapsed])
            cpu_pct = float(r[col_pctcpu] or 0)
            executions = int(r[col_executions] or 0)
            elapsed_per_exec = float(r[col_elapsed_per_exec] or 0)
            pcttotal = float(r[col_pcttotal] or 0)
            pctio = float(r[col_pctio] or 0)

            if pd.isna(elapsed) or np.isinf(elapsed):
                elapsed = 0.0
            if pd.isna(cpu_pct) or np.isinf(cpu_pct):
                cpu_pct = 0.0
            if pd.isna(executions) or np.isinf(executions):
                executions = 0
            if pd.isna(elapsed_per_exec) or np.isinf(elapsed_per_exec):
                elapsed_per_exec = 0.0
            if pd.isna(pcttotal) or np.isinf(pcttotal):
                pcttotal = 0.0
            if pd.isna(pctio) or np.isinf(pctio):
                pctio = 0.0

            cpu = round(elapsed * cpu_pct / 100, 2)
            if pd.isna(cpu) or np.isinf(cpu):
                cpu = 0.0

            risk = "LOW"
            if elapsed >= 100:
                risk = "HIGH"
            elif elapsed >= 50:
                risk = "MEDIUM"

            top_sql.append({
                "sql_id": str(r[col_sql_id]),
                "elapsed": round(elapsed, 2),
                "cpu": round(cpu, 2),
                "executions": executions,
                "elapsed_per_exec": round(elapsed_per_exec, 3),
                "risk": risk,
                "pcttotal": round(pcttotal, 2),
                "pctcpu": round(cpu_pct, 2),
                "pctio": round(pctio, 2)
            })

        return top_sql, raw_sql

    # -----------------------------
    # WAIT EVENTS
    # -----------------------------
    def top_wait_events(self, limit: int = 5):
        wait_events_file = self._find_csv_file("awr_wait_events")
        if not wait_events_file:
            print("No wait events CSV file found in {}".format(self.csv_dir))
            return []

        df = pd.read_csv(wait_events_file)
        df.columns = df.columns.str.strip().str.lower()

        col_name = self._find_col(df, ["statistic_name"])
        col_time = self._find_col(df, ["time_s"])
        col_pct_db = self._find_col(df, ["pct_of_db_time"])
        col_pct_cpu = self._find_col(df, ["pct_of_total_cpu_time"])

        if not col_name or not col_time:
            return []

        df[col_time] = pd.to_numeric(df[col_time], errors="coerce").fillna(0)

        if col_pct_db:
            df[col_pct_db] = pd.to_numeric(df[col_pct_db],
                                           errors="coerce").fillna(0)

        if col_pct_cpu:
            df[col_pct_cpu] = pd.to_numeric(df[col_pct_cpu],
                                            errors="coerce").fillna(0)

        df = df[df[col_time] > 0]
        df = df.sort_values(col_time, ascending=False).head(limit)

        events = []

        for _, r in df.iterrows():
            time_s = float(r[col_time])
            pct_db_time = float(r[col_pct_db]) if col_pct_db else 0.0
            pct_cpu_time = float(r[col_pct_cpu]) if col_pct_cpu else 0.0

            if pd.isna(time_s) or np.isinf(time_s):
                time_s = 0.0
            if pd.isna(pct_db_time) or np.isinf(pct_db_time):
                pct_db_time = 0.0
            if pd.isna(pct_cpu_time) or np.isinf(pct_cpu_time):
                pct_cpu_time = 0.0

            events.append({
                "statistic_name": str(r[col_name]),
                "time_s": round(time_s, 2),
                "pct_of_db_time": round(pct_db_time, 2),
                "pct_of_total_cpu_time": round(pct_cpu_time, 2)
            })

        return events

