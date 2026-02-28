import sys
import os
import pandas as pd
from typing import Optional
from bs4 import BeautifulSoup, Tag, Tag, Tag, Tag, Tag, Tag

# ⚠️ WARNING: This fallback should NEVER be used in production
# Always pass user-specific path: data/users/<username>/parsed_csv/
OUT_DIR = "data/parsed_csv"


def ensure_dir(path=None) -> str:
    """Ensure output directory exists
    
    IMPORTANT: Always pass user-specific directory path.
    Do NOT rely on OUT_DIR fallback.
    """
    if not path:
        path: str = OUT_DIR

    os.makedirs(path, exist_ok=True)
    return path


def normalize_cols(df):
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )
    return df


def parse_generic_table(table) -> pd.DataFrame:
    headers = []
    rows = []

    for i, tr in enumerate(table.find_all("tr")):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if not cols:
            continue

        if i == 0:
            headers = cols
        else:
            rows.append(cols)

    if not headers or not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=headers[: len(rows[0])])
    return normalize_cols(df)


def find_table_after_heading(soup, keywords):
    """Find table following a heading text"""
    if isinstance(keywords, str):
        keywords: list[str] = [keywords]

    for tag in soup.find_all(["p", "h1", "h2", "h3"]):
        text = tag.get_text(strip=True).lower()
        for k in keywords:
            if k.lower() in text:
                return tag.find_next("table")

    return None


# ------------------------------------------------------------------
# OLD SIMPLE DIRECT PARSER (Backward Compatible)
# ------------------------------------------------------------------
def main(html_file) -> None:
    out: str = ensure_dir()

    with open(html_file, "r", errors="ignore") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    # ---------- ACTIVITY OVER TIME ----------
    act_table: Tag | None = find_table_after_heading(
        soup,
        ["activity over time", "active sessions over time"]
    )
    if act_table:
        df: pd.DataFrame = parse_generic_table(act_table)
        if not df.empty:
            df.to_csv(f"{out}/ash_activity_over_time.csv", index=False)

    # ---------- ASH EVENTS ----------
    evt_table: Tag | None = find_table_after_heading(
        soup,
        ["top events", "ash events"]
    )
    if evt_table:
        df: pd.DataFrame = parse_generic_table(evt_table)
        if not df.empty:
            df.to_csv(f"{out}/ash_events.csv", index=False)

    # ---------- ASH FEATURES ----------
    feat_table: Tag | None = find_table_after_heading(
        soup,
        ["ash features", "features"]
    )
    if feat_table:
        df: pd.DataFrame = parse_generic_table(feat_table)
        if not df.empty:
            df.to_csv(f"{out}/ash_features.csv", index=False)

    print("✅ ASH parsed successfully (CSV generated)")


# ------------------------------------------------------------------
# NEW RECOMMENDED FUNCTION (SUPPORTS 3 ARGUMENTS)
# ------------------------------------------------------------------
def parse_ash_with_prefix(html_file, file_prefix, out_dir: Optional[str] = None):
    """
    html_file  -> ASH HTML path
    file_prefix -> filename prefix
    out_dir -> USER SPECIFIC parsed_csv folder
    """

    out: str = ensure_dir(out_dir)

    with open(html_file, "r", errors="ignore") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    generated_files = []

    # ---------- ACTIVITY OVER TIME ----------
    act_table: Tag | None = find_table_after_heading(
        soup,
        ["activity over time", "active sessions over time"]
    )
    if act_table:
        df: pd.DataFrame = parse_generic_table(act_table)
        if not df.empty:
            filename: str = f"ash_activity_over_time_{file_prefix}.csv"
            df.to_csv(f"{out}/{filename}", index=False)
            generated_files.append(filename)

    # ---------- ASH EVENTS ----------
    evt_table: Tag | None = find_table_after_heading(
        soup,
        ["top events", "ash events"]
    )
    if evt_table:
        df: pd.DataFrame = parse_generic_table(evt_table)
        if not df.empty:
            filename: str = f"ash_events_{file_prefix}.csv"
            df.to_csv(f"{out}/{filename}", index=False)
            generated_files.append(filename)

    # ---------- ASH FEATURES ----------
    feat_table: Tag | None = find_table_after_heading(
        soup,
        ["ash features", "features"]
    )
    if feat_table:
        df: pd.DataFrame = parse_generic_table(feat_table)
        if not df.empty:
            filename: str = f"ash_features_{file_prefix}.csv"
            df.to_csv(f"{out}/{filename}", index=False)
            generated_files.append(filename)

    print(
        f"✅ ASH parsed with prefix '{file_prefix}' "
        f"→ {len(generated_files)} CSV generated "
        f"→ saved in {out}"
    )

    return generated_files


if __name__ == "__main__":
    main(sys.argv[1])

