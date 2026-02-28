import sys
import os
import pandas as pd
from typing import Optional, List
from bs4 import BeautifulSoup

# ⚠️ WARNING: This fallback should NEVER be used in production
# Always pass user-specific path: data/users/<username>/parsed_csv/
OUT_DIR = "data/parsed_csv"


def ensure_dir(path: Optional[str] = None) -> str:
    if not path:
        path = OUT_DIR

    os.makedirs(path, exist_ok=True)
    return path


def normalize_cols(df):
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("%", "pct")
        .str.replace("(", "")
        .str.replace(")", "")
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
    if isinstance(keywords, str):
        keywords_list = [keywords]
    else:
        keywords_list = keywords

    for tag in soup.find_all(["p", "h1", "h2", "h3"]):
        text = tag.get_text(strip=True).lower()

        for k in keywords_list:
            if k.lower() in text:
                return tag.find_next("table")

    return None


# ------------------------------------------------------------------
# LOAD PROFILE
# ------------------------------------------------------------------
def parse_load_profile(soup) -> Optional[pd.DataFrame]:
    rows = []

    for tag in soup.find_all(["p", "h2", "h3"]):
        if "load profile" in tag.get_text(strip=True).lower():
            table = tag.find_next("table")
            if not table:
                return None

            for tr in table.find_all("tr"):
                cols = [c.get_text(strip=True) for c in tr.find_all("td")]
                if len(cols) == 3:
                    rows.append({
                        "metric": cols[0],
                        "per_second": cols[1],
                        "per_transaction": cols[2]
                    })

            return pd.DataFrame(rows)

    return None


# ------------------------------------------------------------------
# METADATA
# ------------------------------------------------------------------
def parse_metadata(soup) -> Optional[pd.DataFrame]:
    meta = {}

    for b in soup.find_all("b"):
        txt = b.get_text(strip=True)

        if ":" in txt:
            k, v = txt.split(":", 1)
            meta[k.strip()] = v.strip()

    if meta:
        return pd.DataFrame([meta])

    return None


# ------------------------------------------------------------------
# OLD STYLE DIRECT PARSER
# ------------------------------------------------------------------
def main(html_file) -> None:
    out = ensure_dir()

    with open(html_file, "r", errors="ignore") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    _run_parse_pipeline(soup, out, "awr")


# ------------------------------------------------------------------
# NEW API
# ------------------------------------------------------------------
def parse_awr_with_prefix(html_file, file_prefix, out_dir=None):
    out = ensure_dir(out_dir)

    print("📖 Reading AWR file:", html_file)
    with open(html_file, "r", errors="ignore") as f:
        content = f.read()
        soup = BeautifulSoup(content, "html.parser")

    print("🔍 Parsing AWR HTML with BeautifulSoup...")
    generated_files = _run_parse_pipeline(soup, out, file_prefix)

    required_prefixes = [
        "awr_sql_stats",
        "awr_instance_stats",
        "awr_wait_events"
    ]

    missing = []
    for req in required_prefixes:
        if not any(req in f for f in generated_files):
            missing.append(req)

    if missing:
        raise Exception(
            "AWR parsing incomplete - missing required CSVs: {}. Generated only: {}"
            .format(missing, generated_files)
        )

    print(
        "✅ AWR parsed with prefix '{}' → {} CSV generated → saved in {}"
        .format(file_prefix, len(generated_files), out)
    )

    return generated_files


# ------------------------------------------------------------------
# INTERNAL PIPELINE
# ------------------------------------------------------------------
def _run_parse_pipeline(soup, out, prefix):
    generated_files = []

    # ---------- METADATA ----------
    print("  📋 Extracting metadata...")
    meta_df = parse_metadata(soup)
    if meta_df is not None:
        filename = "awr_metadata_{}.csv".format(prefix)
        meta_df.to_csv("{}/{}".format(out, filename), index=False)
        generated_files.append(filename)
        print("    ✅ Metadata extracted")
    else:
        print("    ⚠️ No metadata found")

    # ---------- SQL STATS ----------
    print("  📊 Searching for SQL statistics table...")
    sql_table = find_table_after_heading(
        soup,
        ["sql ordered by elapsed time", "sql ordered by cpu time", "sql statistics"]
    )

    if sql_table:
        print("    ✅ SQL stats table found, parsing...")
        df = parse_generic_table(sql_table)
        if not df.empty:
            filename = "awr_sql_stats_{}.csv".format(prefix)
            df.to_csv("{}/{}".format(out, filename), index=False)
            generated_files.append(filename)
            print("    ✅ SQL stats CSV generated:", len(df), "rows")
        else:
            print("    ⚠️ SQL stats table was empty")
    else:
        print("    ❌ SQL stats table NOT FOUND")

    # ---------- WAIT EVENTS ----------
    print("  ⏱️ Searching for wait events table...")
    wait_table = find_table_after_heading(
        soup,
        [
            "top timed events",
            "foreground wait events",
            "wait events",
            "top foreground events",
            "top 10 foreground events"
        ]
    )

    if wait_table:
        print("    ✅ Wait events table found, parsing...")
        df = parse_generic_table(wait_table)
        if not df.empty:
            filename = "awr_wait_events_{}.csv".format(prefix)
            df.to_csv("{}/{}".format(out, filename), index=False)
            generated_files.append(filename)
            print("    ✅ Wait events CSV generated:", len(df), "rows")
        else:
            print("    ⚠️ Wait events table was empty")
    else:
        print("    ❌ Wait events table NOT FOUND")

    # ---------- LOAD PROFILE ----------
    lp_df = parse_load_profile(soup)
    if lp_df is not None and not lp_df.empty:
        filename = "awr_load_profile_{}.csv".format(prefix)
        lp_df.to_csv("{}/{}".format(out, filename), index=False)
        generated_files.append(filename)

    # ---------- INSTANCE STATS ----------
    print("  📈 Searching for instance activity stats...")
    inst_table = find_table_after_heading(
        soup,
        ["instance activity stats", "instance activity statistics", "instance activity"]
    )

    if inst_table:
        print("    ✅ Instance stats table found, parsing...")
        df = parse_generic_table(inst_table)
        if not df.empty:
            filename = "awr_instance_stats_{}.csv".format(prefix)
            df.to_csv("{}/{}".format(out, filename), index=False)
            generated_files.append(filename)
            print("    ✅ Instance stats CSV generated:", len(df), "rows")
        else:
            print("    ⚠️ Instance stats table was empty")
    else:
        print("    ❌ Instance stats table NOT FOUND")

    print("\n📦 Total CSV files generated:", len(generated_files))
    return generated_files


if __name__ == "__main__":
    main(sys.argv[1])

