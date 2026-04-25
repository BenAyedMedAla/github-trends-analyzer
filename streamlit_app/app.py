import os
import socket
import time
from html import escape
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import happybase
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(page_title="GitHub Trends", page_icon="⚡", layout="wide")

HBASE_HOST = os.getenv("HBASE_HOST", "hadoop-master")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
DEFAULT_LIMIT = int(os.getenv("STREAMLIT_DEFAULT_LIMIT", "100"))
REFRESH_INTERVAL = int(os.getenv("STREAMLIT_REFRESH_SECONDS", "5"))


def get_connection() -> happybase.Connection:
    return happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=60000)


def is_retryable_hbase_error(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
            EOFError,
            OSError,
            socket.timeout,
        ),
    )


def decode_cell_map(raw: Dict[bytes, bytes]) -> Dict[str, str]:
    decoded: Dict[str, str] = {}
    for k, v in raw.items():
        key = k.decode("utf-8", errors="ignore")
        val = v.decode("utf-8", errors="ignore")
        decoded[key] = val
    return decoded


def scan_rows(
    table_name: str,
    limit: int,
    row_prefix: str = "",
    latest_first: bool = False,
) -> List[Tuple[str, Dict[str, str]]]:
    prefix_bytes = row_prefix.encode("utf-8") if row_prefix else None
    last_error: Exception | None = None

    def do_scan() -> List[Tuple[str, Dict[str, str]]]:
        conn = get_connection()
        try:
            table = conn.table(table_name)
            rows = table.scan(row_prefix=prefix_bytes)
            result: List[Tuple[str, Dict[str, str]]] = []
            for row_key, cells in rows:
                result.append(
                    (row_key.decode("utf-8", errors="ignore"), decode_cell_map(cells))
                )
            if latest_first:
                result.sort(key=lambda x: x[0], reverse=True)
            if limit > 0:
                result = result[:limit]
            return result
        finally:
            conn.close()

    for attempt in range(3):
        try:
            return do_scan()
        except Exception as exc:
            last_error = exc
            if not is_retryable_hbase_error(exc) or attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))

    if last_error is not None:
        raise last_error

    return []


def rows_to_dataframe(rows: List[Tuple[str, Dict[str, str]]]) -> pd.DataFrame:
    records: List[Dict[str, str]] = []
    for row_key, cells in rows:
        record: Dict[str, str] = {"row_key": row_key}
        record.update(cells)
        records.append(record)

    if not records:
        return pd.DataFrame(columns=["row_key"])

    return pd.DataFrame(records)


def get_live_metrics(limit: int = 20) -> pd.DataFrame:
    try:
        rows = scan_rows("live_metrics", limit=limit, latest_first=True)
    except Exception:
        return pd.DataFrame()
    return rows_to_dataframe(rows)


def get_live_events(limit: int = 30) -> pd.DataFrame:
    try:
        rows = scan_rows("live_events", limit=limit, latest_first=True)
    except Exception:
        return pd.DataFrame()
    return rows_to_dataframe(rows)


def get_weekly_metrics(limit: int = 200) -> pd.DataFrame:
    try:
        rows = scan_rows("weekly_metrics", limit=0, latest_first=True)
    except Exception:
        return pd.DataFrame()
    df = rows_to_dataframe(rows)
    if df.empty:
        return df

    if "row_key" in df.columns:
        df = df[df["row_key"].astype(str).str.contains("#", regex=False)]

    if limit > 0:
        df = df.head(limit)

    return df


def get_batch_day_column(df: pd.DataFrame) -> Optional[str]:
    if "stats:day" in df.columns:
        return "stats:day"
    if "stats:week" in df.columns:
        return "stats:week"
    return None


def get_ml_predictions(limit: int = 20) -> pd.DataFrame:
    try:
        rows = scan_rows("ml_predictions", limit=limit, latest_first=False)
    except Exception:
        return pd.DataFrame()
    return rows_to_dataframe(rows)


def trending_repos_df(limit: int = 8) -> pd.DataFrame:
    df = get_live_metrics(limit=limit)
    if df.empty:
        return pd.DataFrame(columns=["repo", "language", "stars"])

    if "repo:name" not in df.columns:
        return pd.DataFrame(columns=["repo", "language", "stars"])

    repo_stars: Dict[str, int] = {}
    repo_lang: Dict[str, str] = {}

    for _, row in df.iterrows():
        name = str(row.get("repo:name", ""))
        lang = str(row.get("repo:language", "Unknown"))
        try:
            stars = int(row.get("repo:stars", 0))
        except (ValueError, TypeError):
            stars = 0
        if name:
            repo_stars[name] = max(repo_stars.get(name, 0), stars)
            if name not in repo_lang:
                repo_lang[name] = lang

    sorted_repos = sorted(repo_stars.items(), key=lambda x: x[1], reverse=True)[:limit]
    records = [
        {"repo": name, "language": repo_lang.get(name, "Unknown"), "stars": stars}
        for name, stars in sorted_repos
    ]
    return pd.DataFrame(records)


def activity_feed_df(limit: int = 20) -> pd.DataFrame:
    df = get_live_events(limit=limit)
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "type", "repo"])

    records = []
    for _, row in df.iterrows():
        row_key = row.get("row_key", "")
        parts = row_key.split("#", 2)
        ts = parts[0] if len(parts) > 0 else ""
        try:
            from datetime import timezone, timedelta
            utc_dt = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            local = utc_dt + timedelta(hours=1)
            ts_disp = local.strftime("%H:%M:%S")
        except Exception:
            ts_disp = f"{ts[8:10]}:{ts[10:12]}:{ts[12:14]}" if len(ts) >= 14 else ts

        records.append({
            "timestamp": ts_disp,
            "type": row.get("event:type", "Event"),
            "repo": row.get("event:repo", ""),
        })

    return pd.DataFrame(records)


def language_stats_df() -> pd.DataFrame:
    df = get_weekly_metrics(limit=0)
    date_col = get_batch_day_column(df)
    required_cols = {"stats:stars", "stats:forks", "repo:language"}
    if df.empty or date_col is None or not required_cols.issubset(set(df.columns)):
        return pd.DataFrame(columns=["language", "thisWeek", "lastWeek"])

    def _safe(v):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    parsed_days = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_day"] = parsed_days
    df["_stars"] = df["stats:stars"].apply(_safe)
    df["_forks"] = df["stats:forks"].apply(_safe)
    df = df[df["_day"].notna()]

    if df.empty:
        return pd.DataFrame(columns=["language", "thisWeek", "lastWeek"])

    df["_language"] = df["repo:language"].apply(
        lambda v: str(v).strip() if str(v).strip() else "Unknown"
    )

    non_unknown_count = (df["_language"] != "Unknown").sum()
    if non_unknown_count > 0:
        df = df[df["_language"] != "Unknown"]

    days = sorted([d for d in df["_day"].unique() if d is not None])
    if not days:
        return pd.DataFrame(columns=["language", "thisWeek", "lastWeek"])

    current_day = days[-1]
    previous_day = days[-2] if len(days) >= 2 else None

    current = (
        df[df["_day"] == current_day]
        .groupby("_language", as_index=False)["_stars"]
        .sum()
        .rename(columns={"_language": "language", "_stars": "thisWeek"})
    )

    if previous_day is not None:
        previous = (
            df[df["_day"] == previous_day]
            .groupby("_language", as_index=False)["_stars"]
            .sum()
            .rename(columns={"_language": "language", "_stars": "lastWeek"})
        )
        result = current.merge(previous, on="language", how="left")
    else:
        result = current.copy()
        result["lastWeek"] = 0

    result["lastWeek"] = result["lastWeek"].fillna(0).astype(int)
    result["thisWeek"] = result["thisWeek"].fillna(0).astype(int)

    if not result.empty:
        result = result.sort_values("thisWeek", ascending=False).reset_index(drop=True)
    return result


def historical_df() -> pd.DataFrame:
    df = get_weekly_metrics(limit=0)
    date_col = get_batch_day_column(df)
    required_cols = {"stats:stars", "repo:language"}
    if df.empty or date_col is None or not required_cols.issubset(set(df.columns)):
        return pd.DataFrame(columns=["day"])

    def _safe(v):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    parsed_days = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_day"] = parsed_days
    df["_stars"] = df["stats:stars"].apply(_safe)
    df = df[df["_day"].notna()]

    if df.empty:
        return pd.DataFrame(columns=["day"])

    df["_language"] = df["repo:language"].apply(
        lambda v: str(v).strip() if str(v).strip() else "Unknown"
    )

    non_unknown_count = (df["_language"] != "Unknown").sum()
    if non_unknown_count > 0:
        df = df[df["_language"] != "Unknown"]

    lang_totals = (
        df.groupby("_language")["_stars"]
        .sum()
        .sort_values(ascending=False)
    )
    languages = lang_totals.head(5).index.tolist()

    if not languages:
        return pd.DataFrame(columns=["day"])

    records = []
    for day, grp in df.groupby("_day"):
        day_lang_totals = grp.groupby("_language")["_stars"].sum()
        record = {"day": day}
        for lang in languages:
            record[lang] = int(day_lang_totals.get(lang, 0))
        records.append(record)

    result = pd.DataFrame(records)
    if not result.empty:
        result = result.sort_values("day").reset_index(drop=True)
        result = result[["day", *languages]]
    return result


def trending_daily_df(limit: int = 10) -> pd.DataFrame:
    df = get_weekly_metrics(limit=2000)
    required_cols = {"stats:stars", "stats:forks"}
    if df.empty or not required_cols.issubset(set(df.columns)):
        return pd.DataFrame(columns=["repo", "language", "score", "stars", "forks"])

    def _safe(v):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    df["_stars"] = df["stats:stars"].apply(_safe)
    df["_forks"] = df["stats:forks"].apply(_safe)
    df["_score"] = df["stats:velocity"].apply(_safe) + df["stats:fork_velocity"].apply(_safe)
    top = df.nlargest(limit, "_score")
    records = []
    for _, row in top.iterrows():
        records.append({
            "repo": row.get("repo:name", row.get("row_key", "")),
            "language": row.get("repo:language", "Unknown"),
            "score": int(row["_score"]),
            "stars": int(row["_stars"]),
            "forks": int(row["_forks"]),
        })
    return pd.DataFrame(records)


def ai_insights_df(limit: int = 10) -> pd.DataFrame:
    df = get_ml_predictions(limit=limit)
    if df.empty:
        return pd.DataFrame(columns=["repo", "probability", "cluster", "predictedStars"])

    records = []
    for _, row in df.iterrows():
        prob = float(row.get("ml:probability", 0))
        records.append({
            "repo": row.get("repo:name", row.get("row_key", "")),
            "probability": prob,
            "cluster": row.get("ml:cluster", "Unknown"),
            "predictedStars": int(float(row.get("ml:predicted_growth", 0))),
        })

    return pd.DataFrame(records)


# ─── Design system ────────────────────────────────────────────────────────────

LANG_COLORS = {
    "Python":     "#3b82f6",
    "TypeScript": "#a78bfa",
    "JavaScript": "#fbbf24",
    "Go":         "#34d399",
    "Rust":       "#f97316",
    "Java":       "#f43f5e",
    "C++":        "#22d3ee",
    "C":          "#94a3b8",
    "Ruby":       "#fb7185",
    "Swift":      "#ff6b6b",
    "Kotlin":     "#c084fc",
    "PHP":        "#818cf8",
    "Unknown":    "#475569",
}

def lang_color(lang: str) -> str:
    return LANG_COLORS.get(lang, "#64748b")


CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Geist+Mono:wght@300;400;500;600;700&family=Geist:wght@300;400;500;600;700;800;900&display=swap');

*, *::before, *::after { box-sizing: border-box; }

/* ── App chrome ── */
[data-testid="stAppViewContainer"] {
    background: #050810;
    color: #e2e8f0;
    font-family: 'Geist', sans-serif;
}
[data-testid="stHeader"] { background: transparent; border: none; }
[data-testid="stMainBlockContainer"] { padding-top: 0 !important; }
[data-testid="stSidebar"] {
    background: #080d1a !important;
    border-right: 1px solid rgba(99, 102, 241, 0.12) !important;
}
[data-testid="stSidebar"] * { color: #94a3b8 !important; font-family: 'Geist Mono', monospace !important; font-size: 0.7rem !important; }
[data-testid="stSidebar"] .stSelectbox label { color: #64748b !important; font-size: 0.65rem !important; text-transform: uppercase; letter-spacing: 0.1em; }

/* ── Scrollbars ── */
::-webkit-scrollbar { width: 3px; height: 3px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(99,102,241,0.3); border-radius: 99px; }
* { scrollbar-width: thin; scrollbar-color: rgba(99,102,241,0.3) transparent; }

/* ── Animations ── */
@keyframes pulse-live {
    0%, 100% { box-shadow: 0 0 0 0 rgba(16,185,129,0.5); opacity: 1; }
    50% { box-shadow: 0 0 0 4px rgba(16,185,129,0); opacity: 0.6; }
}
@keyframes scan-line {
    0% { transform: translateY(-100%); opacity: 0; }
    10% { opacity: 1; }
    90% { opacity: 1; }
    100% { transform: translateY(500%); opacity: 0; }
}
@keyframes shimmer {
    0% { background-position: -200% center; }
    100% { background-position: 200% center; }
}
@keyframes fade-up {
    from { opacity: 0; transform: translateY(6px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes row-flash {
    0%   { background: rgba(16,185,129,0.18); }
    100% { background: transparent; }
}
@keyframes border-flow {
    0%, 100% { opacity: 0.4; }
    50% { opacity: 1; }
}

/* ── Site header ── */
.gh-header {
    padding: 1.1rem 1.75rem 1rem;
    border-bottom: 1px solid rgba(99,102,241,0.1);
    background: linear-gradient(180deg, rgba(99,102,241,0.04) 0%, transparent 100%);
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin-bottom: 1.25rem;
}
.gh-header-left {}
.gh-wordmark {
    font-family: 'Geist', sans-serif;
    font-size: 1.35rem;
    font-weight: 800;
    letter-spacing: -0.04em;
    color: #f1f5f9;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.gh-wordmark-icon {
    width: 26px;
    height: 26px;
    background: linear-gradient(135deg, #6366f1, #8b5cf6);
    border-radius: 7px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 0.85rem;
}
.gh-subtitle {
    font-size: 0.68rem;
    color: #475569;
    font-family: 'Geist Mono', monospace;
    margin-top: 3px;
    letter-spacing: 0.04em;
}
.gh-header-right {
    display: flex;
    align-items: center;
    gap: 1rem;
}
.gh-status-pill {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    font-family: 'Geist Mono', monospace;
    font-size: 0.65rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    padding: 5px 12px;
    border-radius: 99px;
    text-transform: uppercase;
}
.gh-status-pill.live {
    background: rgba(16,185,129,0.08);
    border: 1px solid rgba(16,185,129,0.25);
    color: #10b981;
}
.gh-status-pill.batch {
    background: rgba(99,102,241,0.08);
    border: 1px solid rgba(99,102,241,0.25);
    color: #818cf8;
}
.gh-status-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    flex-shrink: 0;
}
.gh-status-dot.live {
    background: #10b981;
    animation: pulse-live 2s ease-in-out infinite;
}
.gh-status-dot.batch {
    background: #818cf8;
}

/* ── Panel base ── */
.panel {
    border-radius: 12px;
    padding: 1.1rem 1.15rem 1rem;
    position: relative;
    overflow: hidden;
    animation: fade-up 0.35s ease both;
    height: 100%;
    min-height: 440px;
}
.panel-live {
    background: linear-gradient(145deg, #040b14 0%, #060d18 100%);
    border: 1px solid rgba(16,185,129,0.2);
    box-shadow: 0 0 0 1px rgba(16,185,129,0.04) inset, 0 20px 40px rgba(0,0,0,0.4);
}
.panel-live::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent 0%, #10b981 40%, #34d399 60%, transparent 100%);
    animation: border-flow 3s ease-in-out infinite;
}
.panel-live::after {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 100%; height: 2px;
    background: linear-gradient(90deg, transparent, rgba(16,185,129,0.6), transparent);
    animation: scan-line 6s ease-in-out infinite;
    pointer-events: none;
}
.panel-batch {
    background: linear-gradient(145deg, #06060f 0%, #08081a 100%);
    border: 1px solid rgba(99,102,241,0.18);
    box-shadow: 0 0 0 1px rgba(99,102,241,0.04) inset, 0 20px 40px rgba(0,0,0,0.4);
}
.panel-batch::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent 0%, #6366f1 50%, transparent 100%);
    opacity: 0.6;
}

/* ── Panel header ── */
.panel-hd {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.5rem;
    margin-bottom: 0.35rem;
}
.panel-title {
    font-size: 0.88rem;
    font-weight: 700;
    color: #f1f5f9;
    letter-spacing: -0.02em;
    margin: 0;
    font-family: 'Geist', sans-serif;
}
.panel-desc {
    font-size: 0.62rem;
    color: #334155;
    font-family: 'Geist Mono', monospace;
    letter-spacing: 0.02em;
    margin-bottom: 0.9rem;
}
.badge-live {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-family: 'Geist Mono', monospace;
    font-size: 0.6rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    color: #10b981;
    background: rgba(16,185,129,0.08);
    border: 1px solid rgba(16,185,129,0.2);
    padding: 3px 9px;
    border-radius: 99px;
    white-space: nowrap;
    flex-shrink: 0;
}
.badge-live-dot {
    width: 5px; height: 5px;
    border-radius: 50%;
    background: #10b981;
    animation: pulse-live 1.8s ease-in-out infinite;
    flex-shrink: 0;
}
.badge-batch {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-family: 'Geist Mono', monospace;
    font-size: 0.6rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    color: #818cf8;
    background: rgba(99,102,241,0.08);
    border: 1px solid rgba(99,102,241,0.2);
    padding: 3px 9px;
    border-radius: 99px;
    white-space: nowrap;
    flex-shrink: 0;
}
.badge-ml {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-family: 'Geist Mono', monospace;
    font-size: 0.6rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    color: #c084fc;
    background: rgba(192,132,252,0.08);
    border: 1px solid rgba(192,132,252,0.2);
    padding: 3px 9px;
    border-radius: 99px;
    white-space: nowrap;
    flex-shrink: 0;
}

/* ── Scroll containers ── */
.scroll-box {
    max-height: 340px;
    overflow-y: auto;
    padding-right: 2px;
}

/* ── Empty state ── */
.empty-state {
    padding: 2rem 1rem;
    text-align: center;
    color: #334155;
    font-family: 'Geist Mono', monospace;
    font-size: 0.72rem;
}
.empty-state-icon { font-size: 1.5rem; margin-bottom: 0.5rem; opacity: 0.4; }

/* ── Trending repos list ── */
.repo-row {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 7px 8px;
    border-radius: 8px;
    transition: background 0.15s;
    cursor: default;
}
.repo-row:hover { background: rgba(255,255,255,0.03); }
.repo-rank {
    font-family: 'Geist Mono', monospace;
    font-size: 0.62rem;
    color: #1e293b;
    width: 18px;
    text-align: right;
    flex-shrink: 0;
    font-weight: 600;
}
.repo-name {
    flex: 1;
    font-size: 0.8rem;
    font-weight: 500;
    color: #e2e8f0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: 'Geist', sans-serif;
}
.repo-lang-dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
}
.repo-lang-label {
    font-size: 0.62rem;
    color: #475569;
    font-family: 'Geist Mono', monospace;
    width: 72px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.repo-stars {
    font-family: 'Geist Mono', monospace;
    font-size: 0.72rem;
    font-weight: 600;
    color: #10b981;
    flex-shrink: 0;
}
.repo-row-new { animation: row-flash 1.4s ease-out; }

/* ── Activity feed ── */
.feed-row {
    display: flex;
    align-items: center;
    gap: 0.65rem;
    padding: 5px 8px;
    border-radius: 6px;
    font-family: 'Geist Mono', monospace;
    font-size: 0.66rem;
    color: #e2e8f0;
    transition: background 0.12s;
}
.feed-row:hover { background: rgba(255,255,255,0.025); }
.feed-ts { color: #1e293b; width: 62px; flex-shrink: 0; font-weight: 500; }
.feed-type-watch {
    color: #fbbf24;
    background: rgba(251,191,36,0.07);
    border: 1px solid rgba(251,191,36,0.12);
    padding: 1px 7px;
    border-radius: 4px;
    flex-shrink: 0;
    width: 88px;
    text-align: center;
}
.feed-type-fork {
    color: #818cf8;
    background: rgba(129,140,248,0.07);
    border: 1px solid rgba(129,140,248,0.12);
    padding: 1px 7px;
    border-radius: 4px;
    flex-shrink: 0;
    width: 88px;
    text-align: center;
}
.feed-repo {
    flex: 1;
    color: #94a3b8;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

/* ── Rising languages bars ── */
.lang-bar-row {
    display: grid;
    grid-template-columns: 78px 1fr;
    align-items: center;
    gap: 0.65rem;
    margin-bottom: 0.5rem;
}
.lang-bar-label {
    font-family: 'Geist Mono', monospace;
    font-size: 0.7rem;
    color: #94a3b8;
    text-align: right;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.lang-bar-track {
    position: relative;
    height: 24px;
}
.lang-bar-bg {
    position: absolute;
    top: 0; left: 0;
    width: 100%;
    height: 8px;
    background: rgba(255,255,255,0.03);
    border-radius: 4px;
}
.lang-bar-last {
    position: absolute;
    top: 0; left: 0;
    height: 8px;
    border-radius: 4px;
    background: rgba(148, 163, 184, 0.5);
    min-width: 3px;
    transition: width 0.6s cubic-bezier(.4,0,.2,1);
}
.lang-bar-this {
    position: absolute;
    top: 12px; left: 0;
    height: 8px;
    border-radius: 4px;
    background: rgba(34, 211, 238, 0.8);
    min-width: 3px;
    transition: width 0.6s cubic-bezier(.4,0,.2,1);
}
.lang-bar-tooltip {
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    min-width: 160px;
    background: #0d1117;
    border: 1px solid rgba(99,102,241,0.2);
    border-radius: 8px;
    padding: 8px 12px;
    box-shadow: 0 12px 32px rgba(0,0,0,0.5);
    display: none;
    z-index: 30;
    pointer-events: none;
    font-family: 'Geist Mono', monospace;
    font-size: 0.72rem;
}
.lang-bar-track:hover .lang-bar-tooltip { display: block; }
.lang-bar-tooltip .ttlang { color: #f1f5f9; font-weight: 700; margin-bottom: 6px; font-size: 0.8rem; }
.lang-bar-tooltip .ttlast { color: #6366f1; margin-bottom: 2px; }
.lang-bar-tooltip .ttthis { color: #10b981; }
.lang-legend {
    display: flex;
    gap: 1.2rem;
    margin-top: 0.6rem;
    padding-left: 78px;
    font-family: 'Geist Mono', monospace;
    font-size: 0.62rem;
    color: #475569;
}
.lang-legend-item { display: flex; align-items: center; gap: 5px; }
.lang-legend-swatch { width: 14px; height: 6px; border-radius: 2px; }

/* ── Trending daily ── */
.daily-row {
    padding: 6px 8px 10px;
    margin-bottom: 2px;
}
.daily-row-top {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin-bottom: 5px;
}
.daily-rank {
    font-family: 'Geist Mono', monospace;
    font-size: 0.62rem;
    color: #1e293b;
    width: 18px;
    text-align: right;
    flex-shrink: 0;
    font-weight: 600;
}
.daily-name {
    flex: 1;
    font-size: 0.8rem;
    font-weight: 500;
    color: #e2e8f0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.daily-meta {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-family: 'Geist Mono', monospace;
    font-size: 0.63rem;
}
.daily-stars { color: #10b981; }
.daily-forks { color: #818cf8; }
.daily-bar {
    height: 2px;
    background: rgba(255,255,255,0.04);
    border-radius: 1px;
    overflow: hidden;
    margin-left: 26px;
}
.daily-bar-fill {
    height: 100%;
    border-radius: 1px;
    background: linear-gradient(90deg, #6366f1, #8b5cf6, #a78bfa);
    transition: width 0.5s ease;
}

/* ── AI insights ── */
.insight-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 10px 10px;
    border-radius: 8px;
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.03);
    transition: border-color 0.15s, background 0.15s;
    margin-bottom: 6px;
}
.insight-row:hover {
    background: rgba(255,255,255,0.035);
    border-color: rgba(192,132,252,0.12);
}
.insight-name {
    flex: 1;
    font-weight: 600;
    font-size: 0.78rem;
    color: #e2e8f0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: 'Geist', sans-serif;
}
.insight-cluster {
    font-family: 'Geist Mono', monospace;
    font-size: 0.6rem;
    color: #475569;
    width: 110px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.insight-prob-bar {
    width: 60px;
    height: 4px;
    background: rgba(255,255,255,0.06);
    border-radius: 2px;
    overflow: hidden;
}
.insight-prob-fill {
    height: 100%;
    border-radius: 2px;
    background: linear-gradient(90deg, #10b981, #34d399);
}
.insight-prob-label {
    font-family: 'Geist Mono', monospace;
    font-size: 0.72rem;
    font-weight: 700;
    color: #10b981;
    width: 36px;
    text-align: right;
}
.insight-stars {
    font-family: 'Geist Mono', monospace;
    font-size: 0.62rem;
    color: #334155;
    width: 58px;
    text-align: right;
}

/* ── Divider ── */
.section-gap { height: 1rem; }

/* ── Disable Streamlit rerun overlay ── */
[data-stale="true"] { opacity: 1 !important; }
[data-testid="stStatusWidget"] { display: none !important; }
</style>
"""


def render_header():
    st.markdown(CSS, unsafe_allow_html=True)
    st.markdown(
        """
        <div class="gh-header">
          <div class="gh-header-left">
            <div class="gh-wordmark">
              <span class="gh-wordmark-icon">⚡</span>
              GitHub Trends
            </div>
            <div class="gh-subtitle">real-time kafka stream · daily pyspark batch · hbase storage</div>
          </div>
          <div class="gh-header-right">
            <span class="gh-status-pill live">
              <span class="gh-status-dot live"></span>Stream live
            </span>
            <span class="gh-status-pill batch">
              <span class="gh-status-dot batch"></span>Batch daily
            </span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_trending_now(df: pd.DataFrame):
    prev: set = set(st.session_state.get("prev_trending", []))
    curr: set = set(df["repo"].tolist() if not df.empty else [])
    new_repos: set = curr - prev
    st.session_state["prev_trending"] = list(curr)

    if df.empty:
        body = '<div class="empty-state"><div class="empty-state-icon">📡</div>Waiting for stream data…</div>'
    else:
        rows = ""
        for i, row in df.iterrows():
            name = row.get("repo", "")
            lang = row.get("language", "Unknown")
            stars = row.get("stars", 0)
            color = lang_color(lang)
            flash = " repo-row-new" if name in new_repos else ""
            rows += f"""
            <div class="repo-row{flash}">
                <span class="repo-rank">{i+1}</span>
                <span class="repo-name">{escape(name)}</span>
                <span class="repo-lang-dot" style="background:{color};"></span>
                <span class="repo-lang-label">{escape(lang)}</span>
                <span class="repo-stars">★ {stars:,}</span>
            </div>"""
        body = f'<div class="scroll-box">{rows}</div>'

    st.html(f"""
    <div class="panel panel-live">
        <div class="panel-hd">
            <h3 class="panel-title">Trending Now</h3>
            <span class="badge-live"><span class="badge-live-dot"></span>LIVE</span>
        </div>
        <div class="panel-desc">TOP REPOS BY STAR COUNT · CURRENT WINDOW</div>
        {body}
    </div>""")


def render_activity_feed(df: pd.DataFrame):
    if df.empty:
        body = '<div class="empty-state"><div class="empty-state-icon">📡</div>No events yet — waiting for stream…</div>'
    else:
        rows = ""
        for _, row in df.iterrows():
            ts = escape(row.get("timestamp", ""))
            ev = row.get("type", "Event")
            rp = escape(row.get("repo", ""))
            if ev == "WatchEvent":
                type_html = f'<span class="feed-type-watch">★ Watch</span>'
            else:
                type_html = f'<span class="feed-type-fork">⑂ Fork</span>'
            rows += f"""
            <div class="feed-row">
                <span class="feed-ts">{ts}</span>
                {type_html}
                <span class="feed-repo">{rp}</span>
            </div>"""
        body = f'<div class="scroll-box">{rows}</div>'

    st.html(f"""
    <div class="panel panel-live">
        <div class="panel-hd">
            <h3 class="panel-title">Live Activity Feed</h3>
            <span class="badge-live"><span class="badge-live-dot"></span>LIVE</span>
        </div>
        <div class="panel-desc">RAW KAFKA EVENTS · AS THEY ARRIVE</div>
        {body}
    </div>""")


def render_rising_languages(df: pd.DataFrame):
    if df.empty or (
        len(df) > 0 and set(df["language"].astype(str).str.strip().tolist()) == {"Unknown"}
    ):
        msg = "No daily batch data yet." if df.empty else "Language enrichment in progress…"
        body = f'<div class="empty-state"><div class="empty-state-icon">📊</div>{msg}</div>'
        st.html(f"""
        <div class="panel panel-batch">
            <div class="panel-hd">
                <h3 class="panel-title">Rising Languages</h3>
                <span class="badge-batch">BATCH</span>
            </div>
            <div class="panel-desc">DAILY STAR COUNTS BY LANGUAGE</div>
            {body}
        </div>""")
        return

    chart_df = df.head(10).copy()
    chart_df["thisWeek"] = pd.to_numeric(chart_df["thisWeek"], errors="coerce").fillna(0)
    chart_df["lastWeek"] = pd.to_numeric(chart_df["lastWeek"], errors="coerce").fillna(0)
    max_val = max(int(chart_df[["thisWeek","lastWeek"]].values.max()), 1)

    rows = ""
    for _, row in chart_df.iterrows():
        lang = str(row.get("language", "Unknown"))
        tw = max(int(row.get("thisWeek", 0)), 0)
        lw = max(int(row.get("lastWeek", 0)), 0)
        tw_pct = max((tw / max_val) * 100, 1.5)
        lw_pct = max((lw / max_val) * 100, 1.5)
        rows += f"""
        <div class="lang-bar-row">
            <div class="lang-bar-label">{escape(lang)}</div>
            <div class="lang-bar-track">
                <div class="lang-bar-bg"></div>
                <div class="lang-bar-last" style="width:{lw_pct:.2f}%;"></div>
                <div class="lang-bar-this" style="width:{tw_pct:.2f}%;"></div>
                <div class="lang-bar-tooltip">
                    <div class="ttlang">{escape(lang)}</div>
                    <div class="ttlast">Yesterday: {lw:,}</div>
                    <div class="ttthis">Today: {tw:,}</div>
                </div>
            </div>
        </div>"""

    st.html(f"""
    <div class="panel panel-batch">
        <div class="panel-hd">
            <h3 class="panel-title">Rising Languages</h3>
            <span class="badge-batch">BATCH</span>
        </div>
        <div class="panel-desc">DAILY STAR ACTIVITY BY LANGUAGE · HOVER FOR DETAILS</div>
        {rows}
        <div class="lang-legend">
            <span class="lang-legend-item"><span class="lang-legend-swatch" style="background:rgba(148, 163, 184, 0.5);"></span>Yesterday</span>
            <span class="lang-legend-item"><span class="lang-legend-swatch" style="background:rgba(34, 211, 238, 0.8);"></span>Today</span>
        </div>
    </div>""")


def render_historical_trends(df: pd.DataFrame):
    EMPTY_HIST = "LANGUAGE STARS OVER TIME · DAILY PYSPARK BATCHES"
    FULL_HIST  = "LANGUAGE STARS OVER TIME · GROUPED BY DAY · REFRESHED DAILY"

    # Target the named container directly - reliable across Streamlit versions
    st.markdown("""
    <style>
    .st-key-hist_box {
        background: linear-gradient(145deg,#06060f,#08081a) !important;
        border: 1px solid rgba(99,102,241,0.18) !important;
        border-radius: 12px !important;
        margin-top: -0.85rem !important;
        padding: 1.5rem 1.6rem !important;
        box-shadow: 0 0 0 1px rgba(99,102,241,0.04) inset, 0 20px 40px rgba(0,0,0,0.4) !important;
    }
    .st-key-hist_box [data-testid="stPlotlyChart"] {
        margin-top: 1rem;
        padding: 0.5rem 0;
    }
    </style>""", unsafe_allow_html=True)

    def render_header(desc: str) -> None:
        st.html(f"""
        <div class="panel-hd">
            <h3 class="panel-title">Historical Trends</h3>
            <span class="badge-batch">BATCH</span>
        </div>
        <div class="panel-desc">{desc}</div>""")

    with st.container(key="hist_box"):
        if df.empty or len(df.columns) <= 1:
            render_header(EMPTY_HIST)
            st.html('<div class="empty-state"><div class="empty-state-icon">📈</div>No historical data yet — run the batch job first.</div>')
            return

        if len(df.columns) == 2 and "Unknown" in df.columns:
            render_header(EMPTY_HIST)
            st.html('<div class="empty-state"><div class="empty-state-icon">🏷️</div>Language labels unresolved. Run another batch after enrichment.</div>')
            return

        render_header(FULL_HIST)

        chart_df = df.copy()
        # String dates → category axis → exactly one tick per day, no duplicates
        chart_df["day"] = pd.to_datetime(chart_df["day"], errors="coerce").dt.strftime("%d %b")
        chart_df = chart_df.dropna(subset=["day"]).sort_values("day")
        languages = [c for c in chart_df.columns if c != "day"]

        palette = ["#10b981", "#818cf8", "#fbbf24", "#f43f5e", "#22d3ee", "#c084fc", "#34d399"]

        fig = go.Figure()
        for i, lang in enumerate(languages):
            color = palette[i % len(palette)]
            series = pd.to_numeric(chart_df[lang], errors="coerce").fillna(0)
            r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
            fig.add_trace(go.Scatter(
                x=chart_df["day"], y=series,
                name=lang,
                mode="lines",
                line={"color": color, "width": 2.5, "shape": "spline", "smoothing": 0.6},
                fill="tozeroy",
                fillcolor=f"rgba({r},{g},{b},0.04)",
                hovertemplate=f"<b>{lang}</b>: %{{y:,.0f}}<extra></extra>",
            ))

        fig.update_layout(
            height=310,
            margin={"l": 70, "r": 70, "t": 16, "b": 50},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            hoverlabel={"bgcolor": "#0d1117", "bordercolor": "#1e293b",
                        "font": {"color": "#e2e8f0", "size": 12, "family": "Geist Mono"}},
            legend={"orientation": "h", "yanchor": "top", "y": -0.08,
                    "xanchor": "center", "x": 0.5,
                    "font": {"color": "#64748b", "size": 11, "family": "Geist Mono"},
                    "bgcolor": "rgba(0,0,0,0)"},
            xaxis={"type": "category"},
        )
        fig.update_xaxes(
            showgrid=False,
            tickfont={"color": "#475569", "size": 10, "family": "Geist Mono"},
            showline=False, zeroline=False,
            tickmode="auto", nticks=8,
        )
        fig.update_yaxes(
            showgrid=True, gridcolor="rgba(255,255,255,0.03)", gridwidth=1,
            tickfont={"color": "#475569", "size": 10, "family": "Geist Mono"},
            showline=False, zeroline=False, tickformat=",.0f",
        )

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_trending_daily(df: pd.DataFrame):
    if df.empty:
        body = '<div class="empty-state"><div class="empty-state-icon">📦</div>No daily batch data yet — run the batch job first.</div>'
    else:
        max_score = max(df["score"].max(), 1)
        rows = ""
        for i, row in df.iterrows():
            name = escape(row.get("repo", ""))
            lang = row.get("language", "Unknown")
            stars = row.get("stars", 0)
            forks = row.get("forks", 0)
            score = row.get("score", 0)
            pct = max(int(score) / max_score * 100, 2)
            color = lang_color(lang)
            rows += f"""
            <div class="daily-row">
                <div class="daily-row-top">
                    <span class="daily-rank">{i+1}</span>
                    <span class="daily-name">{name}</span>
                    <span class="daily-meta">
                        <span class="daily-stars">★ {int(stars):,}</span>
                        <span style="color:#1e293b;">·</span>
                        <span class="daily-forks">⑂ {int(forks):,}</span>
                        <span style="width:6px;height:6px;border-radius:50%;background:{color};margin-left:4px;display:inline-block;"></span>
                    </span>
                </div>
                <div class="daily-bar">
                    <div class="daily-bar-fill" style="width:{pct:.1f}%;"></div>
                </div>
            </div>"""
        body = f'<div class="scroll-box">{rows}</div>'

    st.html(f"""
    <div class="panel panel-batch">
        <div class="panel-hd">
            <h3 class="panel-title">Trending Yesterday</h3>
            <span class="badge-batch">BATCH</span>
        </div>
        <div class="panel-desc">TOP REPOS BY VELOCITY SCORE · LATEST DAILY BATCH</div>
        {body}
    </div>""")


def render_ai_insights(df: pd.DataFrame):
    if df.empty:
        body = '<div class="empty-state"><div class="empty-state-icon">🤖</div>No ML predictions yet — run the batch job first.</div>'
    else:
        rows = ""
        for _, row in df.iterrows():
            repo = escape(row.get("repo", ""))
            prob = row.get("probability", 0)
            prob_pct = int(prob * 100)
            cluster = escape(str(row.get("cluster", "")))
            stars = int(row.get("predictedStars", 0))
            rows += f"""
            <div class="insight-row">
                <span class="insight-name">{repo}</span>
                <span class="insight-cluster">{cluster}</span>
                <div style="display:flex;flex-direction:column;align-items:flex-end;gap:3px;">
                    <div class="insight-prob-bar">
                        <div class="insight-prob-fill" style="width:{prob_pct}%;"></div>
                    </div>
                    <span class="insight-prob-label">{prob_pct}%</span>
                </div>
                <span class="insight-stars">+{stars:,} ★</span>
            </div>"""
        body = rows

    st.html(f"""
    <div class="panel panel-batch">
        <div class="panel-hd">
            <h3 class="panel-title">AI Insights</h3>
            <span class="badge-ml">BATCH · ML</span>
        </div>
        <div class="panel-desc">TRENDING PROBABILITY · ML CLUSTER · PREDICTED GROWTH</div>
        {body}
    </div>""")


def main():
    st.title("")

    refresh = st.sidebar.selectbox(
        "Refresh rate",
        [5, 10, 30, 60],
        index=0,
        format_func=lambda x: f"{x}s",
    )

    with st.sidebar:
        st.caption(f"Host: {HBASE_HOST}:{HBASE_PORT}")
        st.caption("Tables: live_events · live_metrics · repos · weekly_metrics · ml_predictions")
        st.caption("Orchestration: Airflow DAG (HDFS source)")

    render_header()

    col1, col2 = st.columns(2)
    with col1:
        render_trending_now(trending_repos_df(limit=8))
    with col2:
        render_rising_languages(language_stats_df())

    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

    col3, col4 = st.columns(2)
    with col3:
        render_activity_feed(activity_feed_df(limit=20))
    with col4:
        render_historical_trends(historical_df())

    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

    col5, col6 = st.columns(2)
    with col5:
        render_trending_daily(trending_daily_df(limit=10))
    with col6:
        render_ai_insights(ai_insights_df(limit=5))

    time.sleep(refresh)
    st.rerun()


if __name__ == "__main__":
    main()