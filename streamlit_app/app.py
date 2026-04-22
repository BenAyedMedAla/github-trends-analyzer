import os
import socket
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import happybase
import pandas as pd
import streamlit as st

st.set_page_config(page_title="GitHub Trends Analyzer", page_icon="data", layout="wide")

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
            # Some HBase/Thrift combinations return empty results when the "reverse"
            # scan flag is provided. Use a forward scan for compatibility and sort
            # by row key in memory when latest-first ordering is requested.
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
        # Read all rows first so helper snapshot rows do not consume the caller limit.
        rows = scan_rows("weekly_metrics", limit=0, latest_first=True)
    except Exception:
        return pd.DataFrame()
    df = rows_to_dataframe(rows)
    if df.empty:
        return df

    # Ignore helper rows used for "last week" snapshots.
    # Weekly analytics panels expect rows with the standard week#repo key.
    if "row_key" in df.columns:
        df = df[df["row_key"].astype(str).str.contains("#", regex=False)]

    if limit > 0:
        df = df.head(limit)

    return df


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
    # Use full weekly dataset so all batch panels are consistent.
    df = get_weekly_metrics(limit=0)
    required_cols = {"stats:week", "stats:stars", "stats:forks", "repo:language"}
    if df.empty or not required_cols.issubset(set(df.columns)):
        return pd.DataFrame(columns=["language", "thisWeek", "lastWeek"])

    def _safe(v):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    df["_week"] = df["stats:week"].apply(_safe)
    df["_stars"] = df["stats:stars"].apply(_safe)
    df["_forks"] = df["stats:forks"].apply(_safe)

    # Normalize language labels so empty values are still visible in charts.
    df["_language"] = df["repo:language"].apply(
        lambda v: str(v).strip() if str(v).strip() else "Unknown"
    )

    # Keep Unknown only when it is the only available language bucket.
    non_unknown_count = (df["_language"] != "Unknown").sum()
    if non_unknown_count > 0:
        df = df[df["_language"] != "Unknown"]

    weeks = sorted([w for w in df["_week"].unique() if w is not None])
    if not weeks:
        return pd.DataFrame(columns=["language", "thisWeek", "lastWeek"])

    current_week = weeks[-1]
    previous_week = weeks[-2] if len(weeks) >= 2 else None

    current = (
        df[df["_week"] == current_week]
        .groupby("_language", as_index=False)["_stars"]
        .sum()
        .rename(columns={"_language": "language", "_stars": "thisWeek"})
    )

    if previous_week is not None:
        previous = (
            df[df["_week"] == previous_week]
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
    # Use same complete dataset as language_stats_df for coherent charts.
    df = get_weekly_metrics(limit=0)
    required_cols = {"stats:week", "stats:stars", "repo:language"}
    if df.empty or not required_cols.issubset(set(df.columns)):
        return pd.DataFrame(columns=["day"])

    def _safe(v):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    df["_week"] = df["stats:week"].apply(_safe)
    df["_stars"] = df["stats:stars"].apply(_safe)

    df["_day"] = df["stats:week"].apply(
        lambda x: str(x)[:10] if len(str(x)) >= 10 else "?"
    )

    df["_language"] = df["repo:language"].apply(
        lambda v: str(v).strip() if str(v).strip() else "Unknown"
    )

    # Keep Unknown only when no concrete language labels are available.
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


def trending_weekly_df(limit: int = 10) -> pd.DataFrame:
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


def render_live_badge(streaming: bool = True) -> str:
    if streaming:
        return "LIVE"
    return "BATCH"


CSS = """
<style>
    @import url("https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap");

    [data-testid="stAppViewContainer"] { background: #0d1117; color: #e6edf3; }
    [data-testid="stHeader"] { background: #0d1117; border-bottom: 1px solid #21262d; }
    [data-testid="stMainBlockContainer"] { padding-top: 1rem; }

    .panel-stream {
        background: #161b22;
        border: 2px solid #238636;
        border-radius: 8px;
        padding: 1rem;
        position: relative;
        box-shadow: 0 0 12px rgba(35, 134, 54, 0.15);
    }
    .panel-stream::before {
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, transparent, #238636, transparent);
        animation: pulse-bar 2s ease-in-out infinite;
    }
    .panel-batch {
        background: #161b22;
        border: 1px solid #8957e5;
        border-radius: 8px;
        padding: 1rem;
    }
    .panel-live-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        font-size: 0.7rem;
        font-weight: 600;
        font-family: "JetBrains Mono", monospace;
        background: rgba(35, 134, 54, 0.15);
        color: #238636;
        padding: 2px 10px;
        border-radius: 20px;
        border: 1px solid rgba(35, 134, 54, 0.3);
    }
    .panel-live-badge::before {
        content: "";
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: #238636;
        animation: pulse-dot 1.5s ease-in-out infinite;
    }
    .panel-batch-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        font-size: 0.7rem;
        font-weight: 600;
        font-family: "JetBrains Mono", monospace;
        background: rgba(137, 87, 229, 0.15);
        color: #8957e5;
        padding: 2px 10px;
        border-radius: 20px;
        border: 1px solid rgba(137, 87, 229, 0.3);
    }
    .panel-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 0.75rem;
    }
    .panel-title {
        font-size: 1rem;
        font-weight: 700;
        color: #e6edf3;
        font-family: "Inter", sans-serif;
    }
    .panel-desc {
        font-size: 0.7rem;
        color: #7d8590;
        margin-bottom: 0.75rem;
    }
    .feed-row {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 6px 8px;
        border-radius: 6px;
        font-family: "JetBrains Mono", monospace;
        font-size: 0.72rem;
        color: #e6edf3;
        transition: background 0.15s;
    }
    .feed-row:hover { background: rgba(110, 118, 129, 0.12); }
    .feed-container {
        max-height: 340px;
        overflow-y: auto;
        scrollbar-width: thin;
        scrollbar-color: #30363d #0d1117;
    }
    .feed-container::-webkit-scrollbar { width: 4px; }
    .feed-container::-webkit-scrollbar-track { background: #0d1117; }
    .feed-container::-webkit-scrollbar-thumb { background: #30363d; border-radius: 4px; }
    .feed-row .ts { color: #7d8590; width: 70px; flex-shrink: 0; }
    .feed-row .ev { width: 100px; flex-shrink: 0; color: #79c0ff; }
    .feed-row .rp { flex: 1; color: #e6edf3; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .tr-row {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 8px 12px;
        border-radius: 6px;
        font-size: 0.82rem;
        transition: all 0.2s;
    }
    .tr-row:hover { background: rgba(110, 118, 129, 0.1); }
    .tr-row.flash-row { animation: flash-green 1.2s ease-out; }
    @keyframes flash-green {
        0%   { background: rgba(35, 134, 54, 0.4); }
        100% { background: transparent; }
    }
    .tr-rank { color: #7d8590; font-family: "JetBrains Mono", monospace; width: 20px; text-align: right; }
    .tr-name { flex: 1; font-weight: 500; color: #e6edf3; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .tr-lang { font-size: 0.7rem; color: #7d8590; background: rgba(110, 118, 129, 0.15); padding: 2px 8px; border-radius: 6px; }
    .tr-stars { font-family: "JetBrains Mono", monospace; color: #238636; font-size: 0.82rem; }
    .tr-container { max-height: 340px; overflow-y: auto; scrollbar-width: thin; scrollbar-color: #30363d #161b22; }
    .tr-container::-webkit-scrollbar { width: 4px; }
    .tr-container::-webkit-scrollbar-track { background: #161b22; }
    .tr-container::-webkit-scrollbar-thumb { background: #30363d; border-radius: 4px; }
    .insight-row {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 10px 14px;
        border-radius: 8px;
        background: rgba(110, 118, 129, 0.08);
    }
    .insight-row .repo { flex: 1; min-width: 0; }
    .insight-row .repo-name { font-weight: 600; font-size: 0.85rem; color: #e6edf3; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .insight-row .cluster { font-size: 0.72rem; color: #7d8590; }
    .insight-row .prob { font-weight: 700; font-size: 0.9rem; color: #238636; }
    .insight-row .stars-next { font-size: 0.72rem; color: #7d8590; }
    .site-header {
        border-bottom: 1px solid #21262d;
        padding: 0.75rem 1.5rem;
        background: #0d1117;
    }
    .site-title { font-size: 1.2rem; font-weight: 800; color: #e6edf3; }
    .site-subtitle { font-size: 0.7rem; color: #7d8590; margin-top: 2px; }
    .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; }
    .status-dot-live { background: #238636; animation: pulse-dot 1.5s ease-in-out infinite; }
    .status-dot-batch { background: #8957e5; }
    @keyframes pulse-dot {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.3; }
    }
    @keyframes pulse-bar {
        0% { opacity: 0.3; }
        50% { opacity: 1; }
        100% { opacity: 0.3; }
    }
    [data-testid="stHorizontalBlock"] > div { gap: 1rem; }
    .stHorizontalBlock [data-testid="stVerticalBlock"] { padding: 0 !important; }
</style>
"""


def render_header():
    st.markdown(CSS, unsafe_allow_html=True)
    st.markdown(
        """
        <div class="site-header">
            <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:0.5rem;">
                <div>
                    <div class="site-title">GitHub Trends Analyzer</div>
                    <div class="site-subtitle">Real-time streaming + weekly batch pipeline dashboard</div>
                </div>
                <div style="display:flex;align-items:center;gap:1.5rem;">
                    <span style="display:flex;align-items:center;gap:6px;font-size:0.72rem;font-weight:600;color:#e6edf3;">
                        Stream: <span class="status-dot status-dot-live"></span> LIVE
                    </span>
                    <span style="display:flex;align-items:center;gap:6px;font-size:0.72rem;font-weight:600;color:#e6edf3;">
                        Batch: <span class="status-dot status-dot-batch"></span> Last run weekly
                    </span>
                </div>
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

    rows_html = ""
    if df.empty:
        rows_html = '<div style="padding:8px 12px;color:#7d8590;font-size:0.82rem;">No live metrics yet — waiting for stream...</div>'
    else:
        for i, row in df.iterrows():
            rank = i + 1
            name = row.get("repo", "")
            lang = row.get("language", "Unknown")
            stars = row.get("stars", 0)
            is_new = name in new_repos
            flash = "animation:flash-green 1.2s ease-out;" if is_new else ""
            rows_html += f"""
            <div style="{flash}display:flex;align-items:center;gap:0.75rem;padding:8px 12px;border-radius:6px;transition:all 0.2s;font-size:0.82rem;">
                <span style="color:#7d8590;font-family:monospace;width:20px;text-align:right;">{rank}</span>
                <span style="flex:1;font-weight:500;color:#e6edf3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{name}</span>
                <span style="font-size:0.7rem;color:#7d8590;background:rgba(110,118,129,0.15);padding:2px 8px;border-radius:6px;">{lang}</span>
                <span style="font-family:monospace;color:#238636;">&#11088; {stars}</span>
            </div>"""

    panel_html = f"""
    <div style="background:#161b22;border:2px solid #238636;border-radius:8px;padding:1rem;position:relative;box-shadow:0 0 12px rgba(35,134,54,0.15);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
            <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Trending Now</h3>
            <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(35,134,54,0.15);color:#238636;padding:2px 10px;border-radius:20px;border:1px solid rgba(35,134,54,0.3);">
                <span style="width:6px;height:6px;border-radius:50%;background:#238636;animation:pulse-dot 1.5s ease-in-out infinite;"></span>
                LIVE
            </span>
        </div>
        <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Top repos by star count in last window</div>
        <div style="max-height:340px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:#30363d #161b22;">
            {rows_html}
        </div>
    </div>"""
    st.html(panel_html)


def render_activity_feed(df: pd.DataFrame):
    rows_html = ""
    if df.empty:
        rows_html = '<div style="padding:8px 12px;color:#7d8590;font-size:0.72rem;">No events yet — waiting for stream...</div>'
    else:
        for _, row in df.iterrows():
            ts = row.get("timestamp", "")
            ev = row.get("type", "Event")
            rp = row.get("repo", "")
            display_ev = "&#11088; WatchEvent" if ev == "WatchEvent" else "&#127794; ForkEvent"
            rows_html += f"""
            <div style="display:flex;align-items:center;gap:0.75rem;padding:6px 8px;border-radius:6px;font-family:monospace;font-size:0.72rem;color:#e6edf3;transition:background 0.15s;">
                <span style="color:#7d8590;width:70px;flex-shrink:0;">{ts}</span>
                <span style="width:100px;flex-shrink:0;color:#79c0ff;">{display_ev}</span>
                <span style="flex:1;color:#e6edf3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{rp}</span>
            </div>"""

    panel_html = f"""
    <div style="background:#161b22;border:2px solid #238636;border-radius:8px;padding:1rem;position:relative;box-shadow:0 0 12px rgba(35,134,54,0.15);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
            <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Live Activity Feed</h3>
            <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(35,134,54,0.15);color:#238636;padding:2px 10px;border-radius:20px;border:1px solid rgba(35,134,54,0.3);">
                <span style="width:6px;height:6px;border-radius:50%;background:#238636;animation:pulse-dot 1.5s ease-in-out infinite;"></span>
                LIVE
            </span>
        </div>
        <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Raw Kafka events — as they arrive</div>
        <div style="max-height:340px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:#30363d #161b22;">
            {rows_html}
        </div>
    </div>"""
    st.html(panel_html)


def render_rising_languages(df: pd.DataFrame):
    if df.empty:
        panel_html = """
        <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
                <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Rising Languages</h3>
                <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
            </div>
            <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Stars per language this week</div>
            <div style="padding:16px 12px;color:#7d8590;font-size:0.82rem;">No weekly data yet — run the batch job first.</div>
        </div>"""
        st.html(panel_html)
        return

    # When enrichment has not resolved languages yet, avoid rendering a misleading chart.
    if set(df["language"].astype(str).str.strip().tolist()) == {"Unknown"}:
        panel_html = """
        <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
                <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Rising Languages</h3>
                <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
            </div>
            <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Stars per language this week</div>
            <div style="padding:16px 12px;color:#7d8590;font-size:0.82rem;">Language enrichment is still in progress. Weekly stars are available, but language labels are not resolved yet.</div>
        </div>"""
        st.html(panel_html)
        return

    chart_df = df.head(8).copy().set_index("language")[["thisWeek", "lastWeek"]]

    panel_html = f"""
    <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
            <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Rising Languages</h3>
            <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
        </div>
        <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Stars per language this week</div>
    </div>"""
    st.html(panel_html)
    st.bar_chart(chart_df, use_container_width=True)


def render_historical_trends(df: pd.DataFrame):
    if df.empty or len(df.columns) <= 1:
        panel_html = """
        <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
                <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Historical Trends</h3>
                <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
            </div>
            <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Language stars over time — computed by PySpark from GH Archive</div>
            <div style="padding:16px 12px;color:#7d8590;font-size:0.82rem;">No historical data yet — run the batch job first.</div>
        </div>"""
        st.html(panel_html)
        return

    only_unknown = len(df.columns) == 2 and "Unknown" in df.columns
    if only_unknown:
        panel_html = """
        <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
                <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Historical Trends</h3>
                <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
            </div>
            <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Language stars over time — computed by PySpark from GH Archive</div>
            <div style="padding:16px 12px;color:#7d8590;font-size:0.82rem;">Historical stars are available, but language labels are unresolved (Unknown only). Run one more batch after enrichment to see per-language history.</div>
        </div>"""
        st.html(panel_html)
        return

    languages = [c for c in df.columns if c != "day"]
    chart_df = df.copy().set_index("day")[languages]

    panel_html = f"""
    <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
            <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Historical Trends</h3>
            <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
        </div>
        <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Language stars over time — grouped by day from the latest batch</div>
    </div>"""
    st.html(panel_html)
    st.line_chart(chart_df, use_container_width=True)


def render_trending_weekly(df: pd.DataFrame):
    if df.empty:
        panel_html = """
        <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
                <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Trending This Week</h3>
                <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
            </div>
            <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Top repos by stars + forks this week</div>
            <div style="padding:16px 12px;color:#7d8590;font-size:0.82rem;">No weekly data yet — run the batch job first.</div>
        </div>"""
        st.html(panel_html)
        return

    rows_html = ""
    for i, row in df.iterrows():
        rank = i + 1
        name = row.get("repo", "")
        lang = row.get("language", "Unknown")
        score = row.get("score", 0)
        stars = row.get("stars", 0)
        forks = row.get("forks", 0)
        pct = max(int(score) / max(df["score"].max(), 1) * 100, 2)
        rows_html += f"""
        <div style="display:flex;align-items:center;gap:0.75rem;padding:8px 12px;border-radius:6px;transition:all 0.2s;font-size:0.82rem;">
            <span style="color:#7d8590;font-family:monospace;width:20px;text-align:right;">{rank}</span>
            <span style="flex:1;font-weight:500;color:#e6edf3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{name}</span>
            <span style="font-size:0.7rem;color:#7d8590;background:rgba(110,118,129,0.15);padding:2px 8px;border-radius:6px;">{lang}</span>
            <span style="font-family:monospace;color:#8957e5;width:60px;text-align:right;">&#11088;{int(stars):,} &#127794;{int(forks):,}</span>
        </div>
        <div style="height:3px;background:#21262d;border-radius:2px;overflow:hidden;">
            <div style="width:{pct:.1f}%;height:100%;background:linear-gradient(90deg,#8957e5,#bc8cff);border-radius:2px;"></div>
        </div>"""

    panel_html = f"""
    <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
            <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">Trending This Week</h3>
            <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH</span>
        </div>
        <div style="font-size:0.7rem;color:#7d8590;margin-bottom:0.75rem;">Top repos by stars + forks this week</div>
        <div style="max-height:340px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:#30363d #161b22;display:flex;flex-direction:column;gap:4px;">
            {rows_html}
        </div>
    </div>"""
    st.html(panel_html)


def render_ai_insights(df: pd.DataFrame):
    if df.empty:
        panel_html = """
        <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
                <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">AI Insights</h3>
                <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH + ML</span>
            </div>
            <div style="padding:16px 12px;color:#7d8590;font-size:0.82rem;">No ML predictions yet — run the batch job first.</div>
        </div>"""
        st.html(panel_html)
        return

    rows_html = ""
    for _, row in df.iterrows():
        repo = row.get("repo", "")
        prob = int(row.get("probability", 0) * 100)
        cluster = row.get("cluster", "")
        stars = row.get("predictedStars", 0)
        rows_html += f"""
        <div style="display:flex;align-items:center;gap:1rem;padding:10px 14px;border-radius:8px;background:rgba(110,118,129,0.08);">
            <span style="flex:1;font-weight:600;font-size:0.85rem;color:#e6edf3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{repo}</span>
            <span style="font-size:0.72rem;color:#7d8590;width:120px;">{cluster}</span>
            <span style="font-weight:700;font-size:0.9rem;color:#238636;width:50px;text-align:right;">{prob}%</span>
            <span style="font-size:0.72rem;color:#7d8590;width:60px;text-align:right;">+{int(stars):,} &#11088;</span>
        </div>"""

    panel_html = f"""
    <div style="background:#161b22;border:2px solid #8957e5;border-radius:8px;padding:1rem;box-shadow:0 0 10px rgba(137,87,229,0.12);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
            <h3 style="font-size:1rem;font-weight:700;color:#e6edf3;margin:0;">AI Insights</h3>
            <span style="display:inline-flex;align-items:center;gap:6px;font-size:0.7rem;font-weight:600;font-family:monospace;background:rgba(137,87,229,0.15);color:#8957e5;padding:2px 10px;border-radius:20px;border:1px solid rgba(137,87,229,0.3);">BATCH + ML</span>
        </div>
        <div style="max-height:280px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:#30363d #161b22;display:flex;flex-direction:column;gap:6px;">
            {rows_html}
        </div>
    </div>"""
    st.html(panel_html)


def main():
    st.title("")

    refresh = st.sidebar.selectbox(
        "Refresh rate",
        [5, 10, 30, 60],
        index=0,
        format_func=lambda x: f"{x}s",
    )

    with st.sidebar:
        st.caption(f"Connected to: {HBASE_HOST}:{HBASE_PORT}")
        st.caption("Tables: live_events, live_metrics, repos, weekly_metrics, ml_predictions")
        st.caption("Batch orchestration: Airflow DAG (HDFS source)")

    render_header()

    col1, col2 = st.columns(2)

    with col1:
        trending = trending_repos_df(limit=8)
        render_trending_now(trending)

    with col2:
        lang_stats = language_stats_df()
        render_rising_languages(lang_stats)

    col3, col4 = st.columns(2)

    with col3:
        feed = activity_feed_df(limit=20)
        render_activity_feed(feed)

    with col4:
        hist = historical_df()
        render_historical_trends(hist)

    col5, col6 = st.columns(2)

    with col5:
        weekly = trending_weekly_df(limit=10)
        render_trending_weekly(weekly)

    with col6:
        insights = ai_insights_df(limit=5)
        render_ai_insights(insights)

    time.sleep(refresh)
    st.rerun()


if __name__ == "__main__":
    main()