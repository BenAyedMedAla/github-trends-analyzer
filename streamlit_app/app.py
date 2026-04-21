import os
import socket
import time
from typing import Dict, List, Tuple

import happybase
import pandas as pd
import streamlit as st

HBASE_HOST = os.getenv("HBASE_HOST", "hadoop-master")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
DEFAULT_LIMIT = int(os.getenv("STREAMLIT_DEFAULT_LIMIT", "50"))


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
            rows = table.scan(row_prefix=prefix_bytes, limit=limit, reverse=latest_first)

            result: List[Tuple[str, Dict[str, str]]] = []
            for row_key, cells in rows:
                result.append((row_key.decode("utf-8", errors="ignore"), decode_cell_map(cells)))
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


def main() -> None:
    st.set_page_config(page_title="HBase Explorer", page_icon="data", layout="wide")
    st.title("OpenTrend HBase Explorer")
    st.caption("Inspect live and batch tables directly from HBase.")

    with st.sidebar:
        st.subheader("Connection")
        st.write(f"Host: {HBASE_HOST}")
        st.write(f"Port: {HBASE_PORT}")

        st.subheader("Query")
        table_name = st.selectbox(
            "Table",
            ["live_metrics", "repos", "weekly_metrics", "ml_predictions"],
            index=0,
        )
        limit = st.slider("Rows", min_value=10, max_value=200, value=DEFAULT_LIMIT, step=10)
        latest_first = st.checkbox("Latest rows first", value=True)
        row_prefix = st.text_input("Row key prefix", value="")

        run = st.button("Load rows", type="primary")

    if not run:
        st.info("Choose options in the sidebar, then click Load rows.")
        return

    try:
        rows = scan_rows(
            table_name=table_name,
            limit=limit,
            row_prefix=row_prefix,
            latest_first=latest_first,
        )
    except Exception as exc:
        st.error(f"Failed to read HBase: {exc}")
        return

    st.success(f"Loaded {len(rows)} rows from {table_name}")

    df = rows_to_dataframe(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Quick stats")
    col1, col2 = st.columns(2)
    col1.metric("Rows loaded", len(rows))
    col2.metric("Columns found", max(len(df.columns) - 1, 0))

    if not df.empty:
        st.subheader("Columns")
        st.write(", ".join([c for c in df.columns if c != "row_key"]))


if __name__ == "__main__":
    main()
