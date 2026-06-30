from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


DELTA_ROOT = Path("data/delta")
MAX_PARQUET_FILES = 40


@st.cache_data(ttl=15, show_spinner=False)
def read_delta_table(path: str, max_files: int = MAX_PARQUET_FILES) -> tuple[pd.DataFrame, int, int, list[str]]:
    table_path = Path(path)
    if not table_path.exists():
        return pd.DataFrame(), 0, 0, []

    parquet_files = [file for file in table_path.rglob("*.parquet") if "_delta_log" not in file.parts]
    total_files = len(parquet_files)

    if not parquet_files:
        return pd.DataFrame(), 0, 0, []

    def modified_at(file: Path) -> float:
        try:
            return file.stat().st_mtime
        except OSError:
            return 0

    newest_files = sorted(parquet_files, key=modified_at, reverse=True)[:max_files]
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    for file in newest_files:
        try:
            frames.append(pd.read_parquet(file))
        except Exception as exc:
            errors.append(f"{file.name}: {exc}")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return df, total_files, len(newest_files), errors[:3]


def load_table(label: str, path: Path) -> tuple[pd.DataFrame, int, int]:
    with st.spinner(f"Loading {label}"):
        df, total_files, loaded_files, errors = read_delta_table(str(path))

    if errors:
        st.warning(f"Skipped {len(errors)} recently written {label} file(s): {'; '.join(errors)}")

    return df, total_files, loaded_files


st.set_page_config(page_title="Stock Stream Monitor", layout="wide")
st.title("Real-Time Stock Stream Monitor")

aggregates, aggregate_files, aggregate_loaded_files = load_table("aggregate windows", DELTA_ROOT / "gold" / "tumbling_1m")
anomalies, anomaly_files, anomaly_loaded_files = load_table("anomalies", DELTA_ROOT / "gold" / "anomalies")
silver, silver_files, silver_loaded_files = load_table("valid trades", DELTA_ROOT / "silver" / "valid_trades")

metric_cols = st.columns(4)
metric_cols[0].metric("Valid Trades", f"{len(silver):,}")
metric_cols[1].metric("Anomalies", f"{len(anomalies):,}")
metric_cols[2].metric("Aggregate Windows", f"{len(aggregates):,}")
metric_cols[3].metric("Symbols", f"{silver['symbol'].nunique():,}" if not silver.empty else "0")
st.caption(
    "Showing newest "
    f"{silver_loaded_files}/{silver_files} valid trade, "
    f"{anomaly_loaded_files}/{anomaly_files} anomaly, and "
    f"{aggregate_loaded_files}/{aggregate_files} aggregate Parquet files."
)

if not aggregates.empty:
    aggregates["window_start"] = pd.to_datetime(aggregates["window_start"])
    st.subheader("1-Minute VWAP by Symbol")
    vwap_chart = (
        aggregates.pivot_table(
            index="window_start",
            columns="symbol",
            values="vwap",
            aggfunc="mean",
        )
        .sort_index()
    )
    st.line_chart(vwap_chart)

    st.subheader("Windowed Notional Volume")
    volume_chart = (
        aggregates.pivot_table(
            index="window_start",
            columns="symbol",
            values="total_notional",
            aggfunc="sum",
        )
        .sort_index()
    )
    st.bar_chart(volume_chart)

if not anomalies.empty:
    st.subheader("Latest Anomalies")
    display_cols = [
        "event_ts",
        "symbol",
        "side",
        "quantity",
        "price",
        "notional",
        "anomaly_reason",
        "severity",
    ]
    existing_cols = [col for col in display_cols if col in anomalies.columns]
    st.dataframe(anomalies[existing_cols].sort_values(existing_cols[0], ascending=False), use_container_width=True)
else:
    st.info("No anomalies written yet. Start Kafka, the producer, and the Spark job.")
