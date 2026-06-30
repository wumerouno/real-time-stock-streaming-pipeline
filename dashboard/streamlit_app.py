from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


DELTA_ROOT = Path("data/delta")


@st.cache_data(ttl=15)
def read_delta_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        from deltalake import DeltaTable

        return DeltaTable(str(path)).to_pandas()
    except Exception as exc:
        st.warning(f"Could not read Delta table at {path}: {exc}")
        return pd.DataFrame()


st.set_page_config(page_title="Stock Stream Monitor", layout="wide")
st.title("Real-Time Stock Stream Monitor")

aggregates = read_delta_table(DELTA_ROOT / "gold" / "tumbling_1m")
anomalies = read_delta_table(DELTA_ROOT / "gold" / "anomalies")
silver = read_delta_table(DELTA_ROOT / "silver" / "valid_trades")

metric_cols = st.columns(4)
metric_cols[0].metric("Valid Trades", f"{len(silver):,}")
metric_cols[1].metric("Anomalies", f"{len(anomalies):,}")
metric_cols[2].metric("Aggregate Windows", f"{len(aggregates):,}")
metric_cols[3].metric("Symbols", f"{silver['symbol'].nunique():,}" if not silver.empty else "0")

if not aggregates.empty:
    aggregates["window_start"] = pd.to_datetime(aggregates["window_start"])
    st.subheader("1-Minute VWAP by Symbol")
    st.plotly_chart(
        px.line(
            aggregates.sort_values("window_start"),
            x="window_start",
            y="vwap",
            color="symbol",
            markers=True,
        ),
        use_container_width=True,
    )

    st.subheader("Windowed Notional Volume")
    st.plotly_chart(
        px.bar(
            aggregates.sort_values("window_start"),
            x="window_start",
            y="total_notional",
            color="symbol",
        ),
        use_container_width=True,
    )

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

