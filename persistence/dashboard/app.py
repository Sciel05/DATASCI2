import os
import sys
import time
from datetime import datetime, timezone

# Add project root to sys.path so `persistence` package is importable
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from persistence.alerting.config import CONFIG, AlertConfig
from persistence.alerting.alert_engine import AlertEngine
from persistence.persistence_client import get_persistence_client

st.set_page_config(
    page_title="Market Surveillance Desk",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom high-contrast financial terminal styling (Antislop UI)
st.markdown("""
<style>
    /* Dark terminal theme styling */
    .stApp {
        background-color: #0b0f19;
        color: #e2e8f0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
    }
    
    /* Top metric containers */
    div[data-testid="stMetric"] {
        background-color: #131b2e;
        border: 1px solid #1e293b;
        border-radius: 4px;
        padding: 10px 16px;
    }
    div[data-testid="stMetric"] label {
        color: #94a3b8;
        font-size: 0.8rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #f8fafc;
        font-size: 1.5rem;
        font-weight: 600;
    }

    /* Banners */
    .critical-banner {
        background-color: rgba(239, 68, 68, 0.12);
        border: 1px solid #ef4444;
        border-left: 5px solid #ef4444;
        border-radius: 4px;
        padding: 12px 18px;
        margin-bottom: 16px;
        color: #fca5a5;
    }
    .warning-banner {
        background-color: rgba(245, 158, 11, 0.12);
        border: 1px solid #f59e0b;
        border-left: 5px solid #f59e0b;
        border-radius: 4px;
        padding: 12px 18px;
        margin-bottom: 16px;
        color: #fde68a;
    }
    .calm-banner {
        background-color: rgba(16, 185, 129, 0.08);
        border: 1px solid #10b981;
        border-left: 5px solid #10b981;
        border-radius: 4px;
        padding: 10px 16px;
        margin-bottom: 16px;
        color: #6ee7b7;
        font-size: 0.9rem;
    }

    /* Severity badges */
    .badge-critical {
        background-color: #ef4444;
        color: #ffffff;
        padding: 2px 8px;
        border-radius: 3px;
        font-weight: 700;
        font-size: 0.75rem;
    }
    .badge-warning {
        background-color: #f59e0b;
        color: #000000;
        padding: 2px 8px;
        border-radius: 3px;
        font-weight: 700;
        font-size: 0.75rem;
    }
    .badge-info {
        background-color: #0284c7;
        color: #ffffff;
        padding: 2px 8px;
        border-radius: 3px;
        font-weight: 600;
        font-size: 0.75rem;
    }

    /* Clean table styling */
    .dataframe {
        font-size: 0.85rem !important;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar configuration & controls
st.sidebar.markdown("### SURVEILLANCE CONTROLS")

runtime_mode = st.sidebar.radio(
    "Data Source Backend",
    options=["Mock Simulator (CSV-anchored)", "Live Cassandra"],
    index=0 if CONFIG.use_mock else 1,
    help="Toggle between offline ground-truth replay and live Apache Cassandra database.",
)
is_live = runtime_mode == "Live Cassandra"

lookback_minutes = st.sidebar.selectbox(
    "Lookback Window",
    options=[5, 15, 30, 60],
    index=1,
    format_func=lambda x: f"Trailing {x} minutes",
)

auto_refresh = st.sidebar.checkbox("Auto-refresh Feed", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (sec)", min_value=2, max_value=15, value=5)

st.sidebar.markdown("---")
st.sidebar.markdown("### FILTER ANOMALIES")

selected_ticker = st.sidebar.selectbox(
    "Asset Ticker",
    options=["ALL"] + list(CONFIG.monitored_tickers),
    index=0,
)

selected_type = st.sidebar.selectbox(
    "Anomaly Pattern",
    options=["ALL", "price_shock", "wash_trade"],
    index=0,
)

selected_severity = st.sidebar.selectbox(
    "Severity Tier",
    options=["ALL", "CRITICAL", "WARNING", "INFO"],
    index=0,
)

min_zscore = st.sidebar.slider(
    "Min |Z-Score| Threshold",
    min_value=0.0,
    max_value=6.0,
    value=0.0,
    step=0.5,
)

st.sidebar.markdown("---")
st.sidebar.markdown("### THRESHOLD CALIBRATION")
crit_z = st.sidebar.slider(
    "Critical |Z-Score| Cutoff",
    min_value=1.5,
    max_value=6.0,
    value=float(CONFIG.zscore_critical),
    step=0.25,
    help="Z-Score deviation that triggers CRITICAL severity.",
)
warn_z = st.sidebar.slider(
    "Warning |Z-Score| Cutoff",
    min_value=1.0,
    max_value=4.0,
    value=float(CONFIG.zscore_warning),
    step=0.25,
    help="Z-Score deviation that triggers WARNING severity.",
)

# Instantiate client and engine with calibrated thresholds
cfg = AlertConfig(
    use_mock=not is_live,
    zscore_critical=crit_z,
    zscore_warning=warn_z,
)
try:
    client = get_persistence_client(config=cfg)
    health = client.check_health()
except Exception as e:
    st.error(f"Persistence connection error: {e}")
    st.stop()

engine = AlertEngine(config=cfg)

# Top Bar Header
col_title, col_stat = st.columns([3, 2])
with col_title:
    st.markdown("## Real-Time Market Surveillance Desk")
    st.caption("Statistical anomaly detection & trade abuse monitoring across Kafka · Spark · Cassandra")

with col_stat:
    health_color = "#10b981" if health["status"] == "healthy" else "#ef4444"
    st.markdown(
        f"""
        <div style="text-align: right; padding-top: 8px;">
            <span style="font-size: 0.85rem; color: #94a3b8;">Backend:</span>
            <strong style="color: {health_color}; margin-right: 12px;">{health['backend']}</strong>
            <span style="font-size: 0.85rem; color: #94a3b8;">Latency:</span>
            <strong style="color: #f8fafc;">{health.get('latency_ms', 0):.2f} ms</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("---")

# Query anomalies from client
query_ticker = None if selected_ticker == "ALL" else selected_ticker
df_anomalies = client.fetch_recent_anomalies(
    lookback_minutes=lookback_minutes,
    ticker=query_ticker,
    limit=500,
)

# Apply severity classification
if not df_anomalies.empty:
    df_anomalies["severity"] = df_anomalies.apply(
        lambda r: engine.classify_severity(r["zscore"], r["vwap_divergence"], r["anomaly_type"]),
        axis=1,
    )
    # Filter by user selections
    if selected_type != "ALL":
        df_anomalies = df_anomalies[df_anomalies["anomaly_type"] == selected_type]
    if selected_severity != "ALL":
        df_anomalies = df_anomalies[df_anomalies["severity"] == selected_severity]
    if min_zscore > 0.0:
        df_anomalies = df_anomalies[df_anomalies["zscore"].abs() >= min_zscore]

# Top KPI Cards
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
total_flags = len(df_anomalies)
critical_flags = len(df_anomalies[df_anomalies["severity"] == "CRITICAL"]) if not df_anomalies.empty else 0
warning_flags = len(df_anomalies[df_anomalies["severity"] == "WARNING"]) if not df_anomalies.empty else 0
price_shocks = len(df_anomalies[df_anomalies["anomaly_type"] == "price_shock"]) if not df_anomalies.empty else 0
wash_trades = len(df_anomalies[df_anomalies["anomaly_type"] == "wash_trade"]) if not df_anomalies.empty else 0
max_z = df_anomalies["zscore"].abs().max() if not df_anomalies.empty else 0.0

kpi1.metric("Total Anomaly Flags", f"{total_flags}")
kpi2.metric("Critical Incidents", f"{critical_flags}")
kpi3.metric("Price Shocks", f"{price_shocks}")
kpi4.metric("Wash Trades", f"{wash_trades}")
kpi5.metric("Peak |Z-Score|", f"{max_z:.2f}")

# Active Alert Banner (Critical, Warning, or Normal status)
if critical_flags > 0:
    recent_crit = df_anomalies[df_anomalies["severity"] == "CRITICAL"].iloc[0]
    st.markdown(
        f"""
        <div class="critical-banner">
            <strong>🔴 CRITICAL MARKET INCIDENT DETECTED</strong>: 
            <strong>{recent_crit['ticker']}</strong> flagged for <em>{recent_crit['anomaly_type']}</em> 
            at execution price <strong>${recent_crit['price']:.2f}</strong> with peak Z-Score <strong>{recent_crit['zscore']:+.2f}</strong> 
            (VWAP Divergence: <strong>{recent_crit['vwap_divergence'] * 100:.2f}%</strong>) at {recent_crit['event_time'].strftime('%H:%M:%S UTC')}.
        </div>
        """,
        unsafe_allow_html=True,
    )
elif warning_flags > 0:
    recent_warn = df_anomalies[df_anomalies["severity"] == "WARNING"].iloc[0]
    st.markdown(
        f"""
        <div class="warning-banner">
            <strong>🟡 ELEVATED SURVEILLANCE WARNING</strong>: 
            <strong>{recent_warn['ticker']}</strong> flagged for <em>{recent_warn['anomaly_type']}</em> 
            at execution price <strong>${recent_warn['price']:.2f}</strong> with Z-Score <strong>{recent_warn['zscore']:+.2f}</strong> 
            (VWAP Divergence: <strong>{recent_warn['vwap_divergence'] * 100:.2f}%</strong>) at {recent_warn['event_time'].strftime('%H:%M:%S UTC')}.
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f"""
        <div class="calm-banner">
            ✓ <strong>NORMAL SURVEILLANCE STATUS</strong>: Zero critical statistical anomalies or wash trade alerts in trailing {lookback_minutes}m window.
        </div>
        """,
        unsafe_allow_html=True,
    )

# Section: Market Overview Matrix
st.markdown("#### ASSET HEALTH & SURVEILLANCE OVERVIEW")
metrics = client.fetch_ticker_metrics(lookback_minutes=lookback_minutes)
matrix_rows = []
for sym in CONFIG.monitored_tickers:
    m = metrics.get(sym, {})
    cnt = m.get("anomaly_count", 0)
    shocks = m.get("price_shocks", 0)
    washes = m.get("wash_trades", 0)
    peak_z = m.get("max_zscore", 0.0)

    if peak_z >= cfg.zscore_critical:
        status_label = "🔴 CRITICAL"
    elif peak_z >= cfg.zscore_warning or washes > 0:
        status_label = "🟡 WARNING"
    else:
        status_label = "🟢 NORMAL"

    matrix_rows.append({
        "Ticker": sym,
        "Status": status_label,
        "Total Anomalies": cnt,
        "Price Shocks": shocks,
        "Wash Trades": washes,
        "Peak |Z-Score|": f"{peak_z:.2f}",
        "Latest Event": m.get("latest_event_time").strftime("%H:%M:%S") if m.get("latest_event_time") else "—",
    })

matrix_df = pd.DataFrame(matrix_rows)
st.dataframe(matrix_df, use_container_width=True, hide_index=True)

# Section: Interactive Timeline Co-Plot
st.markdown("#### REAL-TIME ANOMALY TIMELINE CO-PLOT")

if df_anomalies.empty:
    st.info("No anomalies detected matching the current filter criteria in this lookback window.")
else:
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("Execution Price & Anomaly Flags", "Rolling Z-Score Deviation"),
        row_heights=[0.65, 0.35],
    )

    color_map = {
        "CRITICAL": "#ef4444",
        "WARNING": "#f59e0b",
        "INFO": "#38bdf8",
    }

    # Plot 1: Price series with anomaly scatter markers
    for sym, grp in df_anomalies.groupby("ticker"):
        grp_sorted = grp.sort_values("event_time")
        fig.add_trace(
            go.Scatter(
                x=grp_sorted["event_time"],
                y=grp_sorted["price"],
                mode="lines+markers",
                name=f"{sym} Price",
                line=dict(width=1.5),
                marker=dict(
                    size=8,
                    color=[color_map.get(s, "#38bdf8") for s in grp_sorted["severity"]],
                    symbol="circle",
                ),
                hovertemplate="<b>%{fullData.name}</b><br>Time: %{x}<br>Price: $%{y:.2f}<extra></extra>",
            ),
            row=1, col=1,
        )

    # Plot 2: Z-score timeline
    for sym, grp in df_anomalies.groupby("ticker"):
        grp_sorted = grp.sort_values("event_time")
        fig.add_trace(
            go.Scatter(
                x=grp_sorted["event_time"],
                y=grp_sorted["zscore"],
                mode="markers",
                name=f"{sym} Z-Score",
                marker=dict(
                    size=7,
                    color=[color_map.get(s, "#38bdf8") for s in grp_sorted["severity"]],
                ),
                hovertemplate="Time: %{x}<br>Z-Score: %{y:.2f}<extra></extra>",
                showlegend=False,
            ),
            row=2, col=1,
        )

    # Threshold horizontal guides
    fig.add_hline(y=cfg.zscore_warning, line_dash="dash", line_color="#f59e0b", line_width=1, row=2, col=1)
    fig.add_hline(y=-cfg.zscore_warning, line_dash="dash", line_color="#f59e0b", line_width=1, row=2, col=1)
    fig.add_hline(y=cfg.zscore_critical, line_dash="solid", line_color="#ef4444", line_width=1.2, row=2, col=1)
    fig.add_hline(y=-cfg.zscore_critical, line_dash="solid", line_color="#ef4444", line_width=1.2, row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0b0f19",
        plot_bgcolor="#131b2e",
        height=480,
        margin=dict(l=40, r=40, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(gridcolor="#1e293b", row=1, col=1)
    fig.update_yaxes(gridcolor="#1e293b", title_text="Z-Score", row=2, col=1)
    fig.update_xaxes(gridcolor="#1e293b", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)

# Section: Auditable Anomaly Audit Log
st.markdown("#### AUDITABLE ANOMALY LOG")

if not df_anomalies.empty:
    display_df = df_anomalies.copy()
    display_df["Timestamp"] = display_df["event_time"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    display_df["Price ($)"] = display_df["price"].apply(lambda p: f"${p:.2f}")
    display_df["Volume"] = display_df["volume"].apply(lambda v: f"{int(v):,}")
    display_df["Z-Score"] = display_df["zscore"].apply(lambda z: f"{z:+.2f}")
    display_df["VWAP Div (%)"] = display_df["vwap_divergence"].apply(lambda v: f"{v * 100:.2f}%")

    cols_order = ["Timestamp", "ticker", "severity", "anomaly_type", "Price ($)", "Volume", "Z-Score", "VWAP Div (%)"]
    st.dataframe(display_df[cols_order], use_container_width=True, hide_index=True)

    csv_data = display_df[cols_order].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Audit Log (.CSV)",
        data=csv_data,
        file_name=f"surveillance_anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )
else:
    st.caption("No records available to export.")

# Auto-refresh loop
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
