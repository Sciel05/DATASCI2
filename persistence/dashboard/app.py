import os
import sys
import time
from datetime import datetime, timezone, timedelta

# Add project root to sys.path so `persistence` package is importable
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import importlib
import persistence.persistence_client
importlib.reload(persistence.persistence_client)
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

    h2 {
        color: #f1f5f9 !important;
        font-weight: 700 !important;
        letter-spacing: -0.01em !important;
    }
    h4 {
        color: #94a3b8 !important;
        font-size: 0.82rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
        margin-top: 1.2rem !important;
        margin-bottom: 0.5rem !important;
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

    /* Prevent Streamlit download button ghosting / stale duplication during auto-refresh */
    div[data-testid="stDownloadButton"][data-stale="true"] {
        display: none !important;
    }
    div[data-testid="stDownloadButton"] button {
        background-color: #131b2e !important;
        color: #f8fafc !important;
        border: 1px solid #1e293b !important;
        border-radius: 6px !important;
        font-family: monospace !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
        padding: 6px 16px !important;
        transition: all 0.15s ease-in-out !important;
    }
    div[data-testid="stDownloadButton"] button:hover {
        background-color: #1e293b !important;
        border-color: #38bdf8 !important;
        color: #38bdf8 !important;
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
refresh_interval = st.sidebar.slider("Refresh Interval (sec)", min_value=2, max_value=15, value=3)

st.sidebar.markdown("---")
st.sidebar.markdown("### FILTER ANOMALIES")

# Control 1 (Sidebar): Filters anomalies in the Audit Log and Top Metrics (independent from timeline chart)
selected_ticker = st.sidebar.selectbox(
    "Asset Ticker (table filter)",
    options=["ALL"] + list(CONFIG.monitored_tickers),
    index=0,
    help="Filters the Anomaly Audit Log table and desk summary metrics by asset ticker.",
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


def _add_ticker_panels(
    fig: go.Figure,
    sym: str,
    df_t: pd.DataFrame,
    row_offset: int,
    total_rows: int,
    cfg: AlertConfig,
    show_legend: bool = True,
    show_anomaly_legend: bool = True,
    is_multi: bool = False,
) -> tuple[dict, bool]:
    """
    Builds the 2-row panel pair (price candlestick + rolling z-score) for a single ticker.
    Used for both single-ticker view (row_offset=0, total_rows=2) and multi-ticker
    view (row_offset=2*i, total_rows=2*num_tickers).
    Returns (vol_sub_layout_dict, had_anomalies_bool).
    """
    if df_t.empty:
        return {}, False

    df_t = df_t.copy()

    price_row = row_offset + 1
    z_row = row_offset + 2
    vol_axis_idx = total_rows + (row_offset // 2) + 1
    price_yaxis_name = f"y{price_row}" if price_row > 1 else "y"
    price_xaxis_name = f"x{price_row}" if price_row > 1 else "x"
    vol_yaxis_name = f"y{vol_axis_idx}"

    latest_close = df_t["close"].iloc[-1]
    prev_close = df_t["close"].iloc[-2] if len(df_t) > 1 else latest_close
    price_color = "#10b981" if latest_close >= prev_close else "#ef4444"

    # 1. Compact In-Figure Annotations (for multi-ticker mode)
    if is_multi:
        price_y_domain = fig.layout[f"yaxis{price_row}" if price_row > 1 else "yaxis"].domain
        fig.add_annotation(
            x=0.005,
            xref="paper",
            y=price_y_domain[1],
            yref="paper",
            xanchor="left",
            yanchor="top",
            text=f"<b>{sym}</b> · EXECUTION PRICE & ANOMALY FLAGS",
            showarrow=False,
            font=dict(size=10, color="#94a3b8", family="monospace"),
            bgcolor="rgba(19, 27, 46, 0.85)",
            bordercolor="#1e293b",
            borderwidth=1,
            borderpad=3,
        )
        z_y_domain = fig.layout[f"yaxis{z_row}"].domain
        fig.add_annotation(
            x=0.005,
            xref="paper",
            y=z_y_domain[1],
            yref="paper",
            xanchor="left",
            yanchor="top",
            text=f"<b>{sym}</b> · ROLLING Z-SCORE DEVIATION",
            showarrow=False,
            font=dict(size=9, color="#64748b", family="monospace"),
            bgcolor="rgba(19, 27, 46, 0.85)",
            bordercolor="#1e293b",
            borderwidth=1,
            borderpad=2,
        )

    # 2. Candlestick Trace
    fig.add_trace(
        go.Candlestick(
            x=df_t["time"],
            open=df_t["open"],
            high=df_t["high"],
            low=df_t["low"],
            close=df_t["close"],
            increasing_line_color="#10b981",
            decreasing_line_color="#ef4444",
            increasing_fillcolor="#10b981",
            decreasing_fillcolor="#ef4444",
            name="Price" if is_multi else f"{sym}",
            showlegend=show_legend,
            hovertemplate=(
                (f"<b>{sym}</b><br>" if is_multi else "")
                + "Open: <b>$%{open:.2f}</b><br>"
                "High: <b>$%{high:.2f}</b><br>"
                "Low: <b>$%{low:.2f}</b><br>"
                "Close: <b>$%{close:.2f}</b>"
                "<extra></extra>"
            ),
        ),
        row=price_row, col=1,
    )

    # 3. Pinned Price Line & Right Margin Badge
    fig.add_hline(
        y=latest_close,
        line_dash="dot",
        line_color=price_color,
        line_width=1.2,
        row=price_row, col=1,
    )
    fig.add_annotation(
        x=1.0, xref="paper",
        y=latest_close, yref=price_yaxis_name,
        text=f"<b>${latest_close:.2f}</b>",
        showarrow=False,
        font=dict(size=9 if is_multi else 10, color="#ffffff", family="monospace"),
        bgcolor=price_color,
        bordercolor=price_color,
        borderwidth=1,
        borderpad=2 if is_multi else 3,
        xanchor="left",
        yanchor="middle",
    )

    # 4. Volume Bars on Overlaid Y-Axis
    vol_colors = [
        "#10b981" if c >= o else "#ef4444"
        for o, c in zip(df_t["open"], df_t["close"])
    ]
    fig.add_trace(
        go.Bar(
            x=df_t["time"],
            y=df_t["volume"],
            marker_color=vol_colors,
            opacity=0.35,
            name="Volume",
            showlegend=show_legend,
            hovertemplate=f"<b>{sym}</b> Vol: <b>%{{y:,.0f}}</b><extra></extra>" if is_multi else "Vol: <b>%{y:,.0f}</b><extra></extra>",
            xaxis=price_xaxis_name,
            yaxis=vol_yaxis_name,
        )
    )
    max_vol_t = df_t["volume"].max() if not df_t["volume"].empty else 1
    vol_sub_layout = {
        f"yaxis{vol_axis_idx}": dict(
            overlaying=price_yaxis_name,
            side="left",
            showgrid=False,
            range=[0, max_vol_t * 4],
            showticklabels=False,
        )
    }

    # 5. Debounced Anomaly Markers
    marker_times = []
    marker_prices = []
    marker_symbols = []
    marker_colors = []
    marker_hovers = []

    in_episode = False
    for _, bar in df_t.iterrows():
        z = bar["zscore"]
        abs_z = abs(z)
        if abs_z >= cfg.zscore_warning:
            if not in_episode:
                in_episode = True
                is_crit = abs_z >= cfg.zscore_critical
                color = "#ef4444" if is_crit else "#f59e0b"
                severity_label = "CRITICAL" if is_crit else "WARNING"

                if z > 0:
                    marker_symbols.append("triangle-up")
                    marker_prices.append(bar["high"] * 1.001)
                else:
                    marker_symbols.append("triangle-down")
                    marker_prices.append(bar["low"] * 0.999)

                marker_times.append(bar["time"])
                marker_colors.append(color)
                marker_hovers.append(
                    f"<b>{sym}</b> {severity_label}<br>"
                    f"Z-Score: {z:+.2f}<br>"
                    f"Price: ${bar['close']:.2f}"
                )
        else:
            in_episode = False

    if marker_times:
        fig.add_trace(
            go.Scatter(
                x=marker_times,
                y=marker_prices,
                mode="markers",
                name="Anomaly Flag",
                showlegend=show_anomaly_legend,
                marker=dict(
                    symbol=marker_symbols,
                    size=10 if is_multi else 12,
                    color=marker_colors,
                    line=dict(width=1.0 if is_multi else 1.2, color="#ffffff"),
                ),
                text=marker_hovers,
                hovertemplate="%{text}<extra></extra>",
            ),
            row=price_row, col=1,
        )

    # 6. Z-Score Deviation Trace & Threshold Guides
    df_t["zscore"] = df_t["zscore"].clip(-6.0, 6.0)
    zscore_colors = [
        "#ef4444" if abs(z) >= cfg.zscore_critical
        else "#f59e0b" if abs(z) >= cfg.zscore_warning
        else "#38bdf8"
        for z in df_t["zscore"]
    ]
    fig.add_trace(
        go.Scatter(
            x=df_t["time"],
            y=df_t["zscore"],
            mode="lines+markers",
            name="Z-Score",
            line=dict(color="#38bdf8", width=1.5),
            marker=dict(size=4 if is_multi else 5, color=zscore_colors),
            hovertemplate=f"<b>{sym}</b> Z-Score: <b>%{{y:+.2f}}</b><extra></extra>" if is_multi else "Z-Score: <b>%{y:+.2f}</b><extra></extra>",
            showlegend=False,
        ),
        row=z_row, col=1,
    )

    fig.add_hrect(
        y0=-cfg.zscore_warning, y1=cfg.zscore_warning,
        fillcolor="#10b981", opacity=0.10, line_width=1,
        line_color="rgba(16, 185, 129, 0.4)", line_dash="dash",
        layer="below",
        row=z_row, col=1,
    )
    fig.add_hline(y=cfg.zscore_critical, line_dash="solid", line_color="#ef4444", line_width=1.2, row=z_row, col=1)
    fig.add_hline(y=-cfg.zscore_critical, line_dash="solid", line_color="#ef4444", line_width=1.2, row=z_row, col=1)

    # 7. Price and Z-Score Y-Axes Configuration
    price_min = df_t["low"].min()
    price_max = df_t["high"].max()
    price_span = price_max - price_min
    pad_bottom = price_span * 0.07
    pad_top = price_span * 0.07

    # Asymmetric padding: bump top padding to 12% when pinned reference price is within ~5% of price_max
    if price_span > 0 and (price_max - latest_close) <= price_span * 0.05:
        pad_top = price_span * 0.12
    elif price_span > 0 and (latest_close - price_min) <= price_span * 0.05:
        pad_bottom = price_span * 0.12

    price_range_kwargs = (
        {"range": [price_min - pad_bottom, price_max + pad_top]}
        if len(df_t) > 1 and price_max > price_min
        else {}
    )
    fig.update_yaxes(
        gridcolor="#1e293b",
        title_text="Price ($)",
        title_font=dict(size=10 if is_multi else 11, color="#94a3b8", family="monospace"),
        tickfont=dict(size=9 if is_multi else 10, color="#94a3b8", family="monospace"),
        tickformat=".2f",
        side="right",
        row=price_row, col=1,
        **price_range_kwargs,
    )

    z_bound = max(6.0, cfg.zscore_critical * 1.15)
    fig.update_yaxes(
        gridcolor="#1e293b",
        title_text="Z-Score",
        title_font=dict(size=10 if is_multi else 11, color="#94a3b8", family="monospace"),
        tickfont=dict(size=9 if is_multi else 10, color="#94a3b8", family="monospace"),
        range=[-z_bound, z_bound],
        side="right",
        row=z_row, col=1,
    )

    return vol_sub_layout, bool(marker_times)


# ══════════════════════════════════════════════════════════════════
# Live Surveillance Desk Fragment (Option 2: Native Streamlit Fragment)
# Confines periodic reruns to the live telemetry, charts, and tables.
# The sidebar, calibration sliders, and page config remain completely static.
# ══════════════════════════════════════════════════════════════════
@st.fragment(run_every=int(refresh_interval) if auto_refresh else None)
def render_surveillance_desk():
    # Top Bar Header
    col_title, col_stat = st.columns([3, 2])
    with col_title:
        st.markdown("## Real-Time Market Surveillance Desk")
        st.caption("Institutional abuse monitoring & statistical anomaly detection · Kafka · Spark · Cassandra")

    with col_stat:
        try:
            curr_health = client.check_health()
        except Exception:
            curr_health = health
        health_color = "#10b981" if curr_health.get("status") == "healthy" else "#ef4444"
        backend_name = curr_health.get("backend", "MockReplay")
        latency_ms = curr_health.get("latency_ms", 0.0)
        countdown_start = int(refresh_interval)
        is_auto_refresh_str = "true" if auto_refresh else "false"

        clock_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
        <meta charset="utf-8"/>
        <style>
          * {{
            box-sizing: border-box;
          }}
          body {{
            margin: 0;
            padding: 2px 0 0 0;
            background: transparent;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
            text-align: right;
            overflow: hidden;
          }}
          .session-badge {{
            background: rgba(16, 185, 129, 0.15);
            color: #10b981;
            border: 1px solid rgba(16, 185, 129, 0.3);
            padding: 2px 8px;
            border-radius: 4px;
            font-weight: 700;
            font-size: 0.75rem;
            margin-right: 8px;
            display: inline-block;
          }}
          .clock {{
            font-family: monospace;
            font-size: 0.85rem;
            color: #f8fafc;
            font-weight: 600;
          }}
          .clock-edt {{
            color: #64748b;
          }}
          .telemetry {{
            font-size: 0.8rem;
            color: #94a3b8;
            margin-top: 5px;
          }}
          .backend-val {{
            color: {health_color};
            margin-right: 10px;
            font-size: 0.85rem;
            font-weight: bold;
          }}
          .latency-val {{
            color: #f8fafc;
            font-size: 0.85rem;
            font-weight: bold;
            margin-right: 8px;
          }}
          .countdown-badge {{
            background: rgba(56, 189, 248, 0.15);
            color: #38bdf8;
            border: 1px solid rgba(56, 189, 248, 0.35);
            padding: 2px 8px;
            border-radius: 4px;
            font-family: monospace;
            font-weight: 600;
            font-size: 0.78rem;
            display: inline-block;
          }}
          .paused-badge {{
            background: rgba(245, 158, 11, 0.15);
            color: #f59e0b;
            border: 1px solid rgba(245, 158, 11, 0.35);
            padding: 2px 8px;
            border-radius: 4px;
            font-family: monospace;
            font-weight: 600;
            font-size: 0.78rem;
            display: inline-block;
          }}
        </style>
        </head>
        <body>
          <div style="margin-bottom: 3px;">
            <span class="session-badge">● REGULAR SESSION (US EQUITIES)</span>
            <span class="clock" id="live-clock">--:--:-- UTC (--:--:-- EDT)</span>
          </div>
          <div class="telemetry">
            <span>Backend:</span>
            <strong class="backend-val">{backend_name}</strong>
            <span>Latency:</span>
            <strong class="latency-val">{latency_ms:.2f} ms</strong>
            {"<span class='countdown-badge'>⟳ Next Sync: <b id='countdown-val'>" + str(countdown_start) + "s</b></span>" if auto_refresh else "<span class='paused-badge'>⏸ Sync: PAUSED</span>"}
          </div>

          <script>
            // 1. Live Real-Time Market Clock
            function updateClock() {{
              const now = new Date();
              const pad = (n) => String(n).padStart(2, '0');
              const utcStr = pad(now.getUTCHours()) + ':' + pad(now.getUTCMinutes()) + ':' + pad(now.getUTCSeconds()) + ' UTC';

              // Eastern Time (UTC-4)
              const edt = new Date(now.getTime() - (4 * 3600 * 1000));
              let h = edt.getUTCHours();
              const ampm = h >= 12 ? 'PM' : 'AM';
              h = h % 12;
              h = h ? h : 12;
              const edtStr = pad(h) + ':' + pad(edt.getUTCMinutes()) + ':' + pad(edt.getUTCSeconds()) + ' ' + ampm + ' EDT';

              const el = document.getElementById('live-clock');
              if (el) {{
                el.innerHTML = utcStr + ' <span class="clock-edt">(' + edtStr + ')</span>';
              }}
            }}
            updateClock();
            setInterval(updateClock, 1000);

            // 2. Live Countdown to Next Data Refresh
            let remaining = {countdown_start};
            const isAuto = {is_auto_refresh_str};
            function updateCountdown() {{
              if (!isAuto) return;
              const countEl = document.getElementById('countdown-val');
              if (countEl) {{
                if (remaining > 1) {{
                  remaining -= 1;
                  countEl.innerText = remaining + 's';
                }} else {{
                  countEl.innerText = 'Syncing...';
                }}
              }}
            }}
            if (isAuto) {{
              setInterval(updateCountdown, 1000);
            }}
          </script>
        </body>
        </html>
        """
        components.html(clock_html, height=62)

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
        pattern_label = "Sudden Flash Price Drop" if (recent_crit["anomaly_type"] == "price_shock" and recent_crit["zscore"] < 0) else "Extreme Sudden Price Spike" if recent_crit["anomaly_type"] == "price_shock" else "Wash Trading (Fake Self-Trading)"
        st.markdown(
            f"""
            <div class="critical-banner">
                <strong>🔴 CRITICAL MARKET ALERT ({recent_crit['ticker']})</strong>: 
                Severe <strong>{pattern_label}</strong> detected at <strong>${recent_crit['price']:.2f}</strong>. 
                Price moved <strong>{abs(recent_crit['zscore']):.1f}x beyond normal boundaries</strong> at {recent_crit['event_time'].strftime('%H:%M:%S UTC')} 
                <span style="font-size: 0.8rem; opacity: 0.85;">(Peak Z-Score: {recent_crit['zscore']:+.2f} · VWAP Divergence: {recent_crit['vwap_divergence'] * 100:.2f}%)</span>.
            </div>
            """,
            unsafe_allow_html=True,
        )
    elif warning_flags > 0:
        recent_warn = df_anomalies[df_anomalies["severity"] == "WARNING"].iloc[0]
        pattern_label = "Potential Wash Trade (Fake Volume / Self-Trading)" if recent_warn["anomaly_type"] == "wash_trade" else "Unusual Price Shock"
        explanation = "Heavy trading volume with virtually zero price movement" if recent_warn["anomaly_type"] == "wash_trade" else "Elevated volatility detected"
        st.markdown(
            f"""
            <div class="warning-banner">
                <strong>🟡 SURVEILLANCE WARNING ({recent_warn['ticker']})</strong>: 
                <strong>{pattern_label}</strong> flagged at <strong>${recent_warn['price']:.2f}</strong>. 
                {explanation} at {recent_warn['event_time'].strftime('%H:%M:%S UTC')} 
                <span style="font-size: 0.8rem; opacity: 0.85;">(Z-Score: {recent_warn['zscore']:+.2f} · VWAP Divergence: {recent_warn['vwap_divergence'] * 100:.2f}%)</span>.
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
        last_p = m.get("latest_price", 0.0)
        pct_chg = m.get("pct_change", 0.0)

        if peak_z >= cfg.zscore_critical:
            status_label = "🔴 CRITICAL"
        elif peak_z >= cfg.zscore_warning or washes > 0:
            status_label = "🟡 WARNING"
        else:
            status_label = "🟢 NORMAL"

        chg_sign = "+" if pct_chg >= 0 else ""
        chg_arrow = "▲" if pct_chg >= 0 else "▼"
        pct_str = f"{chg_arrow} {chg_sign}{pct_chg:.2f}%"

        matrix_rows.append({
            "Ticker": sym,
            "Status": status_label,
            "Last Price": f"${last_p:.2f}" if last_p > 0 else "—",
            "Window Δ%": pct_str if last_p > 0 else "—",
            "Total Anomalies": cnt,
            "Price Shocks": shocks,
            "Wash Trades": washes,
            "Peak |Z-Score|": f"{peak_z:.2f}",
            "Latest Event": m.get("latest_event_time").strftime("%H:%M:%S") if m.get("latest_event_time") else "—",
        })

    matrix_df = pd.DataFrame(matrix_rows)
    st.dataframe(matrix_df, use_container_width=True, hide_index=True)

    # Section: TradingView-Style Candlestick Chart
    st.markdown("#### REAL-TIME ANOMALY TIMELINE CO-PLOT")

    # Control 1 (Sidebar): "Asset Ticker (table filter)" filters the anomaly audit log table and severity metrics below.
    # Control 2 (Main Page): "Chart Ticker" & "Filter Stacked Tickers" independently control the real-time candlestick & z-score timeline co-plot.
    chart_options = ["ALL"] + list(CONFIG.monitored_tickers)
    default_ticker_idx = 0

    col_sel1, col_sel2 = st.columns([1, 2])
    with col_sel1:
        chart_ticker = st.selectbox(
            "Chart Ticker",
            options=chart_options,
            index=default_ticker_idx,
            key="chart_ticker_select",
            help="Select a single asset or ALL for the multi-ticker stacked timeline co-plot.",
        )

    if chart_ticker == "ALL":
        with col_sel2:
            selected_stacked = st.multiselect(
                "Filter Stacked Tickers",
                options=list(CONFIG.monitored_tickers),
                default=[],
                key="multi_ticker_select",
                help="Select specific tickers to display stacked. When empty, all monitored tickers are shown by default.",
            )
            # Explicit behavior: when empty (nothing selected), render all monitored tickers by default
            if not selected_stacked:
                active_tickers = list(CONFIG.monitored_tickers)
                st.caption("Showing all tickers — select specific ones to narrow.")
            else:
                active_tickers = selected_stacked
                st.caption(f"Showing {len(active_tickers)} selected ticker(s) — clear selection to reset to all.")
    else:
        active_tickers = [chart_ticker]


    # Check if multi-ticker view or single-ticker view
    if len(active_tickers) > 1:
        # ══════════════════════════════════════════════════════════════════
        # Multi-Ticker Stacked View (Constraints 1-5)
        # ══════════════════════════════════════════════════════════════════
        num_tickers = len(active_tickers)
        total_rows = num_tickers * 2
        row_heights = [0.65, 0.35] * num_tickers

        fig = make_subplots(
            rows=total_rows, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_heights=row_heights,
        )

        vol_layout = {}
        shown_anomaly_legend = False

        for i, sym in enumerate(active_tickers):
            df_t = client.fetch_recent_ohlc(ticker=sym, lookback_minutes=lookback_minutes)
            if df_t.empty:
                continue

            vol_sub, had_anomalies = _add_ticker_panels(
                fig=fig,
                sym=sym,
                df_t=df_t,
                row_offset=2 * i,
                total_rows=total_rows,
                cfg=cfg,
                show_legend=(i == 0),
                show_anomaly_legend=(not shown_anomaly_legend),
                is_multi=True,
            )
            vol_layout.update(vol_sub)
            if had_anomalies:
                shown_anomaly_legend = True

        # ── Constraint 2: X-Axis Labels Only on Bottom-Most Row & Disable All Range Sliders ──
        fig.update_xaxes(rangeslider_visible=False)
        for r in range(1, total_rows):
            fig.update_xaxes(showticklabels=False, gridcolor="#1e293b", row=r, col=1)
        fig.update_xaxes(
            showticklabels=True,
            gridcolor="#1e293b",
            tickfont=dict(size=10, color="#94a3b8", family="monospace"),
            row=total_rows, col=1,
        )

        # ── Layout Configuration ──
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="#0b0f19",
            plot_bgcolor="#131b2e",
            height=max(540, 250 * num_tickers),
            margin=dict(l=25, r=65, t=40, b=25),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.01,
                xanchor="right",
                x=1,
                font=dict(size=11, color="#94a3b8", family="monospace"),
            ),
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor="#0f172a",
                font_size=11,
                font_family="monospace",
                font_color="#cbd5e1",
                bordercolor="#334155",
            ),
            xaxis_rangeslider_visible=False,
            **vol_layout,
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        # ══════════════════════════════════════════════════════════════════
        # Single-Ticker View
        # ══════════════════════════════════════════════════════════════════
        single_sym = active_tickers[0]
        df_ohlc = client.fetch_recent_ohlc(ticker=single_sym, lookback_minutes=lookback_minutes)

        if df_ohlc.empty:
            st.info(f"No OHLC data available for {single_sym} in the trailing {lookback_minutes}m window.")
        else:
            # ── Quick OHLCV Header Stat Strip ──
            latest_close = df_ohlc["close"].iloc[-1]
            prev_close = df_ohlc["close"].iloc[-2] if len(df_ohlc) > 1 else latest_close
            first_close = df_ohlc["close"].iloc[0]
            window_chg = latest_close - first_close
            window_chg_pct = (window_chg / first_close) * 100 if first_close else 0.0
            price_color = "#10b981" if latest_close >= prev_close else "#ef4444"
            chg_color = "#10b981" if window_chg >= 0 else "#ef4444"
            chg_sign = "+" if window_chg >= 0 else ""
            chg_arrow = "▲" if window_chg >= 0 else "▼"
            tot_vol = df_ohlc["volume"].sum()
            high_p = df_ohlc["high"].max()
            low_p = df_ohlc["low"].min()
            open_p = df_ohlc["open"].iloc[0]

            company_names = {
                "AAPL": "Apple Inc.",
                "MSFT": "Microsoft Corp.",
                "GOOGL": "Alphabet Inc.",
                "TSLA": "Tesla Inc.",
                "NVDA": "NVIDIA Corp.",
            }
            cname = company_names.get(single_sym, "US Equity")

            st.markdown(
                f"""
                <div style="background: #131b2e; border: 1px solid #1e293b; border-radius: 6px; padding: 10px 16px; margin-bottom: 8px; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; font-family: monospace;">
                    <div>
                        <strong style="color: #f8fafc; font-size: 1.05rem;">{single_sym}</strong> 
                        <span style="color: #94a3b8; font-size: 0.85rem; margin-right: 12px;">· {cname} · 30s Candles · NASDAQ</span>
                        <span style="color: {price_color}; font-weight: 700; font-size: 1.1rem; margin-right: 8px;">${latest_close:.2f}</span>
                        <span style="background: {'rgba(16, 185, 129, 0.15)' if window_chg >= 0 else 'rgba(239, 68, 68, 0.15)'}; color: {chg_color}; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.85rem;">
                            {chg_arrow} {chg_sign}${window_chg:.2f} ({chg_sign}{window_chg_pct:.2f}%)
                        </span>
                    </div>
                    <div style="color: #94a3b8; font-size: 0.82rem;">
                        <span style="margin-right: 12px;">O: <strong style="color: #cbd5e1;">${open_p:.2f}</strong></span>
                        <span style="margin-right: 12px;">H: <strong style="color: #cbd5e1;">${high_p:.2f}</strong></span>
                        <span style="margin-right: 12px;">L: <strong style="color: #cbd5e1;">${low_p:.2f}</strong></span>
                        <span style="margin-right: 12px;">C: <strong style="color: #cbd5e1;">${latest_close:.2f}</strong></span>
                        <span>Vol: <strong style="color: #cbd5e1;">{tot_vol:,.0f}</strong></span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.08,
                subplot_titles=(
                    f"<b>{single_sym}</b> · EXECUTION PRICE & ANOMALY FLAGS",
                    f"<b>{single_sym}</b> · ROLLING Z-SCORE DEVIATION",
                ),
                row_heights=[0.65, 0.35],
            )
            fig.for_each_annotation(lambda a: a.update(
                font=dict(size=11, color="#94a3b8", family="monospace"),
                x=0.005,
                xanchor="left",
                yanchor="bottom",
                yshift=8,
            ))

            vol_layout, _ = _add_ticker_panels(
                fig=fig,
                sym=single_sym,
                df_t=df_ohlc,
                row_offset=0,
                total_rows=2,
                cfg=cfg,
                show_legend=True,
                show_anomaly_legend=True,
                is_multi=False,
            )

            fig.update_xaxes(rangeslider_visible=False)
            fig.update_xaxes(showticklabels=False, gridcolor="#1e293b", row=1, col=1)
            fig.update_xaxes(
                showticklabels=True,
                gridcolor="#1e293b",
                tickfont=dict(size=10, color="#94a3b8", family="monospace"),
                row=2, col=1,
            )

            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="#0b0f19",
                plot_bgcolor="#131b2e",
                height=540,
                margin=dict(l=25, r=65, t=40, b=25),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.03,
                    xanchor="right",
                    x=1,
                    font=dict(size=11, color="#94a3b8", family="monospace"),
                ),
                hovermode="x unified",
                hoverlabel=dict(
                    bgcolor="#0f172a",
                    font_size=11,
                    font_family="monospace",
                    font_color="#cbd5e1",
                    bordercolor="#334155",
                ),
                xaxis_rangeslider_visible=False,
                **vol_layout,
            )

            st.plotly_chart(fig, use_container_width=True)

    # Section: Auditable Anomaly Audit Log
    st.markdown("#### AUDITABLE ANOMALY LOG")

    if not df_anomalies.empty:
        display_df = df_anomalies.copy()
        display_df["Case ID"] = [
            f"INC-{abs(hash(str(t) + sym)) % 10000:04d}"
            for t, sym in zip(display_df["event_time"], display_df["ticker"])
        ]
        display_df["Timestamp"] = display_df["event_time"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        display_df["Price ($)"] = display_df["price"].apply(lambda p: f"${p:.2f}")
        display_df["Volume"] = display_df["volume"].apply(lambda v: f"{int(v):,}")
        display_df["Z-Score"] = display_df["zscore"].apply(lambda z: f"{z:+.2f}")
        display_df["VWAP Div (%)"] = display_df["vwap_divergence"].apply(lambda v: f"{v * 100:.2f}%")

        # Map to FINRA / SEC Regulatory Rule & Desk Status
        rule_map = {
            "price_shock": "SEC Rule 10b-5 (Excessive Volatility)",
            "wash_trade": "FINRA Rule 6140 (Wash Sale / Self-Trade)",
        }
        display_df["Regulatory Rule"] = display_df["anomaly_type"].map(lambda a: rule_map.get(a, "FINRA Surveillance Flag"))

        status_map = {
            "CRITICAL": "🔴 ESCALATED TO COMPLIANCE",
            "WARNING": "🟡 UNDER REVIEW",
            "INFO": "🔵 AUDIT LOGGED",
        }
        display_df["Desk Status"] = display_df["severity"].map(lambda s: status_map.get(s, "UNDER REVIEW"))

        cols_order = [
            "Case ID", "Timestamp", "ticker", "Desk Status", "Regulatory Rule",
            "Price ($)", "Volume", "Z-Score", "VWAP Div (%)"
        ]
        st.dataframe(display_df[cols_order], use_container_width=True, hide_index=True)

        csv_data = display_df[cols_order].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download Audit Log (.CSV)",
            data=csv_data,
            file_name=f"surveillance_anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key="download_audit_log_csv",
        )
    else:
        st.caption("No records available to export.")


# Render the live surveillance desk fragment
render_surveillance_desk()
