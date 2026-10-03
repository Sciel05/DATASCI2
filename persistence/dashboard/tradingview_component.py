"""
tradingview_component.py - Generates an institutional 60 FPS HTML5 Canvas TradingView chart.
Provides smooth, hardware-accelerated candlestick + volume + rolling Z-score timeline with ZERO blackouts.
"""

import json
import os

# Read the local standalone bundle
_JS_PATH = os.path.join(os.path.dirname(__file__), "lightweight-charts.js")
if os.path.exists(_JS_PATH):
    with open(_JS_PATH, "r", encoding="utf-8") as f:
        LIGHTWEIGHT_CHARTS_JS = f.read()
else:
    LIGHTWEIGHT_CHARTS_JS = ""


def get_tradingview_chart_html(
    default_ticker: str = "AAPL",
    lookback_minutes: int = 15,
    is_focused_ticker: bool = True,
    is_live: bool = True,
) -> str:
    """Return an interactive HTML5 TradingView chart with zero DOM-wipe streaming."""
    try:
        from persistence.dashboard.chart_server import get_chart_data_dict
        init_dict = get_chart_data_dict(default_ticker, lookback_minutes, is_live=is_live)
        init_json = json.dumps(init_dict)
    except Exception:
        init_json = "null"

    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<style>
  * {{
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }}
  body {{
    background-color: #0b0f19;
    color: #f8fafc;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
    overflow: hidden;
    user-select: none;
  }}
  #chart-container {{
    display: flex;
    flex-direction: column;
    height: 820px;
    width: 100%;
    background: #0b0f19;
    border: 1px solid #1e293b;
    border-radius: 8px;
    overflow: hidden;
  }}
  /* Header Toolbar */
  .toolbar {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: #131b2e;
    padding: 8px 16px;
    border-bottom: 1px solid #1e293b;
    font-size: 0.85rem;
    height: 44px;
    flex: 0 0 44px;
    box-sizing: border-box;
  }}
  .ticker-selector {{
    display: flex;
    gap: 6px;
    align-items: center;
  }}
  .ticker-btn {{
    background: #1e293b;
    border: 1px solid #334155;
    color: #94a3b8;
    padding: 4px 10px;
    border-radius: 4px;
    font-family: monospace;
    font-weight: 700;
    font-size: 0.8rem;
    cursor: pointer;
    transition: all 0.15s ease;
  }}
  .ticker-btn:hover {{
    background: #334155;
    color: #f8fafc;
  }}
  .ticker-btn.active {{
    background: #0284c7;
    border-color: #38bdf8;
    color: #ffffff;
    box-shadow: 0 0 10px rgba(56, 189, 248, 0.3);
  }}
  .focus-badge {{
    background: rgba(56, 189, 248, 0.15);
    border: 1px solid rgba(56, 189, 248, 0.4);
    color: #38bdf8;
    padding: 3px 10px;
    border-radius: 4px;
    font-family: monospace;
    font-weight: 800;
    font-size: 0.85rem;
    letter-spacing: 0.5px;
  }}
  .action-strip {{
    display: flex;
    gap: 6px;
    align-items: center;
    margin-left: 10px;
  }}
  .action-btn {{
    background: #0f172a;
    border: 1px solid #334155;
    color: #cbd5e1;
    padding: 3px 8px;
    border-radius: 4px;
    font-family: monospace;
    font-weight: 600;
    font-size: 0.75rem;
    cursor: pointer;
    transition: all 0.15s ease;
  }}
  .action-btn:hover {{
    background: #1e293b;
    border-color: #38bdf8;
    color: #38bdf8;
  }}
  .quote-strip {{
    display: flex;
    align-items: center;
    gap: 12px;
    font-family: monospace;
  }}
  .price-val {{
    font-size: 1.15rem;
    font-weight: 800;
  }}
  .chg-val {{
    font-size: 0.82rem;
    font-weight: 700;
    padding: 2px 6px;
    border-radius: 4px;
  }}
  .badge-pos {{
    background: rgba(16, 185, 129, 0.15);
    color: #10b981;
    border: 1px solid rgba(16, 185, 129, 0.3);
  }}
  .badge-neg {{
    background: rgba(239, 68, 68, 0.15);
    color: #ef4444;
    border: 1px solid rgba(239, 68, 68, 0.3);
  }}
  .status-pulse {{
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 0.75rem;
    font-weight: 700;
    color: #10b981;
    background: rgba(16, 185, 129, 0.1);
    padding: 3px 8px;
    border-radius: 4px;
    border: 1px solid rgba(16, 185, 129, 0.25);
  }}
  .candle-timer-badge {{
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 0.75rem;
    font-weight: 600;
    font-family: monospace;
    color: #38bdf8;
    background: rgba(56, 189, 248, 0.12);
    padding: 3px 8px;
    border-radius: 4px;
    border: 1px solid rgba(56, 189, 248, 0.3);
  }}
  .pulse-dot {{
    width: 7px;
    height: 7px;
    background: #10b981;
    border-radius: 50%;
    animation: livePulse 1.4s infinite;
  }}
  @keyframes livePulse {{
    0% {{ transform: scale(0.9); opacity: 0.7; }}
    50% {{ transform: scale(1.3); opacity: 1; box-shadow: 0 0 8px #10b981; }}
    100% {{ transform: scale(0.9); opacity: 0.7; }}
  }}
  /* Chart Canvas Areas */
  #main-chart {{
    height: 380px;
    flex: 0 0 380px;
    width: 100%;
    position: relative;
    min-height: 0;
  }}
  #sub-chart {{
    height: 195px;
    flex: 0 0 195px;
    width: 100%;
    border-top: 1px solid #1e293b;
    position: relative;
    min-height: 0;
  }}
  #vwap-chart {{
    height: 195px;
    flex: 0 0 195px;
    width: 100%;
    border-top: 1px solid #1e293b;
    position: relative;
    min-height: 0;
  }}
  .sub-label {{
    position: absolute;
    top: 6px;
    left: 12px;
    font-size: 0.72rem;
    font-family: monospace;
    font-weight: 700;
    color: #64748b;
    z-index: 10;
    background: rgba(15, 23, 42, 0.75);
    padding: 2px 6px;
    border-radius: 3px;
    border: 1px solid #1e293b;
    pointer-events: none;
  }}
</style>
</head>
<body>

<div id="chart-container">
  <div class="toolbar">
    <div style="display: flex; align-items: center;">
      <div class="ticker-selector">
        <span style="color: #38bdf8; font-weight: 700; margin-right: 6px; font-size: 0.8rem;">SURVEILLANCE FOCUS:</span>
        <span class="focus-badge">{default_ticker}</span>
      </div>
      <div class="action-strip">
        <button class="action-btn" onclick="resetZoom()" title="Reset zoom and fit all candles in lookback window">Fit Window</button>
        <button class="action-btn" onclick="jumpToLive()" title="Scroll to current live candlestick">Live Tip</button>
      </div>
    </div>
    <div class="quote-strip">
      <span id="asset-name" style="color: #94a3b8; font-size: 0.85rem;">{default_ticker} · NASDAQ · 30s Candles</span>
      <span id="live-price" class="price-val" style="color: #10b981;">$---.--</span>
      <span id="live-chg" class="chg-val badge-pos">+0.00%</span>
      <div id="candle-timer-badge" class="candle-timer-badge" title="Seconds remaining until current 30s candle closes and next bar forms">
        <span style="opacity: 0.75;">⏱ Next Candle:</span> <strong id="candle-countdown">--s</strong>
      </div>
      <div class="status-pulse">
        <div class="pulse-dot"></div>
        <span>LIVE STREAMING</span>
      </div>
    </div>
  </div>

  <div id="main-chart">
    <div class="sub-label" id="main-label">{default_ticker} · EXECUTION PRICE & ANOMALY MARKERS</div>
  </div>

  <div id="sub-chart">
    <div class="sub-label">{default_ticker} · ROLLING Z-SCORE DEVIATION (±3.0σ WARN / ±4.5σ CRIT)</div>
  </div>

  <div id="vwap-chart">
    <div class="sub-label">{default_ticker} · VWAP DIVERGENCE (%) (±1.0% THRESHOLD)</div>
  </div>
</div>

<script>
{LIGHTWEIGHT_CHARTS_JS}
</script>

<script>
  const initialData = {init_json};
  let currentTicker = "{default_ticker}";
  let lookback = {lookback_minutes};
  let isInitialLoad = true;

  const mainContainer = document.getElementById("main-chart");
  const subContainer = document.getElementById("sub-chart");
  const vwapContainer = document.getElementById("vwap-chart");

  // Common chart styling for sleek dark institutional look
  const baseChartOptions = {{
    layout: {{
      background: {{ type: "solid", color: "#0b0f19" }},
      textColor: "#94a3b8",
      fontFamily: "monospace",
      fontSize: 11,
    }},
    grid: {{
      vertLines: {{ color: "#162032" }},
      horzLines: {{ color: "#162032" }},
    }},
    crosshair: {{
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: {{ color: "#38bdf8", width: 1, style: 2, labelBackgroundColor: "#0284c7" }},
      horzLine: {{ color: "#38bdf8", width: 1, style: 2, labelBackgroundColor: "#0284c7" }},
    }},
  }};

  // 1. Initialize Main Price & Volume Chart (Hide its timeScale to maximize vertical space)
  const mainChart = LightweightCharts.createChart(mainContainer, {{
    ...baseChartOptions,
    height: 380,
    timeScale: {{
      visible: false,
    }},
    rightPriceScale: {{
      borderColor: "#1e293b",
      scaleMargins: {{ top: 0.08, bottom: 0.22 }},
      autoScale: true,
    }},
  }});

  const candleSeries = mainChart.addCandlestickSeries({{
    upColor: "#10b981",
    downColor: "#ef4444",
    borderVisible: false,
    wickUpColor: "#10b981",
    wickDownColor: "#ef4444",
    priceFormat: {{
      type: "price",
      precision: 2,
      minMove: 0.01,
    }},
  }});

  const volumeSeries = mainChart.addHistogramSeries({{
    color: "#26a69a",
    priceFormat: {{ type: "volume" }},
    priceScaleId: "vol_scale",
    scaleMargins: {{ top: 0.8, bottom: 0 }},
  }});

  mainChart.priceScale("vol_scale").applyOptions({{
    scaleMargins: {{ top: 0.82, bottom: 0 }},
  }});

  // 2. Initialize Sub Z-Score Chart (Hide its timeScale)
  const subChart = LightweightCharts.createChart(subContainer, {{
    ...baseChartOptions,
    height: 195,
    timeScale: {{
      visible: false,
    }},
    rightPriceScale: {{
      borderColor: "#1e293b",
      scaleMargins: {{ top: 0.12, bottom: 0.12 }},
      autoScale: true,
    }},
  }});

  const zscoreSeries = subChart.addLineSeries({{
    color: "#06b6d4",
    lineWidth: 2,
    priceFormat: {{
      type: "custom",
      formatter: (p) => p.toFixed(2) + "σ",
    }},
  }});

  // Critical and Warning threshold lines on Z-score pane
  zscoreSeries.createPriceLine({{ price: 4.5, color: "rgba(239, 68, 68, 0.8)", lineWidth: 1, lineStyle: 2, title: "+4.5σ CRIT" }});
  zscoreSeries.createPriceLine({{ price: 3.0, color: "rgba(245, 158, 11, 0.8)", lineWidth: 1, lineStyle: 2, title: "+3.0σ WARN" }});
  zscoreSeries.createPriceLine({{ price: 0.0, color: "rgba(100, 116, 139, 0.4)", lineWidth: 1, lineStyle: 0, title: "0.0σ MEAN" }});
  zscoreSeries.createPriceLine({{ price: -3.0, color: "rgba(245, 158, 11, 0.8)", lineWidth: 1, lineStyle: 2, title: "-3.0σ WARN" }});
  zscoreSeries.createPriceLine({{ price: -4.5, color: "rgba(239, 68, 68, 0.8)", lineWidth: 1, lineStyle: 2, title: "-4.5σ CRIT" }});

  // 3. Initialize VWAP Divergence Chart (Displays the primary timeScale for all 3 panes)
  const vwapChart = LightweightCharts.createChart(vwapContainer, {{
    ...baseChartOptions,
    height: 195,
    timeScale: {{
      visible: true,
      borderColor: "#1e293b",
      timeVisible: true,
      secondsVisible: true,
    }},
    rightPriceScale: {{
      borderColor: "#1e293b",
      scaleMargins: {{ top: 0.15, bottom: 0.15 }},
      autoScale: true,
    }},
  }});

  const vwapDivSeries = vwapChart.addLineSeries({{
    color: "#a78bfa",
    lineWidth: 2,
    priceFormat: {{
      type: "custom",
      formatter: (p) => p.toFixed(2) + "%",
    }},
  }});

  // VWAP divergence threshold lines at ±1%
  vwapDivSeries.createPriceLine({{ price: 1.0, color: "rgba(239, 68, 68, 0.8)", lineWidth: 1, lineStyle: 2, title: "+1.0% THRESH" }});
  vwapDivSeries.createPriceLine({{ price: 0.0, color: "rgba(100, 116, 139, 0.4)", lineWidth: 1, lineStyle: 0, title: "0.0% MEAN" }});
  vwapDivSeries.createPriceLine({{ price: -1.0, color: "rgba(239, 68, 68, 0.8)", lineWidth: 1, lineStyle: 2, title: "-1.0% THRESH" }});

  // Synchronize crosshair & zooming across all 3 charts safely using LogicalRange
  let isSyncing = false;
  function bindSync(src, targets) {{
    src.timeScale().subscribeVisibleLogicalRangeChange((range) => {{
      if (isSyncing || !range || range.from === null || range.to === null) return;
      isSyncing = true;
      try {{
        targets.forEach((t) => {{
          if (t && t.timeScale) {{
            t.timeScale().setVisibleLogicalRange(range);
          }}
        }});
      }} catch (err) {{
        // sync error guard
      }} finally {{
        isSyncing = false;
      }}
    }});
  }}

  bindSync(mainChart, [subChart, vwapChart]);
  bindSync(subChart, [mainChart, vwapChart]);
  bindSync(vwapChart, [mainChart, subChart]);

  // Handle responsive resizing
  function resizeCharts() {{
    const w = mainContainer.clientWidth;
    if (w > 0) {{
      mainChart.applyOptions({{ width: w, height: 380 }});
      subChart.applyOptions({{ width: w, height: 195 }});
      vwapChart.applyOptions({{ width: w, height: 195 }});
    }}
  }}
  window.addEventListener("resize", resizeCharts);
  setTimeout(resizeCharts, 50);

  // Apply unified data payload
  function applyData(data, fit) {{
    if (!data) return;

    try {{
      if (data.candles && data.candles.length > 0) {{
        candleSeries.setData(data.candles);
      }}
    }} catch (e) {{ console.error("Candle error:", e); }}

    try {{
      if (data.volumes && data.volumes.length > 0) {{
        volumeSeries.setData(data.volumes);
      }}
    }} catch (e) {{ console.error("Volume error:", e); }}

    try {{
      if (data.zscores && data.zscores.length > 0) {{
        zscoreSeries.setData(data.zscores);
      }}
    }} catch (e) {{ console.error("Zscore error:", e); }}

    try {{
      if (data.vwap_divs && data.vwap_divs.length > 0) {{
        vwapDivSeries.setData(data.vwap_divs);
      }}
    }} catch (e) {{ console.error("VWAP error:", e); }}

    try {{
      if (data.markers && data.markers.length > 0) {{
        candleSeries.setMarkers(data.markers);
      }}
    }} catch (e) {{ console.error("Marker error:", e); }}

    if (fit) {{
      try {{ mainChart.timeScale().fitContent(); }} catch (e) {{}}
      try {{ subChart.timeScale().fitContent(); }} catch (e) {{}}
      try {{ vwapChart.timeScale().fitContent(); }} catch (e) {{}}
    }}

    const lp = data.last_price || 0.0;
    const chg = data.pct_change || 0.0;
    const priceEl = document.getElementById("live-price");
    const chgEl = document.getElementById("live-chg");

    if (priceEl && lp > 0) {{
      priceEl.innerText = "$" + lp.toFixed(2);
      priceEl.style.color = chg >= 0 ? "#10b981" : "#ef4444";
    }}
    if (chgEl) {{
      const sign = chg >= 0 ? "+" : "";
      chgEl.innerText = (chg >= 0 ? "▲ " : "▼ ") + sign + chg.toFixed(2) + "%";
      chgEl.className = "chg-val " + (chg >= 0 ? "badge-pos" : "badge-neg");
    }}
  }}

  const isLiveMode = "{"1" if is_live else "0"}";

  // 3. Ultra-Smooth Polling Engine (Port 8506)
  async function fetchAndUpdate() {{
    try {{
      const resp = await fetch("http://127.0.0.1:8506/api/chart_data?ticker=" + currentTicker + "&lookback=" + lookback + "&live=" + isLiveMode);
      if (!resp.ok) return;
      const data = await resp.json();
      applyData(data, isInitialLoad);
      if (isInitialLoad) isInitialLoad = false;
    }} catch (err) {{
      console.warn("Chart sync error:", err);
    }}
  }}

  // Render initial precomputed payload immediately with zero delay
  if (initialData && initialData.candles && initialData.candles.length > 0) {{
    applyData(initialData, true);
    isInitialLoad = false;
  }}

  // Reset zoom to fit entire lookback window
  function resetZoom() {{
    mainChart.timeScale().fitContent();
    subChart.timeScale().fitContent();
    vwapChart.timeScale().fitContent();
  }}

  // Jump / scroll to live right-hand candlestick tip
  function jumpToLive() {{
    mainChart.timeScale().scrollToRealTime();
    subChart.timeScale().scrollToRealTime();
    vwapChart.timeScale().scrollToRealTime();
  }}

  // Switch ticker on button click without reloading the iframe
  function switchTicker(ticker) {{
    if (currentTicker === ticker) return;
    currentTicker = ticker;
    isInitialLoad = true;

    // Update active button state
    document.querySelectorAll(".ticker-btn").forEach((btn) => {{
      btn.classList.toggle("active", btn.innerText === ticker);
    }});

    // Update labels
    document.getElementById("asset-name").innerText = ticker + " · NASDAQ · 30s Candles";
    document.getElementById("main-label").innerText = ticker + " · EXECUTION PRICE & ANOMALY MARKERS";

    fetchAndUpdate();
  }}

  // 30-second candlestick countdown timer (anchored to :00 and :30 boundaries)
  function updateCandleCountdown() {{
    const sec = Math.floor(Date.now() / 1000);
    const rem = 30 - (sec % 30);
    const el = document.getElementById("candle-countdown");
    if (el) {{
      if (rem === 30 || rem === 0) {{
        el.innerText = "New Bar!";
        el.style.color = "#10b981";
      }} else {{
        el.innerText = (rem < 10 ? "0" + rem : rem) + "s";
        el.style.color = "#38bdf8";
      }}
    }}
  }}
  updateCandleCountdown();
  setInterval(updateCandleCountdown, 1000);

  // Background 1-second continuous live loop
  setInterval(fetchAndUpdate, 1000);
</script>
</body>
</html>
"""
