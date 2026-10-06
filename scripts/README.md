# Local Pipeline Runner & Restart Guide

This directory contains standalone runner scripts to start the distributed surveillance pipeline without Docker Desktop, using your local **Ubuntu WSL2** environment for backend services and **Windows PowerShell** for client applications.

---

## Helper Scripts Overview

| Script | Purpose |
| :--- | :--- |
| [`scripts/start_backend.sh`](start_backend.sh) | Starts both **Apache Cassandra** (port `9042`) and **Apache Kafka** (port `9092`) in ~2 seconds. |
| [`scripts/run_spark_detector.sh`](run_spark_detector.sh) | Launches the **Spark Structured Streaming** anomaly detection engine with the Kafka connector pre-configured. |

---

## 1. Prerequisites & Dependencies

Everything below is required for the full pipeline. *(Already installed and configured on this machine)*.

### A. Ubuntu WSL System Packages (For `start_backend.sh`)
These run the distributed database and messaging broker:

| Component | What It Does | Installation Command (in Ubuntu WSL) |
| :--- | :--- | :--- |
| **Java 17 (`openjdk-17-jdk`)** | Runtime for Cassandra and Kafka | `sudo apt install -y openjdk-17-jdk` |
| **Apache Cassandra (4.1+)** | Stores `raw_events` and `anomalies` tables | `sudo apt install -y cassandra` |
| **Apache Kafka (4.2+)** | Message broker holding `market-ticks` topic | Extracted into `~/kafka_2.13-4.2.2` via `curl` |
| **`curl` & `tar`** | Utilities to download and extract Kafka | `sudo apt install -y curl tar` |

---

### B. Ubuntu WSL Python Packages (For `run_spark_detector.sh`)
These run Frank's stream processing and statistical detection engine:

| Package | Purpose | Installation Command (in Ubuntu WSL) |
| :--- | :--- | :--- |
| **`pyspark` (4.2.0)** | Spark Structured Streaming engine | `pip install pyspark==4.2.0` |
| **`cassandra-driver`** | Persists detected anomalies into Cassandra | `pip install cassandra-driver` |
| **`pandas` & `numpy`** | Vectorized math for Z-score, CUSUM, EWMA, and VWAP | `pip install pandas numpy` |
| **Spark Kafka Connector** | Bridges Kafka streams into Spark DataFrames | Handled automatically at runtime via Maven (`--packages org.apache.spark:spark-sql-kafka...`) |

---

### C. Windows PowerShell Packages (For Producer & Dashboard)
These run the market replay producer and the Streamlit web dashboard:

| Package | Purpose | Installation Command (in Windows PowerShell) |
| :--- | :--- | :--- |
| **`streamlit`** | Institutional surveillance desk web UI | `pip install streamlit` |
| **`plotly`** | Interactive 3-pane candlestick and oscillator charts | `pip install plotly` |
| **`confluent-kafka`** | High-throughput producer sending ticks to Kafka | `pip install confluent-kafka` |
| **`cassandra-driver`** | Queries Cassandra for the web dashboard | `pip install cassandra-driver` |
| **`requests`** | Dispatches Discord and Slack webhook notifications | `pip install requests` |
| **`pytest`** | Automated testing suite for persistence & alerting | `pip install pytest` |

---

### Quick Verification Commands
Run these anytime to confirm your setup is 100% operational:

**In Windows PowerShell:**
```powershell
python -c "import streamlit, plotly, cassandra, confluent_kafka, requests; print('>>> Windows dependencies: ALL READY! <<<')"
```

**In Ubuntu WSL:**
```bash
python3 -c "import pyspark, cassandra, pandas, numpy; print('>>> Ubuntu WSL dependencies: ALL READY! <<<')"
```

---

## 2. The "After-Restart" Runbook (4 Windows in Order)

Whenever you restart your machine or want to run the pipeline again, open your terminal windows and run these commands in order:

---

### Window 1: Start Cassandra & Kafka (Ubuntu WSL)
Open your **Ubuntu WSL** terminal:

```bash
/mnt/c/Users/user/Downloads/DATASCI2/scripts/start_backend.sh
```
*(Starts Cassandra on port `9042` and Kafka on port `9092` in the background).*

---

### Window 2: Start Spark Streaming Detector (Ubuntu WSL)
Open a **second Ubuntu WSL** terminal:

```bash
/mnt/c/Users/user/Downloads/DATASCI2/scripts/run_spark_detector.sh
```
*(Spark connects to Kafka, loads the streaming pipeline, and begins listening for incoming ticks).*

---

### Window 3: Start the Producer (Windows PowerShell)
Open a **Windows PowerShell** window:

```powershell
cd C:\Users\user\Downloads\DATASCI2

python data-ingestion/producer.py `
    --csv data-ingestion/aapl_msft_googl_tsla_nvda_ticks_labeled.csv `
    --topic market-ticks `
    --live `
    --speed 100
```
> **Tip:** Change `--speed 100` to `--speed 1000` to stream the entire 140,751-tick dataset in ~2 minutes at ~1,125 msgs/sec.

---

### Window 4: Start the Surveillance Dashboard (Windows PowerShell)
Open a **second Windows PowerShell** window:

```powershell
cd C:\Users\user\Downloads\DATASCI2

$env:USE_MOCK="False"
streamlit run persistence/dashboard/app.py
```
*(Opens `http://localhost:8501` in your browser with **`Backend: Cassandra`** in green, rendering real-time Candlestick charts, moving VWAP, and active anomaly alerts).*

---

### Window 5 (Optional): Alerting Daemon (Windows PowerShell)
If you want real-time anomaly notifications dispatched to Discord:

```powershell
cd C:\Users\user\Downloads\DATASCI2

$env:DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/1556283877759262851/CbrWEg67pWDKwdnAd425ITB6gP8tWKt2qf_jcdiNPmXB8c9p6gLE4bt5Tb8xo1RquX3B"
python persistence/run_alerting.py --live
```
*(Continuously polls Cassandra, logs to `alerts.log`, and pushes critical alerts to Discord).*

---

## 3. How to Stop Everything Cleanly

When you are done with your session:

1. **In Windows PowerShell:**  
   Press `Ctrl + C` in the Producer, Dashboard, and Alerting windows.
2. **In Ubuntu WSL:**  
   Press `Ctrl + C` in the Spark window.
3. **Stop Backend Services (Ubuntu WSL):**
   ```bash
   # Stop Kafka
   ~/kafka_2.13-4.2.2/bin/kafka-server-stop.sh

   # Stop Cassandra
   sudo service cassandra stop
   ```
