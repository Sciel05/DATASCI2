FROM eclipse-temurin:17-jdk-jammy
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir pyspark==4.2.0 pyarrow pandas confluent-kafka cassandra-driver==3.30.1 requests "streamlit>=1.42.0" "plotly>=6.0.0" pytest
WORKDIR /workspace
