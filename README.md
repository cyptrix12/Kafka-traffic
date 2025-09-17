
# 🚦 Real-Time Traffic Congestion Detection

This project demonstrates how to detect road traffic congestion in **real-time** using  
**Kafka + PySpark Structured Streaming + Streamlit**.  

Traffic data is simulated from a CSV file, processed in streaming mode, and displayed in a live dashboard.

---

## 📂 Project Structure

```

traffic-rt/
├─ docker-compose.yml
├─ data/
│  └─ traffic\_sample.csv
├─ producer/
│  ├─ producer.py
│  ├─ Dockerfile
│  └─ requirements.txt
├─ spark/
│  ├─ app/traffic\_stream.py
│  └─ requirements.txt
└─ dashboard/
├─ app.py
├─ Dockerfile
└─ requirements.txt

````

---

## ⚙️ Configuration

**Producer**
- `CSV_PATH` – path to the CSV file with traffic data  
- `SPEED_FACTOR` – playback speed factor (e.g. `60` = 1 minute per second)  
- `LOOP=1` – enable replay loop  
- `USE_NOW=1` – replace timestamps with current time (important for Spark watermarking)  

**Spark**
- `CONGESTION_THRESHOLD` – threshold for congestion detection (default: `25`)  
- `SOURCE_TOPIC`, `AGG_TOPIC`, `ALERT_TOPIC` – Kafka topics used in the pipeline  

---

## ▶️ How to Run

1. Clone the repository:
```bash
git clone https://github.com/cyptrix12/Kafka-traffic.git
cd traffic-rt
````

2. Start the containers:

```bash
docker compose up --build
```

This will start:

* `kafka` – Redpanda broker (Kafka-compatible)
* `producer` – streams CSV traffic data into Kafka
* `spark` – PySpark Structured Streaming job detecting congestion
* `dashboard` – Streamlit web app

3. Open the dashboard:
   👉 [http://localhost:8501](http://localhost:8501)

---
