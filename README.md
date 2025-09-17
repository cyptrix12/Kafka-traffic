
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

## Results and discussion
<img width="1863" height="1038" alt="obraz" src="https://github.com/user-attachments/assets/5547c42f-c895-4306-a189-b022ece67f51" />

1. Partitions
   ``` python
   .config("spark.sql.shuffle.partitions", "1")
   ```
   Setting the number of partitions to one is not a typical solution and will only work for a small volume of data. All computations are executed by a single machine (or in our case by a single thread), since parallelism is not being used here. This limits unused but reserved computational power. With increasing amounts of data such a setup will become a bottleneck, so it is important to remember to switch back to the default 200 or even more partitions.
   We tested removing the line that sets spark.sql.shuffle.partitions = 1, leaving the shuffle configuration at its default value. The application still works correctly, as expected.
   
3. Raw
   ``` python
   raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", SRC_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
   )
   events = (
       raw.selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), schema).alias("d"))
          .select(
              to_timestamp(col("d.timestamp")).alias("timestamp"),
              col("d.road_id").alias("road_id"),
              col("d.vehicle_count").cast("int").alias("vehicle_count")
          )
   )
   ```
   raw is a streaming DataFrame in Spark, not a database. It’s an in-memory representation of the Kafka stream, which is later transformed into a structured DataFrame with the fields that are actually need (timestamp, road_id, vehicle_count).

4. Watermark
   ``` python
   events.withWatermark("timestamp", "2 minutes")
   ```
   A watermark is used when creating a time window for incoming data. Since events with the same timestamp may not arrive at exactly the same time, Spark allows us to set a time buffer (2 minutes) during which computations remain open. The watermark directly marks data that has already been processed, so Spark does not keep recalculating the same results over and over, but instead incrementally updates the existing aggregates.



