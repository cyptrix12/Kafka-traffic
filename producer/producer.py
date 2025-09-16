import os, time, json
import pandas as pd
from dateutil import parser as dtp
from kafka import KafkaProducer

brokers = os.getenv("KAFKA_BROKERS", "kafka:9092")
topic = os.getenv("TOPIC", "traffic_events")
csv_path = os.getenv("CSV_PATH", "/data/traffic_sample.csv")
speed = float(os.getenv("SPEED_FACTOR", "60"))  # 60x przyspieszenie

df = pd.read_csv(csv_path)
df["timestamp"] = df["timestamp"].apply(dtp.parse)

producer = KafkaProducer(
    bootstrap_servers=brokers.split(","),
    value_serializer=lambda d: json.dumps(d).encode("utf-8"),
)

# sort by time to simulate a stream
df = df.sort_values("timestamp").reset_index(drop=True)
print(f"Producing {len(df)} events to topic {topic} @ speed x{speed}...")
start_wall = time.time()
start_ts = df.loc[0, "timestamp"]

for i, row in df.iterrows():
    # real-time pacing based on timestamp deltas
    if i > 0:
        dt = (row["timestamp"] - df.loc[i-1, "timestamp"]).total_seconds()
        delay = max(dt / speed, 0)
        time.sleep(delay)

    event = {
        "timestamp": row["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "road_id": str(row["road_id"]),
        "vehicle_count": int(row["vehicle_count"]),
    }
    producer.send(topic, event)
    print("→", event)

producer.flush()
elapsed = time.time() - start_wall
print(f"Done. Elapsed {elapsed:.2f}s")
