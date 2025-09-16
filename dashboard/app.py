import os, json, threading, time, queue
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

brokers = os.getenv("KAFKA_BROKERS", "kafka:9092")
agg_topic = os.getenv("AGG_TOPIC", "traffic_agg")
alert_topic = os.getenv("ALERT_TOPIC", "congestion_alerts")

st.set_page_config(page_title="Real-Time Traffic", layout="wide")
st.title("🚦 Real-Time Traffic Congestion – PySpark + Kafka")

# Thread-safe queues for incoming data
q_agg = queue.Queue(maxsize=10000)
q_alert = queue.Queue(maxsize=10000)

def consumer_loop(topic, q):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=brokers.split(","),
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    while True:
        for msg in consumer:
            q.put(msg.value)
        time.sleep(0.3)

threading.Thread(target=consumer_loop, args=(agg_topic, q_agg), daemon=True).start()
threading.Thread(target=consumer_loop, args=(alert_topic, q_alert), daemon=True).start()

# State
if "agg" not in st.session_state:
    st.session_state.agg = pd.DataFrame(columns=["road_id","window_end","veh_sum"])
if "alerts" not in st.session_state:
    st.session_state.alerts = pd.DataFrame(columns=["road_id","window_end","veh_sum","severity"])
if "last_sum" not in st.session_state:
    st.session_state.last_sum = {}  # road_id -> last veh_sum

# Sidebar
threshold = st.sidebar.number_input("Congestion threshold (info)", min_value=1, value=25)
st.sidebar.markdown("Źródło: **traffic_agg** i **congestion_alerts** z Kafki")

# Live updaters
def drain(q, df, cols):
    changed = False
    while not q.empty():
        item = q.get()
        df.loc[len(df)] = [item.get(c) for c in cols]
        changed = True
    return changed

col1, col2 = st.columns([2,1], gap="large")

with col2:
    st.subheader("🔔 Alerts (live)")
    placeholder_alerts = st.empty()

with col1:
    st.subheader("📈 Windowed vehicle count per road (1 min / slide 30s)")
    placeholder_chart = st.empty()

# Main loop
while True:
    changed_agg = drain(q_agg, st.session_state.agg, ["road_id","window_end","veh_sum"])
    changed_alert = drain(q_alert, st.session_state.alerts, ["road_id","window_end","veh_sum","severity"])

    if changed_agg:
        df = st.session_state.agg.copy()
        df["window_end"] = pd.to_datetime(df["window_end"])
        df = df.sort_values("window_end")

        # simple trend flag on dashboard: compare to previous point per road
        df["trend"] = ""
        for rid, grp in df.groupby("road_id"):
            prev = None
            idxs = grp.index.tolist()
            for i in idxs:
                cur = df.loc[i, "veh_sum"]
                if prev is not None:
                    df.loc[i, "trend"] = "⬆️" if cur > prev else ("⬇️" if cur < prev else "➡️")
                prev = cur

        st.session_state.agg = df

        # chart
        pivot = df.pivot_table(index="window_end", columns="road_id", values="veh_sum", aggfunc="last")
        placeholder_chart.line_chart(pivot)

    if changed_alert:
        al = st.session_state.alerts.copy()
        al["window_end"] = pd.to_datetime(al["window_end"])
        al = al.sort_values(["window_end","road_id"], ascending=[False, True]).head(15)
        al["info"] = al["road_id"] + " – " + al["window_end"].dt.strftime("%H:%M:%S") + \
                     " – sum=" + al["veh_sum"].astype(str) + " – " + al["severity"].str.upper()
        placeholder_alerts.table(al[["info"]])

    time.sleep(0.5)
