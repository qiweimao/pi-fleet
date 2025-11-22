import os
import json
import time
import socket
import random
import threading

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

DEVICE_ID = os.getenv("DEVICE_ID")
if not DEVICE_ID:
    # default to hostname if not set
    DEVICE_ID = socket.gethostname()

sampling_interval_sec = 5  # default, can be changed via config

# Event to signal when MQTT is connected
mqtt_connected = threading.Event()


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[{DEVICE_ID}] Connected to MQTT with rc={rc}")
        # Listen for config just for this device
        topic = f"devices/{DEVICE_ID}/config"
        client.subscribe(topic)
        print(f"[{DEVICE_ID}] Subscribed to {topic}")
        mqtt_connected.set()  # Signal that connection is established
    else:
        print(f"[{DEVICE_ID}] Failed to connect to MQTT with rc={rc}")


def on_disconnect(client, userdata, rc):
    print(f"[{DEVICE_ID}] Disconnected from MQTT with rc={rc}")
    mqtt_connected.clear()


def on_message(client, userdata, msg):
    global sampling_interval_sec
    print(f"[{DEVICE_ID}] MQTT msg {msg.topic}: {msg.payload.decode(errors='ignore')}")
    if msg.topic.endswith("/config"):
        try:
            cfg = json.loads(msg.payload.decode())
            sampling_interval_sec = int(cfg.get("sampling_interval_sec", sampling_interval_sec))
            print(f"[{DEVICE_ID}] Updated sampling_interval_sec={sampling_interval_sec}")
        except Exception as e:
            print(f"[{DEVICE_ID}] Failed to parse config: {e}")


client = mqtt.Client(client_id=DEVICE_ID)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message


def mqtt_loop():
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            print(f"[{DEVICE_ID}] Attempting to connect to MQTT at {MQTT_HOST}:{MQTT_PORT} (attempt {attempt + 1}/{max_retries})")
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.loop_forever()
            break
        except Exception as e:
            print(f"[{DEVICE_ID}] Connection attempt failed: {e}")
            if attempt < max_retries - 1:
                print(f"[{DEVICE_ID}] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"[{DEVICE_ID}] Max retries reached. Exiting.")
                raise


def telemetry_loop():
    # Wait for MQTT connection before starting telemetry
    print(f"[{DEVICE_ID}] Waiting for MQTT connection...")
    if not mqtt_connected.wait(timeout=30):
        print(f"[{DEVICE_ID}] Timeout waiting for MQTT connection. Exiting.")
        return
    
    print(f"[{DEVICE_ID}] MQTT connected. Starting telemetry loop...")
    while True:
        # Only publish if connected
        if mqtt_connected.is_set() and client.is_connected():
            # fake sensor values
            temp = 20.0 + random.random() * 5
            pressure = 100.0 + random.random() * 10
            payload = {
                "device_id": DEVICE_ID,
                "temp_c": round(temp, 2),
                "pressure_kpa": round(pressure, 2),
                "sampling_interval_sec": sampling_interval_sec,
            }
            topic_status = f"devices/{DEVICE_ID}/status"
            topic_telemetry = f"devices/{DEVICE_ID}/telemetry"
            client.publish(topic_status, json.dumps({"alive": True}))
            client.publish(topic_telemetry, json.dumps(payload))
            print(f"[{DEVICE_ID}] Sent telemetry: {payload}")
        else:
            print(f"[{DEVICE_ID}] MQTT not connected, skipping telemetry publish")
        time.sleep(sampling_interval_sec)


threading.Thread(target=mqtt_loop, daemon=True).start()

if __name__ == "__main__":
    telemetry_loop()
