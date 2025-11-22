import os
import json
import threading
import time
from typing import Dict

import paho.mqtt.client as mqtt
from fastapi import FastAPI
from pydantic import BaseModel

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

app = FastAPI(title="Fleet Backend")

mqtt_client = mqtt.Client()
mqtt_connected = threading.Event()

# ---- MQTT callbacks ----
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Backend connected to MQTT with result code", rc)
        # Subscribe to all device status messages
        client.subscribe("devices/+/status")
        mqtt_connected.set()
    else:
        print(f"Backend failed to connect to MQTT with result code {rc}")

def on_disconnect(client, userdata, rc):
    print(f"Backend disconnected from MQTT with rc={rc}")
    mqtt_connected.clear()

def on_message(client, userdata, msg):
    print(f"[MQTT] {msg.topic}: {msg.payload.decode(errors='ignore')}")

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message

def mqtt_loop():
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            print(f"Backend attempting to connect to MQTT at {MQTT_HOST}:{MQTT_PORT} (attempt {attempt + 1}/{max_retries})")
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
            mqtt_client.loop_forever()
            break
        except Exception as e:
            print(f"Backend connection attempt failed: {e}")
            if attempt < max_retries - 1:
                print(f"Backend retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Backend max retries reached. Continuing without MQTT...")

threading.Thread(target=mqtt_loop, daemon=True).start()

# Wait for MQTT connection before starting server (with timeout)
print("Backend waiting for MQTT connection...")
mqtt_connected.wait(timeout=30)
if mqtt_connected.is_set():
    print("Backend MQTT connected. Starting API server...")
else:
    print("Backend MQTT connection timeout. Starting API server anyway (publish may fail)...")

# ---- API models ----
class ConfigBody(BaseModel):
    sampling_interval_sec: int = 5

# ---- HTTP endpoints ----
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/devices/{device_id}/config")
def set_config(device_id: str, body: ConfigBody):
    if not mqtt_connected.is_set() or not mqtt_client.is_connected():
        return {"error": "MQTT not connected", "sent_to": device_id, "config": None}
    
    topic = f"devices/{device_id}/config"
    payload: Dict = {
        "device_id": device_id,
        "sampling_interval_sec": body.sampling_interval_sec,
    }
    result = mqtt_client.publish(topic, json.dumps(payload))
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"Sent config to {device_id}: {payload}")
        return {"sent_to": device_id, "config": payload}
    else:
        return {"error": f"Failed to publish: {result.rc}", "sent_to": device_id, "config": None}
