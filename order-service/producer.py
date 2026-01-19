import json
import time
from confluent_kafka import Producer

# CRITICAL: Since we are on Windows, we MUST use localhost
KAFKA_BROKER = 'localhost:19092'
TOPIC_NAME = 'TemperatureEventsAPI-1.3.0-temperature.measured'

def delivery_report(err, msg):
    if err:
        print(f'❌ Delivery failed: {err}')
    else:
        print(f'✅ Delivered to {msg.topic()} [{msg.partition()}]')

def run_demo():
    print(f"--- 🚀 PRODUCER STARTING ---")
    print(f"Target: {KAFKA_BROKER}")
    
    p = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'socket.timeout.ms': 10000,
        'api.version.request': True 
    })

    # VALID DATA
    event = {
        "sensorId": "sensor-1",
        "value": 24.5,
        "unit": "C",
        "measuredAt": "2026-01-18T12:00:00Z"
    }
    
    print("Broadcasting data... (Press Ctrl+C to stop)")
    try:
        # Loop forever so Microcks has plenty of time to catch it
        while True:
            p.produce(TOPIC_NAME, json.dumps(event).encode('utf-8'), callback=delivery_report)
            p.poll(0)
            time.sleep(0.5) # Send 2 messages per second
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        p.flush()

if __name__ == "__main__":
    run_demo()