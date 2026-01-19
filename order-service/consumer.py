import json
from kafka import KafkaConsumer
from datetime import datetime

# LOCAL Microcks Kafka (no SSL needed)
KAFKA_BROKER = 'localhost:19092'  # External port for your laptop
TOPIC_NAME = 'TemperatureEventsAPI-1.3.0-temperature.measured' # <--- MATCH 1.3.0

def start_consumer():
    print("=" * 60)
    print("🚀 ZERO QA DEMO - MICROCKS LOCAL SETUP")
    print("=" * 60)
    print(f"\n📡 Connecting to: {KAFKA_BROKER} (local)")
    print(f"📬 Topic: {TOPIC_NAME}")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "─" * 60)

    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            # NO SSL for local setup
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        
        print("✅ CONNECTED! Waiting for mock events from Microcks...")
        print("💡 Expecting events every 3 seconds (as configured in AsyncAPI)")
        print("🛑 Press Ctrl+C to stop\n")
        print("─" * 60)

        event_count = 0
        for message in consumer:
            event_count += 1
            event = message.value
            
            # Display received event
            print(f"\n🌡️  EVENT #{event_count} RECEIVED:")
            print(f"   ├─ Sensor ID:  {event.get('sensorId')}")
            print(f"   ├─ Temperature: {event.get('value')}°{event.get('unit')}")
            print(f"   ├─ Measured At: {event.get('measuredAt')}")
            print(f"   └─ Timestamp:  {datetime.now().strftime('%H:%M:%S')}")
            
            # Validate event structure (Zero QA validation)
            validate_event(event, event_count)
            print("─" * 60)

    except KeyboardInterrupt:
        print(f"\n\n🛑 Consumer stopped by user")
        print(f"📊 Total events received: {event_count}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\n🔍 TROUBLESHOOTING:")
        print("   1. Check if mocks are enabled in Microcks UI (http://localhost:8080)")
        print("   2. Verify all containers are running: docker ps")
        print("   3. Check async-minion logs: docker logs microcks-async-minion")
    finally:
        print("\n✅ Consumer closed")

def validate_event(event, count):
    """Zero QA validation - automatically validates event structure"""
    required_fields = ['sensorId', 'value', 'unit', 'measuredAt']
    missing = [f for f in required_fields if f not in event]
    
    if missing:
        print(f"   ⚠️  VALIDATION FAILED: Missing fields {missing}")
    else:
        print(f"   ✅ VALIDATION PASSED: All required fields present")
    
    # Validate unit enum
    if event.get('unit') not in ['C', 'F']:
        print(f"   ⚠️  VALIDATION WARNING: Invalid unit '{event.get('unit')}'")

if __name__ == "__main__":
    start_consumer()