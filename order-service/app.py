import json
import requests
from datetime import datetime

MICROCKS_KAFKA_ENDPOINT = "http://localhost:8080/api/events"

def publish_order_created():
    event = {
        "orderId": "ORD-123",
        "customerId": "CUST-456",
        "amount": 250.75,
        "createdAt": datetime.utcnow().isoformat()
    }

    response = requests.post(
        MICROCKS_KAFKA_ENDPOINT,
        json={
            "channel": "order.created",
            "payload": event
        }
    )

    print("Status:", response.status_code)
    print("Response:", response.text)

if __name__ == "__main__":
    publish_order_created()
