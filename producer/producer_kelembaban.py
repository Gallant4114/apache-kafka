import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Gudang IDs
gudangs = ['G1', 'G2', 'G3']

print("💧 Starting Humidity Sensor Producer...")
print("📡 Sending humidity data every second...")
print("-" * 40)

try:
    while True:
        gudang = random.choice(gudangs)

        # Sometimes generate high humidity for alerts
        if random.random() < 0.3:  # 30% chance of high humidity
            kelembaban = random.randint(71, 95)  # High humidity
        else:
            kelembaban = random.randint(40, 70)  # Normal humidity
        current_time = datetime.now().strftime("%H:%M:%S")
        data = {
            "gudang_id": gudang,
            "kelembaban": kelembaban,
            "timestamp": current_time  # Add timestamp
        }

        producer.send('sensor-kelembaban-gudang', value=data)

        if kelembaban > 70:
            print(f"💦 [KELEMBABAN TINGGI] {data}")
        else:
            print(f"✅ [KELEMBABAN NORMAL] {data}")

        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Stopping humidity producer...")
    producer.close()
    print("✅ Humidity producer stopped.")