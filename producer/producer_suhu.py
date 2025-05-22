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

print("🌡️  Starting Temperature Sensor Producer...")
print("📡 Sending temperature data every second...")
print("-" * 40)

try:
    while True:
        gudang = random.choice(gudangs)

        # generate high temperature for alerts
        if random.random() < 0.3:  # 30% chance of high temperature
            suhu = random.randint(81, 90)  # High temperature
        else:
            suhu = random.randint(25, 79)  # Normal temperature

        current_time = datetime.now().strftime("%H:%M:%S")
        data = {
            "gudang_id": gudang,
            "suhu": suhu,
            "timestamp": current_time  # Add timestamp
        }

        producer.send('sensor-suhu-gudang', value=data)

        if suhu > 80:
            print(f"🔥 [SUHU TINGGI] {data}")
        else:
            print(f"✅ [SUHU NORMAL] {data}")

        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Stopping temperature producer...")
    producer.close()
    print("✅ Temperature producer stopped.")