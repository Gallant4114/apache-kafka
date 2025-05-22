from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Create Kafka admin client
admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092'
)

# Define topics
topics = [
    NewTopic(
        name='sensor-suhu-gudang',
        num_partitions=3,
        replication_factor=1
    ),
    NewTopic(
        name='sensor-kelembaban-gudang',
        num_partitions=3,
        replication_factor=1
    )
]

print("🔧 Creating Kafka topics...")

try:
    # Create topics
    admin_client.create_topics(topics)
    print("✅ Topics created successfully:")
    print("   - sensor-suhu-gudang")
    print("   - sensor-kelembaban-gudang")

except TopicAlreadyExistsError:
    print("ℹ️  Topics already exist:")
    print("   - sensor-suhu-gudang")
    print("   - sensor-kelembaban-gudang")

except Exception as e:
    print(f"❌ Error creating topics: {e}")

# List existing topics
try:
    existing_topics = admin_client.list_topics()
    print(f"\n📋 All existing topics: {list(existing_topics)}")
except Exception as e:
    print(f"❌ Error listing topics: {e}")

admin_client.close()
print("✅ Topic creation completed.")