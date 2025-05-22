#!/bin/bash

echo "🚀 Starting PySpark Kafka Consumer..."
echo "📋 Installing required packages..."

# Install required Python packages
pip install pyspark kafka-python

echo "✅ Packages installed successfully!"
echo "🔧 Starting Spark application..."

# Run PySpark with Kafka packages
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    consumer.py

echo "🛑 Spark application stopped."