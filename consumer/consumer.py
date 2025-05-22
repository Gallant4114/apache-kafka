from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

print("🚀 Initializing Spark Session...")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("GudangSensorMonitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session initialized successfully!")

# schema untuk sensor
suhu_schema = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("suhu", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

kelembaban_schema = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("kelembaban", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

print("📡 Connecting to Kafka streams...")

# Read from Kafka topics
try:
    suhu_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-suhu-gudang") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    kelembaban_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-kelembaban-gudang") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("✅ Successfully connected to Kafka topics!")

except Exception as e:
    print(f"❌ Error connecting to Kafka: {e}")
    spark.stop()
    exit(1)

# Parse JSON data
print("🔄 Setting up data parsing...")

suhu_parsed = suhu_df.select(
    from_json(col("value").cast("string"), suhu_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

kelembaban_parsed = kelembaban_df.select(
    from_json(col("value").cast("string"), kelembaban_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# Add watermark for windowing (using Kafka timestamp)
suhu_with_watermark = suhu_parsed.withWatermark("kafka_timestamp", "30 seconds")
kelembaban_with_watermark = kelembaban_parsed.withWatermark("kafka_timestamp", "30 seconds")

# Filter for warnings
suhu_warnings = suhu_with_watermark.filter(col("suhu") > 80)
kelembaban_warnings = kelembaban_with_watermark.filter(col("kelembaban") > 70)

print("⚙️ Setting up alert processors...") # Function to process suhu warnings
def process_suhu_warnings(df, epoch_id):
    if df.count() > 0:
        print("\n" + "🔥" * 15)
        print("=== PERINGATAN SUHU TINGGI ===")
        for row in df.collect():
            print(f"[Peringatan Suhu Tinggi]")
            print(f"Gudang {row['gudang_id']}: Suhu {row['suhu']}°C")
        print("=" * 30)

# Function to process kelembaban warnings
def process_kelembaban_warnings(df, epoch_id):
    if df.count() > 0:
        print("\n" + "💧" * 15)
        print("=== PERINGATAN KELEMBABAN TINGGI ===")
        for row in df.collect():
            print(f"[Peringatan Kelembaban Tinggi]")
            print(f"Gudang {row['gudang_id']}: Kelembaban {row['kelembaban']}%")
        print("=" * 35)

# Join streams for critical warnings (10-second window)
print("🔗 Setting up stream joining...")

joined_df = suhu_with_watermark.alias("s").join(
    kelembaban_with_watermark.alias("k"),
    expr("""
        s.gudang_id = k.gudang_id AND
        s.kafka_timestamp >= k.kafka_timestamp - INTERVAL 10 SECONDS AND
        s.kafka_timestamp <= k.kafka_timestamp + INTERVAL 10 SECONDS
    """),
    "inner"
).select(
    col("s.gudang_id").alias("gudang_id"),
    col("s.suhu").alias("suhu"),
    col("k.kelembaban").alias("kelembaban"),
    col("s.kafka_timestamp").alias("timestamp")
)

# Function to process combined warnings
def process_combined_warnings(df, epoch_id):
    if df.count() > 0:
        print("\n" + "🚨" * 25)
        print("=== LAPORAN STATUS GUDANG ===")
        print("🚨" * 25)

        rows = df.collect()
        for row in rows:
            gudang_id = row['gudang_id']
            suhu = row['suhu']
            kelembaban = row['kelembaban']

            # Determine status
            if suhu > 80 and kelembaban > 70:
                status = "PERINGATAN KRITIS - Bahaya tinggi! Barang berisiko rusak"
                print(f"\n🔴 [PERINGATAN KRITIS]")
            elif suhu > 80:
                status = "Suhu tinggi, kelembaban normal"
                print(f"\n🟡 [PERINGATAN SUHU]")
            elif kelembaban > 70:
                status = "Kelembaban tinggi, suhu aman"
                print(f"\n🔵 [PERINGATAN KELEMBABAN]")
            else:
                status = "Aman"
                print(f"\n🟢 [STATUS NORMAL]")

            print(f"Gudang {gudang_id}:")
            print(f"- Suhu: {suhu}°C")
            print(f"- Kelembaban: {kelembaban}%")
            print(f"- Status: {status}")

        print("🚨" * 25)

print("🎯 Starting streaming queries...")

# Start streaming queries with error handling
try:
    suhu_query = suhu_warnings.writeStream \
        .foreachBatch(process_suhu_warnings) \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .option("checkpointLocation", "/tmp/spark-checkpoint-suhu") \
        .start()

    kelembaban_query = kelembaban_warnings.writeStream \
        .foreachBatch(process_kelembaban_warnings) \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .option("checkpointLocation", "/tmp/spark-checkpoint-kelembaban") \
        .start()

    combined_query = joined_df.writeStream \
        .foreachBatch(process_combined_warnings) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .option("checkpointLocation", "/tmp/spark-checkpoint-combined") \
        .start()

    print("🚀 Memulai monitoring gudang...")
    print("📊 Menganalisis data sensor suhu dan kelembaban...")
    print("⚠️  Sistem akan memberikan peringatan jika:")
    print("   - Suhu > 80°C")
    print("   - Kelembaban > 70%")
    print("🔥 Peringatan KRITIS jika kedua kondisi terpenuhi!")
    print("-" * 50)
    print("✅ Monitoring aktif! Menunggu data sensor...")
    print("💡 Tekan Ctrl+C untuk menghentikan monitoring")
    print("-" * 50)

    # Wait for termination
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    print("\n🛑 Menghentikan monitoring...")

except Exception as e:
    print(f"\n❌ Error dalam streaming: {e}")

finally:
    # Clean shutdown
    try:
        if 'suhu_query' in locals():
            suhu_query.stop()
        if 'kelembaban_query' in locals():
            kelembaban_query.stop()
        if 'combined_query' in locals():
            combined_query.stop()
    except:
        pass

    spark.stop()
    print("✅ Monitoring dihentikan dengan aman.")