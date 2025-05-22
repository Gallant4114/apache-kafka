# Problem Based Learning : Apache Kafka
Gallant Damas Hayuaji (5027231037)

Big Data dan Data Lakehouse (B)
***
## Setup Apache Kafka dan PySpark di WSL

**Prerequisites**
- Docker Desktop
- WSL
- Python 3.8+
- Java 8+ (untuk Spark)

**A. Persiapan Sistem WSL**
1. Update sistem Ubuntu di WSL
```bash
sudo apt update && sudo apt upgrade -y
```

2. Install Java
```bash
sudo apt install openjdk-17-jdk -y
```

```bash
# Set JAVA_HOME untuk Java 17
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
source ~/.bashrc

# Verifikasi instalasi Java
java -version
```

![image](https://github.com/user-attachments/assets/aacedfd5-d5a2-465b-8ad2-1cba60677e0c)

3. Install Python dan pip
```bash
sudo apt install python3 python3-pip -y
```

```bash
pip3 install kafka-python pyspark findspark
```

![image](https://github.com/user-attachments/assets/cfe15386-be23-44b0-b295-899b80cc11c7)

**B. Instalasi Apache Kafka**
1. Download dan Setup Kafka
```bash
# Download kafka
wget https://downloads.apache.org/kafka/4.0.0/kafka_2.13-4.0.0.tgz

# Extract file
tar -xzf kafka_2.13-4.0.0.tgz
cd kafka_2.13-4.0.0

# Set environment variables untuk Kafka 4.0.0
echo 'export KAFKA_HOME=~/kafka/kafka_2.13-4.0.0' >> ~/.bashrc
echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
source ~/.bashrc

# Verifikasi instalasi
ls -la $KAFKA_HOME/bin/
```

![image](https://github.com/user-attachments/assets/716b79eb-581a-40a9-8397-efab9fa14677)
***
**C. Start Kafka dengan Docker Desktop**
```bash
# Start containers
docker-compose up -d

# Verify containers running
docker ps
```

![image](https://github.com/user-attachments/assets/3d0e1342-e088-4e3c-a6cc-76140c69e762)

**D. Create Kafka Topics**
```bash
python3 create_topics.py
```

![image](https://github.com/user-attachments/assets/f5c0e36f-0831-4ed3-ae72-cee8b7cc30d7)

**F. Menjalankan Producer**
```bash
# Terminal 2: temperature producer
python3 producer_suhu.py
```

![image](https://github.com/user-attachments/assets/ff61c11b-6e4a-4eb6-89cf-eb3eead73db4)

```bash
# Terminal 3: humidity producer  
python3 producer_kelembaban.py
```

![image](https://github.com/user-attachments/assets/d754f384-cf9e-4d93-844f-a94bb7241f54)

**G. Menjalankan PySpark Consumer**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer.py
```

![image](https://github.com/user-attachments/assets/68bb9645-bd21-4fdd-9150-ba169a1a01a7)

Output yang ditampilkan berdasarkan kondisi gudang.

![image](https://github.com/user-attachments/assets/ae72d526-a750-4b51-be2c-15d8c39e5a49)

Peringatan kelembaban:

![image](https://github.com/user-attachments/assets/11e57a78-139b-490a-afcc-4512b91e1713)

Peringatan suhu:

![image](https://github.com/user-attachments/assets/4b796e4a-f22b-4c84-9079-b23641138b01)

Output gabungan:

![image](https://github.com/user-attachments/assets/b56bb642-87e7-4845-9dc6-d8e020516d7a)

![image](https://github.com/user-attachments/assets/560b60d1-4c76-4361-96cd-cc53af1c95ff)
***
