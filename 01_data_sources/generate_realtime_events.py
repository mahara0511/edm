import json
import time
import random
from uuid import uuid4
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# Cần cài thư viện: pip install kafka-python faker

fake = Faker()

def create_mock_transaction():
    """Tạo dữ liệu giao dịch giả lập (E-Wallet/Banking) theo đúng mô tả"""
    return {
        "event_id": str(uuid4()),
        "account_id": random.randint(1, 1000),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "txn_type": random.choice(["PAYMENT", "TRANSFER", "TOP_UP", "WITHDRAWAL"]),
        "currency": "VND",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "merchant": fake.company(),
        "is_fraud_flag": random.choices([0, 1], weights=[0.98, 0.02])[0] # 2% tỷ lệ lừa đảo
    }

def run_producer():
    print("Khởi động Kafka Producer. Nối tới broker ở localhost:29092...")
    # Broker port dựa trên cấu hình Docker (PLAINTEXT_HOST)
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_name = "realtime_transactions"
    print(f"Bắt đầu đẩy dữ liệu (Streaming) vào topic '{topic_name}'...")

    try:
        while True:
            txn = create_mock_transaction()
            producer.send(topic_name, value=txn)
            print(f">>> Đã gửi Event: {txn['event_id']} - {txn['txn_type']} - Amount: {txn['amount']}")
            # Nghỉ ngẫu nhiên 0.1 đến 1 giây để mô phỏng Real-time Event
            time.sleep(random.uniform(0.1, 1.0))
    except KeyboardInterrupt:
        print("\nĐã dừn Mock Bank Events Producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
