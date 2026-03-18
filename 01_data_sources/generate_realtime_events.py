import json
import random
import time
from datetime import datetime
from uuid import uuid4
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

def create_payment_transaction():
    """Định dạng 1: Công ty Payment Gateway (PAYMENT) - Khớp 100% Định dạng CIF doanh nghiệp"""
    return {
        "source_system": "PAYMENT",
        "event_id": str(uuid4()),
        "cif": f"CIF{random.randint(1, 1000):06d}", # 9 ký tự: CIF + 000XXX
        "card_bin": "450630XXXX",
        "merchant_name": fake.company(),
        "base_amount": round(random.uniform(50.0, 5000.0), 2),
        "currency": "USD",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def create_wallet_transaction():
    """Định dạng 2: Ứng dụng Ví điện tử (E-WALLET) - Khớp 100% Định dạng Wallet ID doanh nghiệp"""
    return {
        "source_system": "EWALLET",
        "event_id": str(uuid4()),
        "wallet_id": f"W{random.randint(500, 1500):06d}", # 7 ký tự: W + 000XXX
        "service_type": random.choice(["FOOD", "TAXI", "MOBILE_TOPUP"]),
        "promo_discount": round(random.uniform(1.0, 5.0), 2),
        "base_amount": round(random.uniform(5.0, 200.0), 2),
        "currency": "USD",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def run_producer():
    print("[!] Khởi động Producer: PAYMENT và EWALLET (Real-time Stream)...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_name = "realtime_transactions"
    
    try:
        while True:
            # Gửi ngẫu nhiên giữa Payment và E-Wallet
            txn = create_payment_transaction() if random.random() > 0.5 else create_wallet_transaction()
            producer.send(topic_name, value=txn)
            print(f">>> Sent from {txn['source_system']}: ID {txn['event_id'][:8]} | Amount: {txn['base_amount']}")
            time.sleep(random.uniform(0.1, 1.0))
    except KeyboardInterrupt:
        print("\n[!] Dừng Generator.")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
