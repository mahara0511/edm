import json
import time
import random
import threading
from uuid import uuid4
from datetime import datetime
from flask import Flask, render_template, jsonify
from faker import Faker
from kafka import KafkaProducer

app = Flask(__name__)
fake = Faker()

# Khởi tạo Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer connected!")
except Exception as e:
    producer = None
    print(f"Lưu ý: Không thể kết nối Kafka - {e}")

# Trạng thái script
state = {
    "is_running": False,
    "events": []
}

def generate_events_background():
    global state
    while True:
        if state["is_running"]:
            txn = {
                "event_id": str(uuid4()),
                "account_id": random.randint(1, 1000),
                "amount": round(random.uniform(10.0, 5000.0), 2),
                "txn_type": random.choice(["PAYMENT", "TRANSFER", "TOP_UP", "WITHDRAWAL"]),
                "currency": "VND",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "merchant": fake.company(),
                "is_fraud_flag": random.choices([0, 1], weights=[0.98, 0.02])[0]
            }
            
            # Đẩy lên Kafka hệt như code cũ
            if producer:
                try:
                    producer.send("realtime_transactions", value=txn)
                except Exception as e:
                    print("Error sending to Kafka:", e)
            
            # Clone một bản đưa vào state để UI lấy lên xem
            txn_ui = txn.copy()
            txn_ui['event_id_short'] = txn['event_id'][:8] + "..."
            txn_ui['time_short'] = datetime.utcnow().strftime("%H:%M:%S")
            
            state["events"].insert(0, txn_ui)
            if len(state["events"]) > 50: # Cho phép show tối đa 50 row mới nhất trên UI
                state["events"].pop()
                
        time.sleep(random.uniform(0.3, 1.2))

# Chạy ngầm thread đẩy data
thread = threading.Thread(target=generate_events_background, daemon=True)
thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/toggle', methods=['POST'])
def toggle():
    state["is_running"] = not state["is_running"]
    return jsonify({"status": "running" if state["is_running"] else "stopped"})

@app.route('/data', methods=['GET'])
def data():
    return jsonify({
        "is_running": state["is_running"],
        "events": state["events"]
    })

if __name__ == '__main__':
    print(">>> Giao diện sẵn sàng tại: http://localhost:5050")
    app.run(port=5050, debug=True, use_reloader=False)
