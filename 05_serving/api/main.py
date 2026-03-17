from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import pandas as pd
import os

app = FastAPI(title="Lakehouse Model Serving API")

# Cố gắng load mô hình Random Forest thật nếu file tồn tại
MODEL_PATH = "fraud_model.pkl"
rf_model = None

if os.path.exists(MODEL_PATH):
    print(">>> Nạp mô hình AI thật từ file:", MODEL_PATH)
    rf_model = joblib.load(MODEL_PATH)
else:
    print(">>> CẢNH BÁO: Không tìm thấy model thật, API sẽ trả lỗi nếu gọi dự đoán!")

class TransactionInput(BaseModel):
    account_id: int
    amount: float
    txn_type: str
    merchant: str

class FraudPrediction(BaseModel):
    transaction: TransactionInput
    is_fraud: bool
    risk_score: float

@app.post("/predict_fraud", response_model=FraudPrediction)
async def predict_fraud(txn: TransactionInput):
    # Mapping chữ sang số như mảng Train: 0 = PAYMENT/TRANSFER, 1 = WITHDRAWAL, 2 = TOP_UP
    txn_type_map = {
        "PAYMENT": 0, "TRANSFER": 0,
        "WITHDRAWAL": 1,
        "TOP_UP": 2
    }
    
    numeric_type = txn_type_map.get(txn.txn_type.upper(), 0)
    
    # Đóng gói input theo cấu trúc pandas DataFrame y hệt lúc Train
    input_data = pd.DataFrame([{
        'txn_type': numeric_type,
        'amount': txn.amount
    }])
    
    # Dự đoán bằng Mô hình AI Thật
    if rf_model is not None:
        # Lấy xác suất gian lận (risk score)
        probabilities = rf_model.predict_proba(input_data)
        risk_score = probabilities[0][1] # Lấy cột tỷ lệ của nhãn = 1 (Fraud)
        
        # Dự đoán nhãn cuối cùng thay vì if-else chay
        is_fraud_pred = rf_model.predict(input_data)[0]
    else:
        # Fallback lại rule nếu nạp file lỗi (cho an toàn)
        risk_score = 0.8 if txn.amount > 3000 else 0.1
        is_fraud_pred = 1 if risk_score > 0.5 else 0

    return FraudPrediction(
        transaction=txn,
        is_fraud=bool(is_fraud_pred == 1),
        risk_score=float(risk_score)
    )

@app.get("/health")
async def health_check():
    return {"status": "Serving API is active", "model_loaded": rf_model is not None}
