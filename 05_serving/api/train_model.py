import os
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

MODEL_PATH = "fraud_model.pkl"

def train_basic_model():
    """Huấn luyện một mô hình Machine Learning phát hiện giao dịch gian lận đơn giản"""
    print(">>> Bắt đầu tạo dữ liệu huấn luyện giả lập để học...")
    # Tạo 1000 dòng dữ liệu giả lập để làm ví dụ
    import numpy as np
    
    np.random.seed(42)
    n_samples = 1000
    
    # Giả định:
    # 0 = PAYMENT/TRANSFER, 1 = WITHDRAWAL, 2 = TOP_UP
    txn_types = np.random.choice([0, 1, 2], size=n_samples, p=[0.7, 0.2, 0.1])
    amounts = np.random.exponential(scale=500, size=n_samples) # Đa số khoản tiền nhỏ
    
    # Quy tắc tạo Label Gian lận (Is Fraud):
    # - Nếu số tiền quá lớn (> 3000) HOẶC (rút tiền + số tiền > 1000), tỷ lệ gian lận cao
    is_fraud = np.zeros(n_samples)
    for i in range(n_samples):
        prob = 0.01  # Tỷ lệ gian lận nền
        if amounts[i] > 3000:
            prob += 0.8
        if txn_types[i] == 1 and amounts[i] > 1000: 
            prob += 0.5
            
        is_fraud[i] = np.random.binomial(1, min(prob, 1.0))

    # Tạo DataFrame Pandas
    df = pd.DataFrame({
        'txn_type': txn_types,
        'amount': amounts,
        'is_fraud': is_fraud
    })
    
    X = df[['txn_type', 'amount']]
    y = df['is_fraud']
    
    print(">>> Đang phân chia tập Train / Test...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print(">>> Bắt đầu huấn luyện mô hình (Random Forest Classifier)...")
    model = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
    model.fit(X_train, y_train)
    
    accuracy = model.score(X_test, y_test)
    print(f">>> Độ chính xác trên tập Test: {accuracy * 100:.2f}%")
    
    # Xuất mô hình ra file Pickle
    joblib.dump(model, MODEL_PATH)
    print(f">>> Đã xuất mô hình máy học thành công ra file: {MODEL_PATH}")

if __name__ == "__main__":
    train_basic_model()
