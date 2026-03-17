import os
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

MODEL_PATH = "fraud_model.pkl"

def train_basic_model():
    """Huấn luyện mô hình từ dữ liệu THẬT trong PostgreSQL"""
    print(">>> Bắt đầu nạp dữ liệu từ Database để học...")
    
    # Kết nối Database Warehouse để lấy data giao dịch
    try:
        from sqlalchemy import create_engine
        engine = create_engine("postgresql://edm_user:edm_pass@host.docker.internal:5433/dw_edm")
        
        # Đọc dữ liệu từ bảng ví điện tử có sẵn (ewallet.transaction)
        # Bảng này ta xem như những giao dịch có status FAILED là Fraud (bị chặn) để làm nhãn Học
        query = """
            SELECT txn_type, txn_amount AS amount, 
                   CASE WHEN txn_status = 'FAILED' THEN 1 ELSE 0 END AS is_fraud 
            FROM ewallet.transaction
        """
        df = pd.read_sql(query, engine)
        print(f">>> Đã lấy được {len(df)} dòng dữ liệu giao dịch thật từ Database!")
        
        if len(df) < 50:
            print(">>> Dữ liệu trong Database có vẻ hơi ít để nặn ra AI thông minh. Nhưng vẫn sẽ tiến hành train...")

    except Exception as e:
        print(f"LỖI KẾT NỐI DB: {e}. Đảm bảo cụm Postgres đã bật!")
        return

    # Chuẩn hoá dữ liệu chữ (Label Encoding) để máy tính hiểu
    # VD: PAYMENT -> 0, WITHDRAWAL -> 1
    type_map = {
        "PAYMENT": 0, "TRANSFER": 0,
        "WITHDRAWAL": 1, "TOP_UP": 2
    }
    df['txn_type'] = df['txn_type'].map(type_map).fillna(0)
    
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
