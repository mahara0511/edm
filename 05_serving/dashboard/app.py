import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine
import time

# Cấu hình giao diện Streamlit
st.set_page_config(page_title="Lakehouse End-to-End Demo", layout="wide")

# Thông tin kết nối nội bộ trong mạng Docker
DB_URL = "postgresql://edm_user:edm_pass@host.docker.internal:5433/dw_edm"
FASTAPI_URL = "http://fastapi:8000"

st.sidebar.title("Lakehouse Menu")
st.sidebar.markdown("Điều hướng qua lại giữa các tầng kiến trúc:")
page = st.sidebar.radio("Chọn Demo:", [
    "1. Tổng quan Kiến trúc (Flow)", 
    "2. Giám sát luồng Real-time", 
    "3. Trải nghiệm AI (Fraud Detection)"
])

st.sidebar.markdown("---")
st.sidebar.info("Đây là ứng dụng Streamlit đóng vai trò Frontend trình diễn mô hình End-to-end cho Data Lakehouse Platform.")

if page == "1. Tổng quan Kiến trúc (Flow)":
    st.title("Tổng quan Luồng kiến trúc Data Lakehouse")
    st.markdown("### Sự di chuyển của Dữ liệu (Medallion Data Engineering & MLOps)")
    
    st.markdown("""
    **Bước 1. Lớp Ingestion (Thu thập sự kiện):** 
    > Dữ liệu giao dịch giả lập của Ngân hàng / E-wallet liên tục đổ về **Apache Kafka** stream bus.
    
    **Bước 2. Lớp Processing (Xử lý đa luồng):** 
    > Động cơ **Apache Spark** hứng stream data ngay lập tức từ Kafka. Nó làm sạch và phân phối: một nhánh đổ xuống Data Lake (S3), nhánh còn lại ghi thẳng vào Operational DW (Postgres) tốc độ cao.
    > Hệ thống **Apache Airflow** thì chạy theo ca đêm (Batch job) để biến đối các chuẩn thiết kế CSDL vận hành (OLTP) sang Star Schema Data Warehouse.
    
    **Bước 3. Lớp Storage (Hồ/Kho dữ liệu):**
    > **MinIO (S3)** chứa Cold Data; **PostgreSQL** gánh Hot Analytical Data.
    
    **Bước 4. Lớp Serving (Phân phối dịch vụ):**
    > Các chỉ số kinh doanh được ghim lên **Metabase BI Dashboard** (Cổng 3000).
    > Data Scientist sử dụng **Jupyter Notebook** đọc dòng lịch sử dữ liệu để Train ra Model Trí tuệ Nhân tạo.
    > Model đó được đưa lên chạy Real-time inference bằng **FastAPI** phục vụ cho **Streamlit UI**!
    """)
    st.image('https://raw.githubusercontent.com/streamlit/streamlit/develop/docs/images/streamlit-logo-secondary.png', width=150)

elif page == "2. Giám sát luồng Real-time":
    st.title("Màn hình Giám sát Storage Dữ liệu Nóng (Live)")
    st.markdown("Dữ liệu Streaming ngay lúc này rơi vào CSDL `dw_edm` bảng `public.realtime_transactions` (bơm tự động bằng Spark).")
    
    if st.button("Làm mới DB (Refresh Data)"):
        pass

    try:
        from sqlalchemy import text
        engine = create_engine(DB_URL)
        
        # Đảm bảo bảng đã tồn tại trước khi Query (Phòng trường hợp Spark chưa kịp tạo hoặc Database mới toanh)
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.realtime_transactions (
                    id              SERIAL          PRIMARY KEY,
                    event_id        VARCHAR(100),
                    account_id      INT,
                    amount          NUMERIC(18,2),
                    txn_type        VARCHAR(30),
                    currency        VARCHAR(10),
                    timestamp       TIMESTAMP,
                    merchant        VARCHAR(255),
                    is_fraud_flag   INT
                );
            """))
            query = "SELECT * FROM public.realtime_transactions ORDER BY timestamp DESC LIMIT 50"
            df = pd.read_sql(query, conn)
        
        if not df.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("Giao dịch gần nhất", len(df) if len(df) < 50 else f">= {len(df)}")
            col2.metric("Số tiền mới nhất", f"${df['amount'].iloc[0]:,.2f}")
            col3.metric("Số lượng Cảnh báo Cắm cờ Fraud", int(df['is_fraud_flag'].sum()))

            st.dataframe(df, use_container_width=True)
            
            # Vẽ một chart dòng chảy tiền ngắn
            st.markdown("### Khối lượng giao dịch luồng")
            st.area_chart(df.set_index('timestamp')['amount'])
        else:
            st.warning("Database trống! Cần bật Kafka và Spark (2 terminal) để đẩy luồng vào DB.")
    except Exception as e:
        st.error(f"Lỗi nối Postgres DB: {e}. Vui lòng kiểm tra file start_realtime_demo.ps1")

elif page == "3. Trải nghiệm AI (Fraud Detection)":
    st.title("AI Serving Inference (Dự đoán Gian Lận)")
    st.markdown("Giao diện gọi Cổng phân phối Model Machine Learning **FastAPI** mà Data Scientist vừa huấn luyện (Mô hình Random Forest).")
    
    with st.form("fraud_form"):
        col1, col2 = st.columns(2)
        with col1:
            account_id = st.number_input("ID Tài khoản", value=404, min_value=1)
            txn_type = st.selectbox("Loại Hình Giao Dịch", ["PAYMENT", "TRANSFER", "TOP_UP", "WITHDRAWAL"])
        with col2:
            amount = st.number_input("Số tiền GD ($)", value=3500.0, step=10.0)
            merchant = st.text_input("Tên Đối Tác (Merchant)", value="UNKNOWN_VENDOR")
        
        submitted = st.form_submit_button("Thực thi Phân Tích Rủi Ro")
        
    if submitted:
        payload = {
            "account_id": int(account_id),
            "amount": float(amount),
            "txn_type": txn_type,
            "merchant": merchant
        }
        
        with st.spinner("AI đang quét truy vết..."):
            time.sleep(0.5) # Fake loading for effect
            try:
                # Gọi sang FastAPI nội bộ
                res = requests.post(f"{FASTAPI_URL}/predict_fraud", json=payload)
                if res.status_code == 200:
                    data = res.json()
                    
                    risk_pct = data['risk_score'] * 100
                    st.markdown("### Kết Quả AI Phán Xét:")
                    
                    if data['is_fraud']:
                        st.error(f"PHÁT HIỆN GIAN LẬN (CÚ RÚT TIỀN RỦI RO CAO) | Điểm Rủi Ro: {risk_pct:.1f}%")
                    else:
                        st.success(f"GIAO DỊCH AN TOÀN | Điểm Rủi Ro: {risk_pct:.1f}%")
                    
                    st.markdown("---")
                    st.markdown("*(Log chi tiết JSON máy học trả về)*")
                    st.json(data)
                else:
                    st.error("FastAPI từ chối kết nối / Lỗi Serve AI.")
            except requests.exceptions.ConnectionError:
                st.error("Không thể kết nối FastAPI. Đảm bảo container 'lakehouse-fastapi' đã được up và lắng nghe cổng 8000.")
