from vnstock import stock_historical_data
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os

# 1. BẢO MẬT: Lấy URL từ biến môi trường (Không ghi lộ mật khẩu ra đây nữa)
DB_URL = os.environ.get("DB_URL")

if not DB_URL:
    raise ValueError("❌ Không tìm thấy DB_URL! Hãy check lại GitHub Secrets.")

print("🚀 Đang khởi động Pipeline...")

try:
    # --- PHASE 1: EXTRACT (Cào Data FPT) ---
    start_date = "2024-01-01"
    today = datetime.now().strftime("%Y-%m-%d")
    
    print(f"📥 Đang lấy dữ liệu FPT từ {start_date} đến {today}...")
    df = stock_historical_data(symbol='FPT', start_date=start_date, end_date=today, resolution='1D', type='stock')
    df.rename(columns={'time': 'date'}, inplace=True)
    print("✅ Đã lấy xong data FPT!")

    # --- PHASE 2 & 3: LOAD TO CLOUD DATABASE ---
    print("☁️ Đang kết nối lên Cloud Database (Neon)...")
    engine = create_engine(DB_URL)
    
    # Dùng 'append' để nối thêm data mới vào ngày hôm sau thay vì xóa đi viết lại ('replace')
    df.to_sql('Fact_Stock_Prices', engine, if_exists='append', index=False)
    
    print("🎉 BÙM! TẤT CẢ DATA FPT ĐÃ ĐƯỢC BƠM LÊN CLOUD THÀNH CÔNG!")

except Exception as e:
    print(f"❌ Toang rồi bro, lỗi đây: {e}")