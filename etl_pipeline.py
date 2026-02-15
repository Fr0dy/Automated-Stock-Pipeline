from vnstock import stock_historical_data
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
import time  # Thêm cái này để bắt con Bot phải "thở"

# 1. BẢO MẬT: Lấy URL từ két sắt GitHub
DB_URL = os.environ.get("DB_URL")

if not DB_URL:
    raise ValueError("❌ Không tìm thấy DB_URL! Hãy check lại GitHub Secrets.")

print("🚀 Đang khởi động Pipeline VN30...")

# Danh sách VN30
vn30_tickers = [
    'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 
    'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI', 'STB', 
    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'
]

try:
    start_date = "2024-01-01"
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Tạo một cái rổ rỗng cực to để chứa data của vài thằng
    all_data = pd.DataFrame()
    
    print(f"📥 Đang lệnh cho Bot cào {len(vn30_tickers)} mã. Anh em đứng lùi ra...")

    # VÒNG LẶP: Lùa Bot đi cào từng thằng một
    for ticker in vn30_tickers:
        print(f"⏳ Đang móc data của: {ticker}...")
        df = stock_historical_data(symbol=ticker, start_date=start_date, end_date=today, resolution='1D', type='stock')
        
        # Nếu có data thì nhét vào rổ to
        if not df.empty:
            df.rename(columns={'time': 'date'}, inplace=True)
            # Nối data thằng này vào đít thằng trước
            all_data = pd.concat([all_data, df], ignore_index=True)
            
        # QUAN TRỌNG: Ngủ 1 giây để lừa nhà mạng, chống bị khóa IP
        time.sleep(1)
        
    print(f"✅ Đã gom xong data! Tổng cộng có {len(all_data)} dòng.")

    # --- PHASE 2 & 3: BƠM 1 PHÁT LÊN CLOUD ---
    print("☁️ Đang mở ống nước kết nối lên Neon Database...")
    engine = create_engine(DB_URL)
    
    # Bơm cả cái rổ to lên Cloud (Dùng append để giữ data cũ)
    all_data.to_sql('Fact_Stock_Prices', engine, if_exists='append', index=False)
    
    print(f"🎉 BÙM! TOÀN BỘ DATA VN30 ĐÃ VÀO KHO AN TOÀN!")

except Exception as e:
    print(f"❌ Toang rồi ông giáo ơi, lỗi đây: {e}")
