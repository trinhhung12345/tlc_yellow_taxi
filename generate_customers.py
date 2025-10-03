import pandas as pd
import random
from faker import Faker
from tqdm import tqdm
from datetime import datetime, timedelta

# --- Cấu hình ---
# !!! CẢNH BÁO: 2,000,000 bản ghi sẽ tạo ra file lớn và mất nhiều thời gian.
# Để thử nghiệm nhanh, bạn có thể giảm xuống 10000 hoặc 200000.
NUM_RECORDS = 2000000 
OUTPUT_CSV_FILE = 'customers_data.csv'

# --- Hàm chính để tạo dữ liệu ---

def main():
    """
    Hàm chính để tạo dữ liệu khách hàng và lưu vào file CSV.
    """
    fake = Faker()

    print(f"Bắt đầu tạo {NUM_RECORDS} bản ghi cho bảng CUSTOMERS...")
    print("Quá trình này có thể mất vài phút. Vui lòng đợi...")

    # ----- Chuẩn bị các set để đảm bảo tính duy nhất -----
    generated_phones = set()
    generated_emails = set()

    # Hàm nội bộ để tạo SĐT duy nhất
    def generate_unique_phone():
        while True:
            # Tạo số điện thoại theo định dạng đơn giản, dễ đọc
            phone = f"09{random.randint(0, 8)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            if phone not in generated_phones:
                generated_phones.add(phone)
                return phone
    
    # Hàm nội bộ để tạo email duy nhất
    def generate_unique_email():
        while True:
            email = fake.unique.email()
            if email not in generated_emails:
                generated_emails.add(email)
                return email

    records = []

    # Sử dụng tqdm để theo dõi tiến trình
    for _ in tqdm(range(NUM_RECORDS), desc="Đang tạo dữ liệu khách hàng"):
        
        display_name = fake.name()
        phone_number = generate_unique_phone()
        email = generate_unique_email()
        
        # Tạo ngày đăng ký ngẫu nhiên trong vòng 5 năm gần nhất (tính đến 01/01/2023)
        end_date = datetime(2023, 1, 1)
        start_date = end_date - timedelta(days=5*365)
        registration_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        # Phân khúc khách hàng theo tỷ lệ thực tế hơn
        # 55% Standard, 25% New, 15% Loyal, 5% VIP
        customer_segment = random.choices(
            ['Standard', 'New', 'Loyal', 'VIP'],
            weights=[0.55, 0.25, 0.15, 0.05],
            k=1
        )[0]

        records.append({
            'display_name': display_name,
            'phone_number': phone_number,
            'email': email,
            'registration_date': registration_date,
            'customer_segment': customer_segment
        })

    # Chuyển đổi thành DataFrame
    print("\nĐang chuyển đổi dữ liệu và ghi ra file CSV...")
    df = pd.DataFrame(records)

    # Xuất ra file CSV
    df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8')
    
    print("\nHoàn thành!")
    print(f"Đã tạo thành công file '{OUTPUT_CSV_FILE}' với {len(df)} dòng dữ liệu.")
    print(f"Kích thước file khoảng: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Chạy hàm main
if __name__ == "__main__":
    main()