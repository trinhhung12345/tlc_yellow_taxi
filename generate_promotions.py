import pandas as pd
import random
from faker import Faker
from tqdm import tqdm
from datetime import datetime, timedelta

# --- Cấu hình ---
NUM_RECORDS = 300
OUTPUT_CSV_FILE = 'promotions_data.csv'
# Giả định "ngày hôm nay" trong bối cảnh dữ liệu của chúng ta là cuối tháng 01/2023
SIMULATION_NOW = datetime(2023, 1, 31)

# --- Nguồn dữ liệu giả lập để mã code và mô tả trông thật hơn ---
PROMO_NOUNS = ['SALE', 'DEAL', 'SAVE', 'RIDE', 'UBER', 'NYC', 'WINTER', 'TRIP', 'GO']
PROMO_ADJECTIVES = ['BIG', 'SUPER', 'MEGA', 'WEEKEND', 'NEWUSER', 'HAPPY', 'FLASH']
DESCRIPTION_TEMPLATES = [
    "Enjoy a {value} discount on your next ride. Limited time offer!",
    "Special deal! Get {value} off for any trip completed this month.",
    "Welcome offer for new users: {value} off your first ride.",
    "Weekend Special: Use this code for {value} off on Saturday or Sunday.",
    "Flash sale! A sweet {value} discount just for you."
]

# --- Hàm chính để tạo dữ liệu ---

def main():
    """
    Hàm chính để tạo dữ liệu khuyến mãi và lưu vào file CSV.
    """
    fake = Faker()
    print(f"Bắt đầu tạo {NUM_RECORDS} bản ghi cho bảng PROMOTIONS...")

    records = []
    generated_codes = set()

    # Hàm nội bộ để tạo mã code duy nhất
    def generate_unique_promo_code():
        while True:
            adj = random.choice(PROMO_ADJECTIVES)
            noun = random.choice(PROMO_NOUNS)
            num = random.randint(10, 99)
            code = f"{adj}{noun}{num}"
            if code not in generated_codes:
                generated_codes.add(code)
                return code

    for _ in tqdm(range(NUM_RECORDS), desc="Đang tạo dữ liệu khuyến mãi"):
        promo_code = generate_unique_promo_code()
        
        # Quyết định loại và giá trị giảm giá
        discount_type = random.choice(['Percentage', 'Fixed'])
        if discount_type == 'Percentage':
            # Giảm giá phần trăm (10, 15, 20, 25, 50%)
            discount_value = random.choice([10, 15, 20, 25, 50])
            desc_val_str = f"{int(discount_value)}%"
        else: # 'Fixed'
            # Giảm giá tiền cố định (từ 2.00 đến 10.00 USD)
            discount_value = round(random.uniform(2.0, 10.0), 2)
            desc_val_str = f"${discount_value}"

        # Tạo mô tả từ template
        description = random.choice(DESCRIPTION_TEMPLATES).format(value=desc_val_str)
        
        # Tạo ngày bắt đầu và kết thúc
        # Tạo ra các khuyến mãi trong quá khứ, hiện tại và tương lai
        start_date_offset = random.randint(-365, 90) # Từ 1 năm trước đến 3 tháng sau
        start_date = SIMULATION_NOW + timedelta(days=start_date_offset)
        
        duration = random.randint(7, 90) # Khuyến mãi kéo dài từ 7 đến 90 ngày
        end_date = start_date + timedelta(days=duration)
        
        # is_active được xác định một cách logic
        is_active = (start_date <= SIMULATION_NOW and end_date >= SIMULATION_NOW)
        
        records.append({
            'promo_code': promo_code,
            'description': description,
            'discount_value': discount_value,
            'discount_type': discount_type,
            'start_date': start_date.strftime('%Y-%m-%d %H:%M:%S'), # Định dạng chuẩn
            'end_date': end_date.strftime('%Y-%m-%d %H:%M:%S'),
            'is_active': is_active
        })

    # Chuyển đổi thành DataFrame
    df = pd.DataFrame(records)
    
    # Xuất ra file CSV
    df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8')
    
    print("\nHoàn thành!")
    print(f"Đã tạo thành công file '{OUTPUT_CSV_FILE}' với {len(df)} dòng dữ liệu.")
    print("Các cột trong file:", list(df.columns))


if __name__ == "__main__":
    main()