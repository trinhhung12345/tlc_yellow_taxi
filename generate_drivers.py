import pandas as pd
import random
from faker import Faker
from tqdm import tqdm
from datetime import date

# --- Cấu hình ---
NUM_RECORDS = 80000
OUTPUT_CSV_FILE = 'drivers_data.csv'

# --- Hàm chính để tạo dữ liệu ---

def main():
    """
    Hàm chính để tạo dữ liệu tài xế và lưu vào file CSV.
    """
    # Khởi tạo Faker để tạo dữ liệu giả
    fake = Faker()

    print(f"Bắt đầu tạo {NUM_RECORDS} bản ghi cho bảng DRIVERS...")

    # ----- Bước quan trọng: Chuẩn bị dữ liệu cho các ràng buộc -----

    # 1. Chuẩn bị danh sách vehicle_id duy nhất
    # Tạo một danh sách các ID từ 1 đến 80000 (vì vehicle_id là SERIAL, bắt đầu từ 1)
    vehicle_ids = list(range(1, NUM_RECORDS + 1))
    # Xáo trộn danh sách này để gán ngẫu nhiên cho mỗi tài xế
    random.shuffle(vehicle_ids)
    
    # 2. Chuẩn bị một set để đảm bảo license_number là duy nhất
    generated_licenses = set()

    # Hàm nội bộ để tạo license_number duy nhất
    def generate_unique_license_number():
        while True:
            # Tạo license number theo định dạng phổ biến của New York (ví dụ: T1234567C)
            plate = f"T{random.randint(1000000, 9999999)}C"
            if plate not in generated_licenses:
                generated_licenses.add(plate)
                return plate

    # List để chứa các bản ghi
    records = []

    # Sử dụng tqdm để tạo thanh tiến trình
    for _ in tqdm(range(NUM_RECORDS), desc="Đang tạo dữ liệu tài xế"):
        # Tạo tên đầy đủ
        legal_name = fake.name()
        
        # Tạo license number duy nhất
        license_number = generate_unique_license_number()
        
        # Tạo ngày sinh hợp lý cho tài xế (từ 21 đến 65 tuổi)
        # faker.date_of_birth() trả về đối tượng date, hoàn hảo cho PostgreSQL
        date_of_birth = fake.date_of_birth(minimum_age=21, maximum_age=65)

        # Tạo trạng thái tài xế theo tỷ lệ: 85% Active, 10% Inactive, 5% Suspended
        driver_status = random.choices(
            ['Active', 'Inactive', 'Suspended'], 
            weights=[0.85, 0.10, 0.05], 
            k=1
        )[0]

        # Lấy một vehicle_id duy nhất từ danh sách đã xáo trộn
        # .pop() lấy và xóa phần tử cuối cùng, đảm bảo mỗi ID chỉ được dùng một lần
        vehicle_id = vehicle_ids.pop()

        records.append({
            'legal_name': legal_name,
            'license_number': license_number,
            'date_of_birth': date_of_birth,
            'driver_status': driver_status,
            'vehicle_id': vehicle_id
        })

    # Chuyển đổi thành DataFrame
    df = pd.DataFrame(records)

    # Xuất ra file CSV
    df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8')
    
    print("\nHoàn thành!")
    print(f"Đã tạo thành công file '{OUTPUT_CSV_FILE}' với {len(df)} dòng dữ liệu.")
    print("Các cột trong file:", list(df.columns))

# Chạy hàm main
if __name__ == "__main__":
    main()