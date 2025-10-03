import pandas as pd
import random
import string
from tqdm import tqdm

# --- Cấu hình ---
NUM_RECORDS = 80000
OUTPUT_CSV_FILE = 'vehicles_data.csv'

# --- Nguồn dữ liệu giả lập ---

# Danh sách các hãng xe và model phổ biến tại Mỹ để dữ liệu trông thật hơn
CAR_MAKES_MODELS = {
    'Toyota': ['Camry', 'Corolla', 'RAV4', 'Highlander', 'Sienna'],
    'Ford': ['F-150', 'Explorer', 'Escape', 'Fusion', 'Mustang'],
    'Honda': ['Accord', 'Civic', 'CR-V', 'Pilot', 'Odyssey'],
    'Chevrolet': ['Silverado', 'Equinox', 'Malibu', 'Tahoe', 'Suburban'],
    'Nissan': ['Altima', 'Sentra', 'Rogue', 'Titan', 'Pathfinder'],
    'Jeep': ['Wrangler', 'Grand Cherokee', 'Cherokee'],
    'Hyundai': ['Elantra', 'Sonata', 'Tucson', 'Santa Fe'],
    'Kia': ['Optima', 'Sorento', 'Sportage', 'Forte'],
    'Subaru': ['Outback', 'Forester', 'Crosstrek'],
    'Dodge': ['Charger', 'Challenger', 'Grand Caravan']
}

# Danh sách các màu xe phổ biến
COLORS = [
    'Black', 'White', 'Silver', 'Gray', 'Blue', 
    'Red', 'Brown', 'Green', 'Beige', 'Charcoal'
]

# --- Các hàm tạo dữ liệu ---

def generate_unique_license_plate(existing_plates):
    """
    Tạo một biển số xe duy nhất theo định dạng 'ABC-1234'.
    Sử dụng một set để đảm bảo tính duy nhất và hiệu quả khi kiểm tra.
    """
    while True:
        # Tạo 3 chữ cái ngẫu nhiên
        letters = ''.join(random.choices(string.ascii_uppercase, k=3))
        # Tạo 4 chữ số ngẫu nhiên
        digits = ''.join(random.choices(string.digits, k=4))
        plate = f"{letters}-{digits}"
        
        if plate not in existing_plates:
            existing_plates.add(plate)
            return plate

# --- Hàm chính để tạo dữ liệu ---

def main():
    """
    Hàm chính để tạo dữ liệu và lưu vào file CSV.
    """
    print(f"Bắt đầu tạo {NUM_RECORDS} bản ghi cho bảng VEHICLES...")
    
    # Sử dụng một list để lưu các dictionary, sau đó chuyển thành DataFrame
    records = []
    
    # Set để đảm bảo biển số xe là duy nhất
    generated_plates = set()
    
    # Sử dụng tqdm để tạo thanh tiến trình
    for _ in tqdm(range(NUM_RECORDS), desc="Đang tạo dữ liệu xe"):
        # 1. Chọn ngẫu nhiên hãng xe và model
        make = random.choice(list(CAR_MAKES_MODELS.keys()))
        model = random.choice(CAR_MAKES_MODELS[make])
        make_model_str = f"{make} {model}"
        
        # 2. Tạo biển số xe duy nhất
        plate = generate_unique_license_plate(generated_plates)
        
        # 3. Chọn màu sắc ngẫu nhiên
        color = random.choice(COLORS)
        
        # 4. Chọn sức chứa ngẫu nhiên, ưu tiên xe 4 chỗ
        # (60% là xe 4 chỗ, 25% là 5 chỗ, 15% là 7 chỗ)
        capacity = random.choices([4, 5, 7], weights=[0.60, 0.25, 0.15], k=1)[0]
        
        # Thêm bản ghi vào list
        records.append({
            'license_plate': plate,
            'make_model': make_model_str,
            'color': color,
            'capacity': capacity
        })
        
    # Chuyển list of dictionaries thành pandas DataFrame
    df = pd.DataFrame(records)
    
    # Sắp xếp lại cột theo đúng thứ tự trong CSDL (dù không bắt buộc)
    df = df[['license_plate', 'make_model', 'color', 'capacity']]
    
    # Xuất DataFrame ra file CSV mà không có cột index của pandas
    df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8')
    
    print("\nHoàn thành!")
    print(f"Đã tạo thành công file '{OUTPUT_CSV_FILE}' với {len(df)} dòng dữ liệu.")
    print("Các cột trong file:", list(df.columns))

# Chạy hàm main khi script được thực thi
if __name__ == "__main__":
    main()