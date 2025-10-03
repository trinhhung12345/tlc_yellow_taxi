import pandas as pd
import psycopg2
import random
from tqdm import tqdm
import sys

# --- Cấu hình ---

# !!! QUAN TRỌNG: CẬP NHẬT CẢ HAI BỘ CẤU HÌNH KẾT NỐI DƯỚI ĐÂY !!!

# Cấu hình kết nối cho CSDL của Hệ thống CRM
DB_CONFIG_CRM = {
    "dbname": "Uber_crm_db",  # << TÊN DB HỆ THỐNG CRM
    "user": "postgres",
    "password": "hung12345",
    "host": "localhost",
    "port": "5432"
}

# Cấu hình kết nối cho CSDL của Hệ thống Vận hành
DB_CONFIG_OPERATIONS = {
    "dbname": "Uber_operation_DB",  # << TÊN DB HỆ THỐNG VẬN HÀNH
    "user": "postgres",
    "password": "hung12345",
    "host": "localhost",
    "port": "5432"
}

NUM_RECORDS = 1200000
OUTPUT_CSV_FILE = 'trip_feedback_data.csv'

# --- Hàm helper để lấy dữ liệu từ CSDL CRM ---

def get_active_promotion_ids_from_crm():
    """
    Kết nối đến CSDL CRM, lấy danh sách các promotion_id đang hoạt động,
    và đóng kết nối.
    """
    conn_crm = None
    try:
        print("Đang kết nối đến CSDL CRM để lấy danh sách khuyến mãi...")
        conn_crm = psycopg2.connect(**DB_CONFIG_CRM)
        cur = conn_crm.cursor()
        
        cur.execute("SELECT promotion_id FROM promotions WHERE is_active = TRUE;")
        active_ids = [row[0] for row in cur.fetchall()]
        
        print(f"Lấy thành công {len(active_ids)} mã khuyến mãi đang hoạt động.")
        return active_ids
    except psycopg2.Error as e:
        print(f"Lỗi khi kết nối đến CSDL CRM: {e}", file=sys.stderr)
        # Trả về list rỗng nếu có lỗi để script không bị dừng
        return []
    finally:
        if conn_crm:
            conn_crm.close()
            print("Đã đóng kết nối đến CSDL CRM.")


# --- Hàm chính ---

def main():
    """
    Hàm chính để điều phối toàn bộ quá trình.
    """
    # Bước 1: Lấy danh sách khuyến mãi từ CSDL CRM trước tiên.
    active_promotion_ids = get_active_promotion_ids_from_crm()
    if not active_promotion_ids:
        print("Cảnh báo: Không có mã khuyến mãi nào được tìm thấy. Các chuyến đi sẽ không có khuyến mãi.", file=sys.stderr)
        # Gán một giá trị tạm để script không lỗi
        active_promotion_ids = [None] 

    # Bước 2: Bắt đầu quy trình chính với CSDL Vận hành.
    conn_ops = None
    try:
        print("\nĐang kết nối đến CSDL Vận hành để lấy dữ liệu chuyến đi...")
        conn_ops = psycopg2.connect(**DB_CONFIG_OPERATIONS)
        cur = conn_ops.cursor()
        print("Kết nối thành công.")
        
        print("Đang lấy dữ liệu các chuyến đi từ bảng TRIPS...")
        cur.execute("SELECT trip_id, customer_id FROM trips;")
        trip_data = cur.fetchall()
        
        if not trip_data:
            print("Lỗi: Không tìm thấy dữ liệu trong bảng TRIPS.", file=sys.stderr)
            return

        print(f"Bắt đầu tạo {NUM_RECORDS} bản ghi feedback...")
        if len(trip_data) < NUM_RECORDS:
            sampled_trips = trip_data
        else:
            sampled_trips = random.sample(trip_data, k=NUM_RECORDS)

        records = []
        for trip_id, customer_id in tqdm(sampled_trips, desc="Đang tạo feedback"):
            rating = random.choices([5, 4, 3, 2, 1, None], weights=[0.50, 0.30, 0.08, 0.01, 0.01, 0.10], k=1)[0]
            
            promo_id = None
            if active_promotion_ids != [None]:
                promo_id = random.choices([random.choice(active_promotion_ids), None], weights=[0.25, 0.75], k=1)[0]
            
            records.append({
                'trip_id': trip_id,
                'customer_id': customer_id,
                'rating_for_driver': rating,
                'used_promotion_id': promo_id
            })

        print("\nĐang chuyển đổi và ghi ra file CSV...")
        df = pd.DataFrame(records).fillna('')
        df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8')
        
        print("Hoàn thành!")
        print(f"Đã tạo thành công file '{OUTPUT_CSV_FILE}' với {len(df)} dòng dữ liệu.")

    except psycopg2.Error as e:
        print(f"Lỗi khi làm việc với CSDL Vận hành: {e}", file=sys.stderr)
    finally:
        if conn_ops:
            conn_ops.close()
            print("Đã đóng kết nối đến CSDL Vận hành.")


if __name__ == "__main__":
    main()