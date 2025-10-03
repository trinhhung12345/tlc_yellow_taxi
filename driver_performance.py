import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm
import sys
import random

# --- Cấu hình ---

# Cấu hình kết nối cho CSDL của Hệ thống CRM (nơi chứa DRIVER_PERFORMANCE)
DB_CONFIG_CRM = {
    "dbname": "Uber_crm_db",  
    "user": "postgres",
    "password": "hung12345",
    "host": "localhost",
    "port": "5432"
}

# Cấu hình kết nối cho CSDL của Hệ thống Vận hành (nơi chứa TRIPS)
DB_CONFIG_OPERATIONS = {
    "dbname": "Uber_operation_DB", 
    "user": "postgres",
    "password": "hung12345",
    "host": "localhost",
    "port": "5432"
}

# --- Hàm helper ---
# --- Hàm helper ---
def get_data_from_db(config, query, description):
    """Hàm chung để kết nối và lấy dữ liệu vào Pandas DataFrame."""
    conn = None
    try:
        print(f"Đang kết nối đến CSDL '{config['dbname']}' để {description}...")
        conn = psycopg2.connect(**config)
        df = pd.read_sql_query(query, conn)
        print(f"Lấy thành công {len(df)} dòng.")
        return df
    except psycopg2.Error as e:
        print(f"Lỗi khi kết nối đến '{config['dbname']}': {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def calculate_acceptance_rate(rating):
    """Hàm giả lập acceptance_rate dựa trên điểm trung bình."""
    if rating >= 4.5:
        return round(random.uniform(95.0, 100.0), 2)
    elif rating >= 4.0:
        return round(random.uniform(90.0, 95.0), 2)
    else:
        return round(random.uniform(70.0, 90.0), 2)

# --- Hàm chính ---
def main():
    feedback_query = "SELECT trip_id, rating_for_driver FROM trip_feedback WHERE rating_for_driver IS NOT NULL;"
    df_feedback = get_data_from_db(DB_CONFIG_CRM, feedback_query, "lấy dữ liệu feedback")

    trips_query = "SELECT trip_id, driver_id FROM trips;"
    df_trips = get_data_from_db(DB_CONFIG_OPERATIONS, trips_query, "lấy dữ liệu chuyến đi")

    print("\nĐang thực hiện JOIN dữ liệu feedback và trips trong bộ nhớ...")
    merged_df = pd.merge(df_feedback, df_trips, on='trip_id')
    print(f"Join thành công, có {len(merged_df)} bản ghi đánh giá được liên kết với tài xế.")

    print("Đang tính toán điểm trung bình cho mỗi tài xế...")
    performance_df = merged_df.groupby('driver_id')['rating_for_driver'].mean().reset_index()
    
    performance_df.rename(columns={'rating_for_driver': 'average_rating'}, inplace=True)
    performance_df['average_rating'] = performance_df['average_rating'].round(2)
    
    print("Đang giả lập tỷ lệ chấp nhận chuyến...")
    performance_df['acceptance_rate'] = performance_df['average_rating'].apply(calculate_acceptance_rate)
    
    print(f"Đã tính toán xong hiệu suất cho {len(performance_df)} tài xế.")

    conn_crm = None
    try:
        print("\nĐang kết nối đến CSDL CRM để chèn dữ liệu hiệu suất...")
        conn_crm = psycopg2.connect(**DB_CONFIG_CRM)
        cur = conn_crm.cursor()

        cur.execute("TRUNCATE TABLE driver_performance;")
        print("Đã xóa dữ liệu cũ trong bảng driver_performance.")
        
        # --- PHẦN SỬA LỖI NẰM Ở ĐÂY ---
        print("Đang chuyển đổi dữ liệu sang kiểu Python gốc...")
        data_to_insert = []
        for row in tqdm(performance_df.itertuples(index=False), total=len(performance_df), desc="Chuyển đổi"):
            # Chuyển đổi tường minh từng giá trị thành kiểu Python gốc
            driver_id = int(row.driver_id)
            average_rating = float(row.average_rating)
            acceptance_rate = float(row.acceptance_rate)
            data_to_insert.append((driver_id, average_rating, acceptance_rate))
        # --- KẾT THÚC PHẦN SỬA LỖI ---
        
        insert_query = "INSERT INTO driver_performance (driver_id, average_rating, acceptance_rate) VALUES %s;"
        
        print(f"Đang chèn {len(data_to_insert)} bản ghi vào bảng driver_performance...")
        execute_values(cur, insert_query, data_to_insert)
        
        conn_crm.commit()
        print(f"Hoàn thành! Đã chèn thành công {cur.rowcount} dòng.")

    except psycopg2.Error as e:
        print(f"Lỗi khi chèn dữ liệu vào CSDL CRM: {e}", file=sys.stderr)
    finally:
        if conn_crm:
            conn_crm.close()

if __name__ == "__main__":
    main()