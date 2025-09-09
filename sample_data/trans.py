import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Đường dẫn đến file CSV đầu vào và file Parquet đầu ra
csv_file_path = './sample_data/sip_invite.csv'
parquet_file_path = './sample_data/sip_invite.parquet'

# try:
#     # Bước 1: Đọc file CSV vào DataFrame của Pandas
#     df = pd.read_csv(csv_file_path)
#     print("Đã đọc file CSV thành công!")
#     print(df.head())
#     # df["msisdn"] = df["msisdn"].astype('str')
#     df["from_phone_enc"] = df["from_phone_enc"].astype('str')
#     df["to_phone_enc"] = df["to_phone_enc"].astype('str')

#     # Bước 2: Chuyển đổi DataFrame của Pandas thành Table của PyArrow
#     table = pa.Table.from_pandas(df)

#     # Bước 3: Ghi Table của PyArrow xuống file Parquet
#     pq.write_table(table, parquet_file_path)
#     print(f"\nĐã ghi file Parquet thành công tại: {parquet_file_path}")

# except FileNotFoundError:
#     print(f"Lỗi: Không tìm thấy file {csv_file_path}. Hãy kiểm tra lại đường dẫn.")
# except Exception as e:
#     print(f"Đã xảy ra lỗi: {e}")
try:
    # Bước 1: Đọc file Parquet thành Table của PyArrow
    table = pq.read_table(parquet_file_path)

    # Bước 2: Chuyển đổi Table của PyArrow thành DataFrame của Pandas
    df = table.to_pandas()

    # Bước 3: In nội dung của DataFrame ra màn hình
    print(f"Đã đọc file Parquet thành công: {parquet_file_path}")
    print("\n--- Nội dung của DataFrame ---")
    print(df)
    print(df.dtypes)
    
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file {parquet_file_path}. Hãy kiểm tra lại đường dẫn.")
except Exception as e:
    print(f"Đã xảy ra lỗi: {e}")