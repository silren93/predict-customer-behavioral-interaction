# run_etl_content.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, sum, greatest, concat_ws, lit,
    regexp_extract, countDistinct, input_file_name, coalesce
)
from functools import reduce

def process_content_logs(spark, input_path):
    # Nội dung hàm này giữ nguyên, không thay đổi
    print(f"\nBắt đầu: Đang đọc dữ liệu từ: {input_path}")
    try:
        dir_path = input_path.split('*')[0]
        if not os.path.isdir(dir_path):
            print(f"LỖI: Thư mục không tồn tại: {dir_path}")
            return None
        df_content = spark.read.json(input_path)
        print("Đọc dữ liệu thành công.")
    except Exception as e:
        print(f"LỖI: Không thể đọc tệp. Vui lòng kiểm tra lại đường dẫn.")
        print(f"   Chi tiết lỗi: {e}")
        return None

    print("\nBắt đầu: Đang xử lý và biến đổi dữ liệu")
    
    df_with_filepath = df_content.withColumn("filePath", input_file_name())
    df_extracted = df_with_filepath.select("_source.*",
                                           regexp_extract(col("filePath"), r'(\d{8})\.json$', 1).alias("Date"))
    df_extracted = df_extracted.filter(col("Date") != "")

    df_categorized = df_extracted.withColumn("Type",
        when(col("AppName") == "CHANNEL", "Truyen Hinh").when(col("AppName") == "RELAX", "Giai Tri")
        .when(col("AppName") == "CHILD", "Thieu Nhi").when((col("AppName") == "FIMS") | (col("AppName") == "VOD"), "Phim Truyen")
        .when((col("AppName") == "KPLUS") | (col("AppName") == "SPORT"), "The Thao").otherwise("Khac")
    )
    df_cleaned = df_categorized.filter(col("Type") != "Khac").filter(col("Contract").isNotNull()) \
                               .filter(col("Contract") != '0').select("Contract", "Type", "TotalDuration", "Date")

    df_activeness = df_cleaned.groupBy("Contract").agg(countDistinct("Date").alias("Activeness"))
    print("Tính toán 'Activeness' thành công.")

    df_pivoted = df_cleaned.groupBy("Contract").pivot("Type").agg(sum("TotalDuration"))
    print("Pivot dữ liệu thành công.")

    df_main = df_pivoted.join(df_activeness, "Contract", "left")

    genre_cols = ["Giai Tri", "Phim Truyen", "The Thao", "Thieu Nhi", "Truyen Hinh"]
    for g_col in genre_cols:
        if g_col not in df_main.columns:
            df_main = df_main.withColumn(g_col, lit(None).cast('long'))

    df_main = df_main.withColumn("Most_Watch",
        when(greatest(*[coalesce(col(c), lit(0)) for c in genre_cols]) == col("Truyen Hinh"), "Truyen Hinh")
        .when(greatest(*[coalesce(col(c), lit(0)) for c in genre_cols]) == col("Phim Truyen"), "Phim Truyen")
        .when(greatest(*[coalesce(col(c), lit(0)) for c in genre_cols]) == col("The Thao"), "The Thao")
        .when(greatest(*[coalesce(col(c), lit(0)) for c in genre_cols]) == col("Thieu Nhi"), "Thieu Nhi")
        .when(greatest(*[coalesce(col(c), lit(0)) for c in genre_cols]) == col("Giai Tri"), "Giai Tri")
    )
    df_main = df_main.withColumn("Taste", concat_ws("-",
        *[when(col(c).isNotNull(), lit(c)) for c in genre_cols]
    ))
    print("Tính toán 'Most_Watch' và 'Taste' thành công.")
    
    df_main = df_main.withColumn("Total_Duration_All",
                                 reduce(lambda a, b: a + b, [coalesce(col(c), lit(0)) for c in genre_cols]))

    quantiles_df = df_main.filter(col("Total_Duration_All") > 0)
    quantiles = quantiles_df.approxQuantile("Total_Duration_All", [0.25, 0.75], 0.01)
    q1 = quantiles[0]
    q3 = quantiles[1]

    df_main = df_main.withColumn("IQR_type",
        when(col("Total_Duration_All") < q1, "lower")
        .when((col("Total_Duration_All") >= q1) & (col("Total_Duration_All") <= q3), "middle")
        .otherwise("upper")
    )
    print("Tính toán 'IQR_type' thành công.")

    df_final = df_main.withColumn("Clinginess",
        when((col("Activeness") <= 15) & (col("IQR_type") == "lower"), "low")
        .when((col("Activeness") > 15) & (col("IQR_type") == "lower"), "medium")
        .when((col("Activeness") <= 10) & (col("IQR_type") == "middle"), "medium")
        .when((col("Activeness") <= 10) & (col("IQR_type") == "upper"), "medium")
        .when((col("Activeness") > 10) & (col("IQR_type") == "middle"), "high")
        .when((col("Activeness") > 10) & (col("IQR_type") == "upper"), "high")
        .otherwise("undefined")
    )
    print("Tính toán 'Clinginess' thành công.")

    return df_final


if __name__ == "__main__":
    print("KHỞI ĐỘNG ETL PIPELINE CHO LOG CONTENT")

    spark = SparkSession.builder \
        .appName("ETL_Log_Content_Optimized") \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    input_path = "dataset/log_content/*.json"
    final_df = process_content_logs(spark, input_path)
    
    if final_df:
        print("\nOUTPUT:")
        final_df.select("Contract", "Activeness", "Most_Watch", "Taste", "Total_Duration_All", "IQR_type", "Clinginess").show(20, truncate=False)
        print("ETL Pipeline hoàn tất thành công!")
    else:
        print("ETL Pipeline không thể hoàn thành do lỗi.")

    spark.stop()