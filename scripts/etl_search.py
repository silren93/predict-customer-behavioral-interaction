import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import DateType

def process_search_logs(spark, input_path):
    """
    Hàm này thực hiện các bước đầu tiên của pipeline ETL cho log search
    """
    print(f"\nBắt đầu: Đang đọc dữ liệu Parquet từ: {input_path}")
    try:
        df_search = spark.read.option("mergeSchema", "true") \
                             .option("recursiveFileLookup", "true") \
                             .parquet(input_path)
        
        print("Đọc dữ liệu Parquet thành công.")
        print(f"Tổng số records: {df_search.count()}")
        
    except Exception as e:
        print(f"LỖI: Không thể đọc tệp. Vui lòng kiểm tra lại đường dẫn.")
        print(f"   Chi tiết lỗi: {e}")
        return None

    print("\nBắt đầu: Đang xử lý và biến đổi dữ liệu")

    # Làm sạch dữ liệu
    df_cleaned = df_search.select("user_id", "keyword", "datetime", "category") \
                          .filter(col("user_id").isNotNull()) \
                          .filter(col("keyword").isNotNull())

    print(f"Sau khi làm sạch: {df_cleaned.count()} records")

    # Lọc datetime hợp lệ
    df_valid_datetime = df_cleaned.filter(
        col("datetime").rlike(r"^\d{4}[-/]\d{2}[-/]\d{2}.*") |
        col("datetime").rlike(r"^\d{4}-\d{1,2}-\d{1,2}.*")
    )
    
    print(f"Sau khi lọc datetime hợp lệ: {df_valid_datetime.count()} records")

    # Chuyển đổi datetime và tạo cột month
    df_with_month = df_valid_datetime.withColumn("date", to_date(col("datetime"))) \
                                    .withColumn("month", month(col("date"))) \
                                    .filter(col("date").isNotNull())

    print(f"Sau khi chuyển đổi datetime: {df_with_month.count()} records")

    # Tách dữ liệu theo tháng
    df_june = df_with_month.filter(col("month") == 6)
    df_july = df_with_month.filter(col("month") == 7)

    print(f"Dữ liệu tháng 6: {df_june.count()} records")
    print(f"Dữ liệu tháng 7: {df_july.count()} records")

    # Tính Most_Search cho tháng 6
    df_june_counts = df_june.groupBy("user_id", "keyword", "category") \
                           .agg(count("*").alias("search_count"))
    
    window_june = Window.partitionBy("user_id").orderBy(desc("search_count"))
    df_june_ranked = df_june_counts.withColumn("rank", row_number().over(window_june))
    df_june_most = df_june_ranked.filter(col("rank") == 1) \
                                .select(col("user_id"), 
                                       col("keyword").alias("keyword_june"),
                                       col("category").alias("Category_june"))

    # Tính Most_Search cho tháng 7
    df_july_counts = df_july.groupBy("user_id", "keyword", "category") \
                           .agg(count("*").alias("search_count"))
    
    window_july = Window.partitionBy("user_id").orderBy(desc("search_count"))
    df_july_ranked = df_july_counts.withColumn("rank", row_number().over(window_july))
    df_july_most = df_july_ranked.filter(col("rank") == 1) \
                                .select(col("user_id"), 
                                       col("keyword").alias("keyword_july"),
                                       col("category").alias("Category_july"))

    # Kết hợp dữ liệu hai tháng
    df_combined = df_june_most.join(df_july_most, on="user_id", how="full_outer")

    # Tạo cột Most_Search_june và Most_Search_july
    df_with_most_search = df_combined.withColumn("Most_Search_june", col("keyword_june")) \
                                    .withColumn("Most_Search_july", col("keyword_july"))

    # ✅ Logic Behavior_Change đơn giản
    df_with_behavior = df_with_most_search.withColumn("Behavior_Change", 
        when(col("keyword_june").isNull() & col("keyword_july").isNull(), "No Data")
        .when(col("keyword_june").isNull() & col("keyword_july").isNotNull(), "New")
        .when(col("keyword_june").isNotNull() & col("keyword_july").isNull(), "Inactive")
        .when(col("keyword_june") == col("keyword_july"), "Unchanged")
        .otherwise("Changed")
    )

    # ✅ Logic Explain_Changed đơn giản
    df_final = df_with_behavior.withColumn("Explain_Changed", 
        when(col("Behavior_Change") == "Unchanged", "Unchanged")
        .when(col("Behavior_Change") == "New", "New")
        .when(col("Behavior_Change") == "Inactive", "Inactive")
        .when(col("Behavior_Change") == "No Data", "No Data")
        .when(col("Behavior_Change") == "Changed", "Changed")
        .otherwise("Other")
    )

    # Chọn các cột cuối cùng
    df_result = df_final.select(
        "user_id",
        "keyword_june", 
        "keyword_july",
        "Most_Search_june",
        "Most_Search_july", 
        "Category_june",
        "Category_july",
        "Behavior_Change",
        "Explain_Changed"
    )

    print("Tính toán 'Most_Search' thành công.")
    
    # Tạo DataFrame đơn giản cho Most_Search
    df_most_searched = df_result.select(col("user_id"), col("Most_Search_july").alias("Most_Search"))
    
    return df_most_searched, df_result


if __name__ == "__main__":
    print("KHỞI ĐỘNG ETL PIPELINE PHÂN TÍCH HÀNH VI TÌM KIẾM")

    spark = SparkSession.builder \
        .appName("ETL_Search_Behavior_Analysis") \
        .master("local[*]") \
        .getOrCreate()
    
    input_path = "dataset/log_search/"

    # Gọi hàm xử lý chính
    final_df, behavior_change_df = process_search_logs(spark, input_path)
    
    if final_df:
        print("\n=== KẾT QUẢ PHÂN TÍCH HÀNH VI TÌM KIẾM ===")
        print(f"Tổng số users được phân tích: {behavior_change_df.count()}")
        
        # Hiển thị phân phối behavior change
        print("\nPhân phối Behavior Change:")
        behavior_change_df.groupBy("Behavior_Change").count().orderBy(desc("count")).show()
        
        print("\nPhân phối Explain Changed:")
        behavior_change_df.groupBy("Explain_Changed").count().orderBy(desc("count")).show()
        
        # Hiển thị một số kết quả mẫu
        print("\nMẫu kết quả:")
        behavior_change_df.show(20, truncate=False)
        
        print("Hoàn tất xử lý log_search!")
    else:
        print("ETL Pipeline không thể hoàn thành do lỗi.")

    spark.stop()