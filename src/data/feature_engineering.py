"""
Feature engineering for Customer360 ML project
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *

class FeatureEngineering:
    def __init__(self):
        pass
    
    def create_customer_features(self, df):
        """Create customer behavior features"""
        # Calculate total viewing time
        df = df.withColumn("Total_Watch_Time", 
                          col("Giai_Tri") + col("Phim_Truyen") + 
                          col("The_Thao") + col("Thieu_Nhi") + col("Truyen_Hinh"))
        
        # Create engagement score
        df = df.withColumn("Engagement_Score", 
                          col("Total_Watch_Time") * col("Activeness"))
        
        # Content preference ratios
        df = df.withColumn("Truyen_Hinh_Ratio", 
                          col("Truyen_Hinh") / (col("Total_Watch_Time") + 1))
        
        return df
    
    def create_segmentation_features(self, df):
        """Create features for customer segmentation"""
        # Viewing diversity
        df = df.withColumn("Content_Diversity", 
                          when(col("Giai_Tri") > 0, 1).otherwise(0) +
                          when(col("Phim_Truyen") > 0, 1).otherwise(0) +
                          when(col("The_Thao") > 0, 1).otherwise(0) +
                          when(col("Thieu_Nhi") > 0, 1).otherwise(0) +
                          when(col("Truyen_Hinh") > 0, 1).otherwise(0))
        
        return df
