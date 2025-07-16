"""
Enhanced version của main_pipeline.py hiện tại
Không cần Snowflake, tập trung vào MySQL + advanced features
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rand, expr, when, lit, count, desc, current_timestamp, avg, sum as spark_sum
from pyspark.sql.window import Window
import random
import os
import json
from datetime import datetime
import logging

# Import existing modules
from etl_content import process_content_logs
from etl_search import process_search_logs

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedCustomer360Pipeline:
    def __init__(self):
        self.spark = self.create_spark_session()
        
    def create_spark_session(self):
        """Create optimized Spark session"""
        return SparkSession.builder \
            .appName("Customer360_Enhanced_Pipeline") \
            .master("local[*]") \
            .config("spark.driver.memory", "8g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.extraClassPath", "drivers/mysql-connector-j-8.4.0.jar") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def create_enhanced_fake_mapping(self, content_contracts, search_contracts):
        """Enhanced fake mapping với progress tracking"""
        logger.info("🔗 Creating enhanced fake mapping...")
        
        content_list = content_contracts.collect()
        search_list = search_contracts.collect()
        
        logger.info(f"📊 Content contracts: {len(content_list):,}")
        logger.info(f"🔍 Search contracts: {len(search_list):,}")
        
        # Tạo mapping với 70% match rate
        min_size = min(len(content_list), len(search_list))
        sample_size = min(min_size, int(len(content_list) * 0.7), 100000)
        
        mapping_data = []
        search_list_copy = search_list.copy()
        random.shuffle(search_list_copy)
        
        logger.info(f"🎯 Creating {sample_size:,} mappings...")
        
        for i in range(sample_size):
            if i < len(content_list) and i < len(search_list_copy):
                content_contract = content_list[i]['Contract']
                search_contract = search_list_copy[i]['user_id']  # FIX: Use correct column name
                
                if content_contract and search_contract:
                    mapping_data.append((content_contract, search_contract))
                    
            # Progress tracking
            if (i + 1) % 25000 == 0:
                progress = (i + 1) / sample_size * 100
                logger.info(f"📈 Progress: {progress:.1f}% ({i + 1:,}/{sample_size:,})")
        
        logger.info(f"✅ Created {len(mapping_data):,} fake mappings")
        
        if mapping_data:
            mapping_df = self.spark.createDataFrame(mapping_data, ["content_contract", "search_contract"])
            mapping_df = mapping_df.withColumn("mapping_timestamp", current_timestamp()) \
                                   .withColumn("mapping_type", lit("enhanced_fake"))
            return mapping_df
        else:
            logger.error("❌ No mappings created!")
            return None
    
    def generate_comprehensive_analytics(self, df_customer360):
        """Tạo analytics toàn diện với correct column names"""
        logger.info("📊 Generating comprehensive analytics...")
        
        # Debug: Show actual column names
        logger.info("📋 Available columns:")
        for col_name in df_customer360.columns:
            logger.info(f"   - '{col_name}'")
        
        # Basic statistics
        total_customers = df_customer360.count()
        customers_with_search = df_customer360.filter(col("Most_Search").isNotNull()).count()
        match_rate = (customers_with_search / total_customers * 100) if total_customers > 0 else 0
        
        # Advanced analytics
        analytics_data = {
            "total_customers": total_customers,
            "customers_with_search": customers_with_search,
            "match_rate": round(match_rate, 2),
            "analysis_timestamp": datetime.now().isoformat()
        }
        
        # Content category analysis - FIX: Use correct column names with spaces
        content_categories = {
            "Giai_Tri": "Giai_Tri",      # Now using underscores after standardization
            "Phim_Truyen": "Phim_Truyen",
            "The_Thao": "The_Thao", 
            "Thieu_Nhi": "Thieu_Nhi",
            "Truyen_Hinh": "Truyen_Hinh"
        }
        
        for actual_col, analytics_key in content_categories.items():
            try:
                # Use standardized column name with underscore
                category_stats = df_customer360.filter(col(actual_col).isNotNull()).agg(
                    count("*").alias("users"),
                    avg(actual_col).alias("avg_duration"),
                    spark_sum(actual_col).alias("total_duration")
                ).collect()[0]
                
                # Store with underscore key for consistency
                analytics_data[f"{analytics_key}_users"] = category_stats["users"] if category_stats["users"] else 0
                analytics_data[f"{analytics_key}_avg_duration"] = category_stats["avg_duration"] if category_stats["avg_duration"] else 0
                analytics_data[f"{analytics_key}_total_duration"] = category_stats["total_duration"] if category_stats["total_duration"] else 0
                
                logger.info(f"✅ {actual_col}: {category_stats['users']} users, avg: {category_stats['avg_duration']:.0f}")
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to process {actual_col}: {e}")
                # Set defaults
                analytics_data[f"{analytics_key}_users"] = 0
                analytics_data[f"{analytics_key}_avg_duration"] = 0
                analytics_data[f"{analytics_key}_total_duration"] = 0
        
        # Customer segmentation analysis
        try:
            segmentation_stats = df_customer360.groupBy("IQR_type", "Clinginess").agg(
                count("*").alias("customer_count"),
                avg("Total_Duration_All").alias("avg_duration"),
                avg("Activeness").alias("avg_activeness")
            ).collect()
            
            analytics_data["customer_segments"] = {}
            for row in segmentation_stats:
                segment_key = f"{row['IQR_type']}_{row['Clinginess']}"
                analytics_data["customer_segments"][segment_key] = {
                    "count": row["customer_count"],
                    "avg_duration": row["avg_duration"],
                    "avg_activeness": row["avg_activeness"]
                }
        except Exception as e:
            logger.warning(f"⚠️ Failed to process segmentation: {e}")
            analytics_data["customer_segments"] = {}
        
        # Save analytics to JSON
        os.makedirs("output/analytics", exist_ok=True)
        with open("output/analytics/comprehensive_analytics.json", "w") as f:
            json.dump(analytics_data, f, indent=2)
        
        logger.info("✅ Analytics saved to output/analytics/comprehensive_analytics.json")
        
        # Log key metrics
        logger.info("📈 KEY METRICS:")
        logger.info(f"   Total Customers: {total_customers:,}")
        logger.info(f"   Match Rate: {match_rate:.2f}%")
        logger.info(f"   Customers with Search: {customers_with_search:,}")
        
        return analytics_data
    
    def create_customer_insights(self, df_customer360):
        """Tạo customer insights với correct column names"""
        logger.info("💡 Creating customer insights...")
        
        # Enhanced customer tiers
        df_insights = df_customer360.withColumn(
            "customer_tier",
            when((col("IQR_type") == "upper") & (col("Clinginess") == "high"), "Premium")
            .when((col("IQR_type") == "middle") & (col("Clinginess") == "high"), "Loyal")
            .when((col("IQR_type") == "lower") & (col("Clinginess") == "low"), "At_Risk")
            .otherwise("Standard")
        ).withColumn(
            "engagement_score",
            (col("Activeness") * col("Total_Duration_All")) / 10000
        ).withColumn(
            "search_activity_level",
            when(col("Most_Search").isNotNull(), "Active_Searcher")
            .otherwise("Passive_Viewer")
        ).withColumn(
            "content_diversity",
            # FIX: Use standardized column names with underscores
            (when(col("Giai_Tri").isNotNull(), 1).otherwise(0) +
             when(col("Phim_Truyen").isNotNull(), 1).otherwise(0) +
             when(col("The_Thao").isNotNull(), 1).otherwise(0) +
             when(col("Thieu_Nhi").isNotNull(), 1).otherwise(0) +
             when(col("Truyen_Hinh").isNotNull(), 1).otherwise(0))
        ).withColumn(
            "insights_timestamp",
            current_timestamp()
        )
        
        # Show customer tier distribution
        try:
            tier_distribution = df_insights.groupBy("customer_tier").count().orderBy(desc("count"))
            logger.info("🎯 CUSTOMER TIER DISTRIBUTION:")
            tier_distribution.show(truncate=False)
            
            # Save insights
            df_insights.write.mode("overwrite").parquet("output/customer_insights")
            tier_distribution.write.mode("overwrite").parquet("output/tier_distribution")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to create tier distribution: {e}")
        
        return df_insights
    
    def standardize_column_names(self, df):
        """Standardize column names - convert spaces to underscores"""
        logger.info("🔧 Standardizing column names...")
        
        # Column mapping: space -> underscore
        column_mapping = {
            "Giai Tri": "Giai_Tri",
            "Phim Truyen": "Phim_Truyen", 
            "The Thao": "The_Thao",
            "Thieu Nhi": "Thieu_Nhi",
            "Truyen Hinh": "Truyen_Hinh"
        }
        
        # Apply column renaming
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
                logger.info(f"   📝 Renamed: '{old_name}' -> '{new_name}'")
        
        return df

    def run_enhanced_pipeline(self):
        """Chạy toàn bộ pipeline với các bước nâng cao - FIX: Use correct data paths"""
        logger.info("🚀 Starting enhanced Customer 360 pipeline...")
        
        try:
            # Step 1: Process content logs - FIX: Use correct data path
            logger.info("--- STEP 1: Processing Content Logs ---")
            content_input_path = "dataset/log_content/*.json"
            df_content_processed = process_content_logs(self.spark, content_input_path)
            
            if df_content_processed is None:
                raise Exception("Content processing failed")
            
            logger.info(f"✅ Content processing completed: {df_content_processed.count():,} records")
            
            # Step 2: Process search logs - FIX: Use correct data path
            logger.info("--- STEP 2: Processing Search Logs ---")
            search_input_path = "dataset/log_search/"
            df_search_processed, df_behavior_change = process_search_logs(self.spark, search_input_path)
            
            if df_search_processed is None:
                raise Exception("Search processing failed")
            
            if isinstance(df_search_processed, tuple):
                df_search_processed = df_search_processed[0]
            
            logger.info(f"✅ Search processing completed: {df_search_processed.count():,} records")
            
            # Step 3: Create enhanced fake mapping - FIX: Move before join
            logger.info("--- STEP 3: Creating Enhanced Fake Mapping ---")
            content_contracts = df_content_processed.select("Contract").distinct().filter(col("Contract").isNotNull()).cache()
            search_contracts = df_search_processed.select("user_id").distinct().filter(col("user_id").isNotNull()).cache()
            
            fake_mapping = self.create_enhanced_fake_mapping(content_contracts, search_contracts)
            
            if fake_mapping is None:
                raise Exception("Fake mapping creation failed")
            
            logger.info(f"✅ Created enhanced fake mapping: {fake_mapping.count():,} mappings")
            
            # Step 4: Join data - FIX: Use correct join logic
            logger.info("--- STEP 4: Joining Data with Enhanced Mapping ---")
            df_search_mapped = df_search_processed.join(
                fake_mapping,
                df_search_processed["user_id"] == fake_mapping["search_contract"],
                "inner"
            ).select(
                col("content_contract").alias("Contract"),
                col("Most_Search")
            )

            df_customer360 = df_content_processed.join(df_search_mapped, on="Contract", how="left")

            # Step 5: Standardize column names
            logger.info("--- STEP 5: Standardizing Column Names ---")
            df_customer360 = self.standardize_column_names(df_customer360)

            # Add metadata
            df_customer360 = df_customer360.withColumn("etl_timestamp", current_timestamp()) \
                                          .withColumn("etl_version", lit("2.0_enhanced"))

            logger.info(f"✅ Data joining completed: {df_customer360.count():,} records")
            
            # Step 6: Generate comprehensive analytics
            logger.info("--- STEP 6: Generating Comprehensive Analytics ---")
            analytics_results = self.generate_comprehensive_analytics(df_customer360)
            
            logger.info("✅ Comprehensive analytics generated")
            
            # Step 7: Create customer insights
            logger.info("--- STEP 7: Creating Customer Insights ---")
            df_insights = self.create_customer_insights(df_customer360)
            
            logger.info(f"✅ Customer insights created: {df_insights.count():,} records")
            
            # Step 8: Save final outputs
            logger.info("--- STEP 8: Saving Final Outputs ---")
            
            # Save to CSV
            df_customer360.repartition(1).write.mode("overwrite").option("header", "true").csv("output/customer360_enhanced.csv")
            df_insights.repartition(1).write.mode("overwrite").option("header", "true").csv("output/customer_insights_enhanced.csv")
            
            # Save to Parquet for better performance
            df_customer360.write.mode("overwrite").parquet("output/customer360_enhanced_final")
            df_insights.write.mode("overwrite").parquet("output/customer_insights_enhanced_final")
            
            # Save mapping
            fake_mapping.write.mode("overwrite").parquet("output/enhanced_fake_mapping")
            
            logger.info("✅ Final outputs saved")
            
            # Final summary
            logger.info("🎉 ENHANCED CUSTOMER360 PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"📊 Final Dataset: {df_customer360.count():,} records")
            logger.info(f"💡 Customer Insights: {df_insights.count():,} records")
            logger.info(f"🔗 Fake Mappings: {fake_mapping.count():,} records")
            logger.info(f"📈 Match Rate: {analytics_results['match_rate']}%")
            
            return df_customer360, df_insights, analytics_results
            
        except Exception as e:
            logger.error(f"❌ ENHANCED PIPELINE FAILED: {e}")
            raise
            
        finally:
            self.spark.stop()

if __name__ == "__main__":
    print("🚀 ENHANCED CUSTOMER360 PIPELINE")
    print("=" * 50)
    
    # Create output directories
    os.makedirs("output", exist_ok=True)
    os.makedirs("output/analytics", exist_ok=True)
    
    # Run enhanced pipeline
    pipeline = EnhancedCustomer360Pipeline()
    results = pipeline.run_enhanced_pipeline()
    
    print("\n🎊 Enhanced Pipeline completed successfully!")
    print("📁 Check output/ directory for results")
    print("📊 Check output/analytics/ for comprehensive analytics")