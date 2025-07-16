from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import logging
import json
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MySQLIntegration:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.mysql_config = {
            "host": "localhost",
            "port": "3306",
            "database": "customer360_db",
            "user": "root",
            "password": "your_password",  # Thay đổi password
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    
    def create_spark_session(self):
        """Create Spark session WITHOUT MySQL connector (sẽ dùng CSV thay thế)"""
        return SparkSession.builder \
            .appName("MySQL Integration") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def save_to_csv_for_mysql(self, df, table_name):
        """Save DataFrame to CSV for MySQL import"""
        logger.info(f"💾 Saving data to CSV for MySQL import: {table_name}")
        
        try:
            # Save to CSV
            output_path = f"output/{table_name}_mysql.csv"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
            
            logger.info(f"✅ Successfully saved {df.count()} records to CSV: {output_path}")
            
            # Tạo MySQL import script
            self.create_mysql_import_script(table_name, output_path)
            
        except Exception as e:
            logger.error(f"❌ Failed to save to CSV: {str(e)}")
            raise
    
    def create_mysql_import_script(self, table_name, csv_path):
        """Tạo script để import CSV vào MySQL"""
        script_content = f"""
-- MySQL Import Script cho {table_name}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

USE customer360_db;

-- Disable foreign key checks for faster import
SET FOREIGN_KEY_CHECKS = 0;

-- Clear existing data
TRUNCATE TABLE {table_name};

-- Load data from CSV
LOAD DATA LOCAL INFILE '{csv_path}/part-00000*.csv'
INTO TABLE {table_name}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS;

-- Enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Verify import
SELECT COUNT(*) as total_records FROM {table_name};
SELECT * FROM {table_name} LIMIT 10;
"""
        
        with open(f"sql/{table_name}_import.sql", "w") as f:
            f.write(script_content)
        
        logger.info(f"📋 MySQL import script created: sql/{table_name}_import.sql")
    
    def load_processed_data(self):
        """Load processed data from pipeline output"""
        logger.info("📂 Loading processed data...")
        
        try:
            # Load main customer360 data
            df_customer360 = self.spark.read.parquet("output/customer360_enhanced_final")
            logger.info(f"✅ Loaded customer360 data: {df_customer360.count()} records")
            
            return df_customer360
            
        except Exception as e:
            logger.error(f"❌ Failed to load data: {str(e)}")
            raise
    
    def prepare_for_mysql(self, df):
        """Prepare DataFrame for MySQL insertion - SỬA LỖI"""
        logger.info("🔧 Preparing data for MySQL...")
        
        # Add ETL metadata - SỬA LỖI: Dùng current_timestamp() và lit()
        df_prepared = df.withColumn("etl_timestamp", current_timestamp()) \
                       .withColumn("etl_version", lit("v1.0"))
        
        # Handle null values according to schema defaults
        df_prepared = df_prepared.fillna({
            "Giai_Tri": 0,
            "Phim_Truyen": 0,
            "The_Thao": 0,
            "Thieu_Nhi": 0,
            "Truyen_Hinh": 0,
            "Activeness": 0,
            "Total_Duration_All": 0,
            "Most_Watch": "",
            "Taste": "",
            "IQR_type": "",
            "Clinginess": "",
            "Most_Search": "",
            "etl_version": "v1.0"
        })
        
        # Ensure Contract column is not null (Primary Key)
        df_prepared = df_prepared.filter(df_prepared.Contract.isNotNull())
        
        logger.info("✅ Data prepared for MySQL")
        return df_prepared
    
    def run_mysql_integration(self):
        """Main function to run MySQL integration"""
        logger.info("🚀 Starting MySQL Integration...")
        
        try:
            # Load processed data
            df = self.load_processed_data()
            
            # Prepare data for MySQL
            df_prepared = self.prepare_for_mysql(df)
            
            # Show sample data
            logger.info("📊 Sample data preview:")
            df_prepared.show(5)
            
            # Show schema
            logger.info("📋 Data schema:")
            df_prepared.printSchema()
            
            # Save to CSV for MySQL import
            self.save_to_csv_for_mysql(df_prepared, "customer360_enhanced")
            
            logger.info("🎉 MySQL Integration completed successfully!")
            logger.info("📝 Next steps:")
            logger.info("   1. Run MySQL server")
            logger.info("   2. Create database: mysql -u root -p < sql/schema.sql")
            logger.info("   3. Import data: mysql -u root -p < sql/customer360_enhanced_import.sql")
            
        except Exception as e:
            logger.error(f"❌ MySQL Integration failed: {str(e)}")
            raise
        finally:
            self.spark.stop()

# Main execution
if __name__ == "__main__":
    print("🗄️ MYSQL INTEGRATION FOR CUSTOMER360")
    print("=" * 50)
    
    mysql_integration = MySQLIntegration()
    mysql_integration.run_mysql_integration()