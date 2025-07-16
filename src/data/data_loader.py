"""
Data loading utilities for Customer360 ML project
"""
import pandas as pd
from pyspark.sql import SparkSession
import yaml

class DataLoader:
    def __init__(self, config_path="config/database_config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.spark = self.create_spark_session()
    
    def create_spark_session(self):
        """Create Spark session"""
        return SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master(self.config['spark']['master']) \
            .getOrCreate()
    
    def load_customer_data(self, path):
        """Load customer data"""
        return self.spark.read.csv(path, header=True, inferSchema=True)
    
    def load_search_data(self, path):
        """Load search data"""
        return self.spark.read.csv(path, header=True, inferSchema=True)
