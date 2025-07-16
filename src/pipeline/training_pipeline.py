"""
Training pipeline for Customer360 ML project
"""
import sys
import os
sys.path.append(os.path.abspath('.'))

from src.data.data_loader import DataLoader
from src.data.feature_engineering import FeatureEngineering
from src.models.customer_segmentation import CustomerSegmentation
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrainingPipeline:
    def __init__(self):
        self.data_loader = DataLoader()
        self.feature_eng = FeatureEngineering()
        self.segmentation = CustomerSegmentation()
        
        # Load config
        with open("config/pipeline_config.json", "r") as f:
            self.config = json.load(f)
    
    def load_data(self):
        """Load processed data"""
        logger.info("Loading processed data...")
        df = self.data_loader.spark.read.parquet("data/processed/customer360_enhanced_final")
        logger.info(f"Data loaded: {df.count()} records")
        return df
    
    def engineer_features(self, df):
        """Engineer features for ML"""
        logger.info("Engineering features...")
        df = self.feature_eng.create_customer_features(df)
        df = self.feature_eng.create_segmentation_features(df)
        return df
    
    def train_segmentation_model(self, df):
        """Train customer segmentation model"""
        logger.info("Training segmentation model...")
        model = self.segmentation.train_model(df)
        
        # Save model
        model.write().overwrite().save("models/customer_segmentation_model")
        logger.info("Model saved to models/customer_segmentation_model")
        
        return model
    
    def run_pipeline(self):
        """Run full training pipeline"""
        logger.info("Starting training pipeline...")
        
        # Load data
        df = self.load_data()
        
        # Engineer features
        df = self.engineer_features(df)
        
        # Train model
        model = self.train_segmentation_model(df)
        
        # Generate predictions
        predictions = self.segmentation.predict_segments(model, df)
        predictions.write.mode("overwrite").parquet("outputs/predictions/customer_segments")
        
        logger.info("Training pipeline completed successfully!")

if __name__ == "__main__":
    pipeline = TrainingPipeline()
    pipeline.run_pipeline()
