import os
import shutil
import json
from pathlib import Path

def create_ml_project_structure():
    """Tạo lại cấu trúc project theo chuẩn ML"""
    print("🏗️ RESTRUCTURING TO ML PROJECT")
    print("=" * 50)
    
    # Define new structure
    ml_structure = {
        "config": [],
        "data": ["raw", "processed", "external"],
        "src": ["data", "models", "utils", "pipeline"],
        "notebooks": [],
        "tests": [],
        "models": [],
        "outputs": ["reports", "figures", "predictions"],
        "deployment": ["docker", "kubernetes", "sql", "monitoring"]
    }
    
    # Create directories
    for main_dir, sub_dirs in ml_structure.items():
        os.makedirs(main_dir, exist_ok=True)
        print(f"📁 Created: {main_dir}/")
        
        for sub_dir in sub_dirs:
            sub_path = os.path.join(main_dir, sub_dir)
            os.makedirs(sub_path, exist_ok=True)
            print(f"  📁 Created: {sub_path}/")
    
    print("\n✅ ML project structure created!")

def migrate_existing_files():
    """Di chuyển files hiện tại vào cấu trúc mới"""
    print("\n📦 MIGRATING EXISTING FILES")
    print("=" * 30)
    
    # Migration mapping
    migrations = {
        # Source files
        "main_pipeline.py": "src/pipeline/training_pipeline.py",
        "mysql_integration.py": "src/utils/database_utils.py",
        "create_dashboard.py": "src/utils/visualization_utils.py",
        
        # Data files
        "output/customer360_enhanced_final/": "data/processed/",
        "output/customer360_enhanced_mysql.csv/": "data/processed/",
        "output/analytics/": "outputs/reports/",
        "output/customer360_dashboard.png": "outputs/figures/customer360_dashboard.png",
        "output/customer360_final_report.txt": "outputs/reports/customer360_final_report.txt",
        
        # SQL files
        "sql/": "deployment/sql/",
        
        # Config files would be created new
    }
    
    for src, dst in migrations.items():
        if os.path.exists(src):
            try:
                # Create destination directory if needed
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                
                if os.path.isdir(src):
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)
                else:
                    shutil.copy2(src, dst)
                
                print(f"✅ Moved: {src} -> {dst}")
                
            except Exception as e:
                print(f"❌ Failed to move {src}: {str(e)}")
    
    print("\n✅ File migration completed!")

def create_config_files():
    """Tạo các config files"""
    print("\n⚙️ CREATING CONFIG FILES")
    print("=" * 30)
    
    # Database config
    db_config = {
        "mysql": {
            "host": "localhost",
            "port": 3306,
            "database": "customer360_db",
            "user": "root",
            "password": "your_password"
        },
        "spark": {
            "app_name": "Customer360-ML",
            "master": "local[*]",
            "config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }
    }
    
    with open("config/database_config.yaml", "w") as f:
        import yaml
        yaml.dump(db_config, f, default_flow_style=False)
    
    # Model config
    model_config = {
        "customer_segmentation": {
            "algorithm": "kmeans",
            "n_clusters": 4,
            "features": ["Total_Duration_All", "Activeness", "Giai_Tri", "Phim_Truyen"]
        },
        "behavior_prediction": {
            "algorithm": "random_forest",
            "n_estimators": 100,
            "max_depth": 10,
            "target": "Most_Watch"
        }
    }
    
    with open("config/model_config.yaml", "w") as f:
        yaml.dump(model_config, f, default_flow_style=False)
    
    # Pipeline config
    pipeline_config = {
        "data_sources": {
            "customer_data": "data/raw/contract_data.csv",
            "search_data": "data/raw/search_data.csv",
            "content_data": "data/raw/content_data.csv"
        },
        "output_paths": {
            "processed_data": "data/processed/",
            "models": "models/",
            "reports": "outputs/reports/"
        },
        "ml_pipeline": {
            "train_test_split": 0.8,
            "validation_split": 0.2,
            "random_state": 42
        }
    }
    
    with open("config/pipeline_config.yaml", "w") as f:
        yaml.dump(pipeline_config, f, default_flow_style=False)
    
    print("✅ Config files created!")

def create_src_modules():
    """Tạo các module chính"""
    print("\n🐍 CREATING SOURCE MODULES")
    print("=" * 30)
    
    # Data loader
    data_loader_code = '''"""
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
        return SparkSession.builder \\
            .appName(self.config['spark']['app_name']) \\
            .master(self.config['spark']['master']) \\
            .getOrCreate()
    
    def load_customer_data(self, path):
        """Load customer data"""
        return self.spark.read.csv(path, header=True, inferSchema=True)
    
    def load_search_data(self, path):
        """Load search data"""
        return self.spark.read.csv(path, header=True, inferSchema=True)
'''
    
    with open("src/data/data_loader.py", "w") as f:
        f.write(data_loader_code)
    
    # Feature engineering
    feature_eng_code = '''"""
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
'''
    
    with open("src/data/feature_engineering.py", "w") as f:
        f.write(feature_eng_code)
    
    # Customer segmentation model
    segmentation_code = '''"""
Customer segmentation model using ML
"""
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
import yaml

class CustomerSegmentation:
    def __init__(self, config_path="config/model_config.yaml"):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.model_config = config['customer_segmentation']
    
    def prepare_features(self, df):
        """Prepare features for clustering"""
        assembler = VectorAssembler(
            inputCols=self.model_config['features'],
            outputCol="features"
        )
        return assembler.transform(df)
    
    def train_model(self, df):
        """Train clustering model"""
        # Prepare features
        df_features = self.prepare_features(df)
        
        # Initialize KMeans
        kmeans = KMeans(
            k=self.model_config['n_clusters'],
            featuresCol="features",
            predictionCol="cluster"
        )
        
        # Train model
        model = kmeans.fit(df_features)
        return model
    
    def predict_segments(self, model, df):
        """Predict customer segments"""
        df_features = self.prepare_features(df)
        return model.transform(df_features)
'''
    
    with open("src/models/customer_segmentation.py", "w") as f:
        f.write(segmentation_code)
    
    # Create __init__.py files
    init_dirs = [
        "src/data/__init__.py",
        "src/models/__init__.py", 
        "src/utils/__init__.py",
        "src/pipeline/__init__.py"
    ]
    
    for init_file in init_dirs:
        with open(init_file, "w") as f:
            f.write("# Package init file\n")
    
    print("✅ Source modules created!")

def create_requirements():
    """Tạo requirements.txt"""
    requirements = [
        "pyspark==3.4.0",
        "pandas==1.5.3",
        "numpy==1.24.3",
        "scikit-learn==1.3.0",
        "matplotlib==3.7.1",
        "seaborn==0.12.2",
        "jupyter==1.0.0",
        "yaml==0.2.5",
        "mysql-connector-python==8.1.0",
        "python-dotenv==1.0.0",
        "pytest==7.4.0",
        "black==23.7.0",
        "flake8==6.0.0"
    ]
    
    with open("requirements.txt", "w") as f:
        for req in requirements:
            f.write(req + "\n")
    
    print("✅ Requirements.txt created!")

def create_readme():
    """Tạo README.md"""
    readme_content = '''# Customer360 Behavioral Interaction Prediction

## Project Overview
Machine Learning project for predicting customer behavioral interactions using Spark and advanced analytics.

## Project Structure
```
├── config/          # Configuration files
├── data/           # Data storage (raw, processed, external)
├── src/            # Source code
│   ├── data/       # Data processing modules
│   ├── models/     # ML models
│   ├── utils/      # Utility functions
│   └── pipeline/   # ML pipelines
├── notebooks/      # Jupyter notebooks
├── tests/         # Unit tests
├── models/        # Trained models
├── outputs/       # Reports and predictions
└── deployment/    # Deployment configurations
```

## Quick Start
```bash
# Install dependencies
pip install -r requirements.txt

# Run training pipeline
python src/pipeline/training_pipeline.py

# Run inference
python src/pipeline/inference_pipeline.py
```

## Key Features
- Customer segmentation using KMeans clustering
- Behavior prediction with Random Forest
- Real-time recommendation engine
- Comprehensive analytics dashboard
- MySQL integration for production

## Configuration
Edit config files in `config/` directory:
- `database_config.yaml` - Database connections
- `model_config.yaml` - ML model parameters
- `pipeline_config.yaml` - Pipeline settings

## Data Pipeline
1. **Data Ingestion**: Load raw customer data
2. **Feature Engineering**: Create behavioral features
3. **Model Training**: Train ML models
4. **Prediction**: Generate customer insights
5. **Deployment**: Deploy to production

## Models
- **Customer Segmentation**: KMeans clustering
- **Behavior Prediction**: Random Forest classifier
- **Recommendation Engine**: Collaborative filtering

## Technologies
- Apache Spark for big data processing
- Python for ML development
- MySQL for data storage
- Docker for containerization
- Kubernetes for orchestration
'''
    
    # SỬA LỖI: Thêm encoding='utf-8'
    with open("README.md", "w", encoding='utf-8') as f:
        f.write(readme_content)
    
    print("✅ README.md created!")

def create_makefile():
    """Tạo Makefile"""
    makefile_content = '''# Customer360 ML Project Makefile

.PHONY: install test train predict clean

# Install dependencies
install:
    pip install -r requirements.txt

# Run tests
test:
    pytest tests/ -v

# Train models
train:
    python src/pipeline/training_pipeline.py

# Generate predictions
predict:
    python src/pipeline/inference_pipeline.py

# Clean outputs
clean:
    rm -rf outputs/predictions/*
    rm -rf models/*.pkl

# Setup development environment
setup:
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

# Code formatting
format:
    black src/
    flake8 src/

# Run full pipeline
pipeline:
    make train
    make predict
    make test
'''
    
    with open("Makefile", "w", encoding='utf-8') as f:
        f.write(makefile_content)
    
    print("✅ Makefile created!")

def create_config_files_without_yaml():
    """Tạo config files bằng JSON thay vì YAML"""
    print("\n⚙️ CREATING CONFIG FILES (JSON)")
    print("=" * 30)
    
    # Database config
    db_config = {
        "mysql": {
            "host": "localhost",
            "port": 3306,
            "database": "customer360_db",
            "user": "root",
            "password": "your_password"
        },
        "spark": {
            "app_name": "Customer360-ML",
            "master": "local[*]",
            "config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }
    }
    
    with open("config/database_config.json", "w", encoding='utf-8') as f:
        json.dump(db_config, f, indent=2)
    
    # Model config
    model_config = {
        "customer_segmentation": {
            "algorithm": "kmeans",
            "n_clusters": 4,
            "features": ["Total_Duration_All", "Activeness", "Giai_Tri", "Phim_Truyen"]
        },
        "behavior_prediction": {
            "algorithm": "random_forest",
            "n_estimators": 100,
            "max_depth": 10,
            "target": "Most_Watch"
        }
    }
    
    with open("config/model_config.json", "w", encoding='utf-8') as f:
        json.dump(model_config, f, indent=2)
    
    # Pipeline config
    pipeline_config = {
        "data_sources": {
            "customer_data": "data/raw/contract_data.csv",
            "search_data": "data/raw/search_data.csv",
            "content_data": "data/raw/content_data.csv"
        },
        "output_paths": {
            "processed_data": "data/processed/",
            "models": "models/",
            "reports": "outputs/reports/"
        },
        "ml_pipeline": {
            "train_test_split": 0.8,
            "validation_split": 0.2,
            "random_state": 42
        }
    }
    
    with open("config/pipeline_config.json", "w", encoding='utf-8') as f:
        json.dump(pipeline_config, f, indent=2)
    
    print("✅ Config files created (JSON format)!")

def create_additional_ml_files():
    """Tạo thêm các file ML cần thiết"""
    print("\n🔧 CREATING ADDITIONAL ML FILES")
    print("=" * 30)
    
    # Training pipeline
    training_pipeline_code = '''"""
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
'''
    
    with open("src/pipeline/training_pipeline.py", "w", encoding='utf-8') as f:
        f.write(training_pipeline_code)
    
    # Test file
    test_code = '''"""
Unit tests for Customer360 ML project
"""
import pytest
import sys
import os
sys.path.append(os.path.abspath('.'))

from src.data.feature_engineering import FeatureEngineering
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestFeatureEngineering:
    @classmethod
    def setup_class(cls):
        """Setup Spark session for tests"""
        cls.spark = SparkSession.builder.appName("TestCustomer360").getOrCreate()
    
    def test_create_customer_features(self):
        """Test customer feature creation"""
        # Create sample data
        data = [
            ("customer1", 100, 200, 50, 0, 300),
            ("customer2", 0, 0, 0, 100, 500)
        ]
        columns = ["Contract", "Giai_Tri", "Phim_Truyen", "The_Thao", "Thieu_Nhi", "Truyen_Hinh"]
        df = self.spark.createDataFrame(data, columns)
        
        # Test feature engineering
        fe = FeatureEngineering()
        df_features = fe.create_customer_features(df)
        
        # Check if new columns are created
        assert "Total_Watch_Time" in df_features.columns
        assert "Engagement_Score" in df_features.columns
        assert "Truyen_Hinh_Ratio" in df_features.columns
        
        # Check values
        result = df_features.collect()
        assert result[0]["Total_Watch_Time"] == 650  # 100+200+50+0+300
        assert result[1]["Total_Watch_Time"] == 600  # 0+0+0+100+500
    
    @classmethod
    def teardown_class(cls):
        """Cleanup Spark session"""
        cls.spark.stop()

if __name__ == "__main__":
    pytest.main([__file__])
'''
    
    with open("tests/test_feature_engineering.py", "w", encoding='utf-8') as f:
        f.write(test_code)
    
    # .gitignore
    gitignore_content = '''# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Jupyter Notebook
.ipynb_checkpoints

# Data
data/raw/*.csv
data/processed/*.parquet
*.db

# Models
models/*.pkl
models/*.joblib

# Outputs
outputs/predictions/*
outputs/reports/*.html
outputs/figures/*.png

# Logs
*.log

# OS
.DS_Store
Thumbs.db

# Spark
metastore_db/
spark-warehouse/
derby.log
'''
    
    with open(".gitignore", "w", encoding='utf-8') as f:
        f.write(gitignore_content)
    
    print("✅ Additional ML files created!")

if __name__ == "__main__":
    create_ml_project_structure()
    migrate_existing_files()
    
    # Use JSON config instead of YAML
    create_config_files_without_yaml()
    
    create_src_modules()
    create_requirements()
    create_readme()
    create_makefile()
    create_additional_ml_files()
    
    print("\n🎉 ML PROJECT RESTRUCTURE COMPLETED!")
    print("=" * 50)
    print("✅ New structure created")
    print("✅ Files migrated")
    print("✅ Config files created (JSON)")
    print("✅ Source modules created")
    print("✅ Documentation created")
    print("✅ Additional ML files created")
    print("\n🚀 Ready for ML development!")