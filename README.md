# Customer360 Behavioral Interaction Prediction

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
