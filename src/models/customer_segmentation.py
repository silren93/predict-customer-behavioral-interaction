"""
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
