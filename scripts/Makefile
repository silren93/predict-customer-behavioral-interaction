# Customer360 ML Project Makefile

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
