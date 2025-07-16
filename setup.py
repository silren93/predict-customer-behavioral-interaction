from setuptools import setup, find_packages

setup(
    name="spark-dataframe-merge-pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.0.0",
        "mysql-connector-python>=8.0.0"
    ],
    entry_points={
        "console_scripts": [
            "run-pipeline=main_pipeline:main"
        ]
    },
)