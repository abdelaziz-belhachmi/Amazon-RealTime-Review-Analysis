# Real-Time Customer Reviews Analysis on Amazon Products

[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Apache-orange.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-Apache-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/docker-v20.10-blue.svg)](https://www.docker.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-v5.0-green.svg)](https://www.mongodb.com/)

---

## Project Overview

- Real-time exploration of reviews using Kafka  
- Data preprocessing (lemmatization, vectorization, TF-IDF, etc.)  
- Dataset creation (partitioning, class indexing, target label creation)  
- Training selected models with 80% of the data (`Data.json`)  
- Validating model performance and tuning hyperparameters on 10% of the data  
- Testing the models with the remaining 10% to evaluate final performance  
- Selecting the best performing model  
- Saving the best model  
- Using the chosen model within the proposed architecture to present prediction results in two modes: online and offline  
- Online mode: continuously presents real-time prediction results for each review with associated information (using the 10% test data)  
- Offline mode: dashboard dedicated to analysis and visualization of prediction results on the 10% test data, logged into the database  

## Technologies Used

- **Kafka** for real-time streaming and review ingestion  
- **Zookeeper** for Kafka cluster management  
- **Apache Spark** (Streaming + MLlib with PySpark) for distributed processing and model training  
- **MongoDB** for NoSQL storage of prediction results  
- **Docker** for containerization  
- **Python** and **Jupyter Notebooks** for development and experimentation  
- **Django / Flask / JavaScript** for web application and dashboard  

## Dataset

Source: [Amazon Review Sentiment Analysis Dataset on Kaggle](https://www.kaggle.com/code/soniaahlawat/sentiment-analysis-amazon-review/input)

| Field Name       | Description                                       |
|------------------|-------------------------------------------------|
| `reviewerID`     | Reviewer’s unique ID                             |
| `asin`           | Product ID                                       |
| `reviewerName`   | Reviewer’s name                                  |
| `helpful`        | Helpfulness rating, e.g. `[2, 3]` means 2 out of 3 found the review helpful |
| `reviewText`     | Text content of the review                       |
| `overall`        | Product rating (out of 5)                        |
| `summary`        | Summary of the review                            |
| `unixReviewTime` | Review time (Unix timestamp)                     |
| `reviewTime`     | Review time (human-readable)                     |

### Target Class (Label) Definition

- `overall < 3`: Negative review  
- `overall = 3`: Neutral review  
- `overall > 3`: Positive review  
