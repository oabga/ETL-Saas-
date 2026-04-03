# Automated ETL Pipeline on AWS

## Overview
Automated ETL Pipeline on AWS is a cloud-based data processing system designed to automatically handle uploaded CSV files through a RESTful web API.  
The system performs data cleaning, preprocessing, transformation, and storage in an optimized analytical format for downstream data analysis.

## Objectives
- Automate CSV data ingestion
- Clean and standardize raw data
- Convert data into analytics-friendly format
- Store processed output in cloud storage
- Deploy scalable backend services in cloud-native infrastructure

## System Architecture
The project follows an ETL workflow:

1. **Extract**
   - Users upload CSV files through FastAPI endpoints
   - Files are received and validated before processing

2. **Transform**
   - Handle missing values
   - Detect and treat outliers
   - Standardize data formats
   - Normalize column types

3. **Load**
   - Convert processed CSV into Parquet format
   - Upload transformed files to Amazon S3

## Technologies Used
- **FastAPI** for REST API backend
- **Pandas** for data preprocessing
- **Docker** for containerization
- **Kubernetes** for orchestration
- **Amazon S3** for cloud storage
- **AWS infrastructure** for deployment

## Core Features
- CSV upload via API
- Automatic preprocessing pipeline
- Missing value handling
- Outlier treatment
- Format standardization
- CSV to Parquet conversion
- Cloud storage integration

## Data Processing Pipeline
### Missing Value Handling
- Fill numerical missing values using statistical methods
- Fill categorical missing values using mode values

### Outlier Treatment
- Detect outliers using IQR method
- Replace extreme values where necessary

### Format Standardization
- Normalize date formats
- Standardize numeric columns
- Clean inconsistent categorical values

## Deployment
The backend service is containerized using Docker and deployed in Kubernetes environment.

Deployment workflow:
- Build Docker image
- Push image to container registry
- Deploy service using Kubernetes manifests
- Connect application with Amazon S3 storage

## Output
Processed files are stored in:
- **Parquet format**
- **Amazon S3 bucket**

This improves:
- Storage efficiency
- Query performance
- Compatibility with analytics systems
