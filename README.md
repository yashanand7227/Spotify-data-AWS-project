# AWS Data Pipeline for Spotify Data Analysis

This project builds a **serverless data pipeline** using AWS services to process and analyze **Spotify preprocessed CSV files**.

---

## **Architecture Overview**
![AWS Data Pipeline](Spotify AWS Architecture.png)

### **Pipeline Workflow**
1. **S3 (Staging)** – Manually uploaded **preprocessed Spotify CSV data**.
2. **AWS Glue (Transformation)** – Performs ETL (Extract, Transform, Load) on the data.
3. **S3 (Destination)** – Stores the transformed data.
4. **AWS Glue Crawler** – Creates a schema for the transformed data.
5. **Amazon Athena** – Runs SQL queries on the processed data.
6. **Amazon QuickSight** – Visualizes insights from the Spotify data.

---
