AWS Data Pipeline for Spotify Data Analysis

Architecture Overview

Pipeline Workflow
S3 (Staging) – Manually uploaded preprocessed Spotify CSV data.
AWS Glue (Transformation) – Performs ETL (Extract, Transform, Load) on the data.
S3 (Destination) – Stores the transformed data.
AWS Glue Crawler – Creates a schema for the transformed data.
Amazon Athena – Runs SQL queries on the processed data.
Amazon QuickSight – Visualizes insights from the Spotify data.

