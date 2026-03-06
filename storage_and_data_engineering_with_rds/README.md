# Storage and data Engineerig with RDS

----
## Project Overview
- This project demonstrates a cloud-based data engineering pipeline built using AWS services and Python.
- The pipeline ingests a CSV dataset, stores raw data in Amazon S3, performs data cleaning and transformations using Python (Pandas), generates analytical datasets, and stores structured data in an AWS RDS database.

The goal is to simulate a real-world data engineering workflow, including ingestion, processing, storage, and analytical querying.

---
## Dataset
link: https://github.com/Parashuram-V-Pawar/datasets/blob/4c4e6d581d59cfe4c37df47c558e12e8f8729213/superstore_sales.csv

---
## Technology and Services used
- Cloud Platform: AWS
- AWS Services:
    - Amazon EC2 - Linux Instance
    - Amazon S3 – Data Lake storage
    - Amazon RDS – Managed relational database
- Programming Language: Python 3
- Libraries : Pandas

---
## Data Flow:
```
Local System
    ↓
Raw Dataset Upload
    ↓
Amazon S3 (Raw Data Lake)
    ↓
Python Data Processing (Pandas)
    ↓
Amazon S3 (Processed)
    ↓
AWS RDS (Microsoft SQL Server)
    ↓
SQL Analytics Queries
```

---
## Installation
```
-> Install this system level ODBC driver(for linux systems):
sudo yum install gcc gcc-c++ python3-devel unixODBC-devel -y

Clone the repository
-> git clone https://github.com/Parashuram-V-Pawar/aws-cloud-and-storage-integration.git

Move to project folder
-> cd aws-cloud-and-storage-integration/storage_and_data_engineering_with_rds

Create a virtual environment inside it
python3 -m venv venv
source venv/bin/activate

Install dependencies
-> pip install -r requirements.txt

Run the application
```

## Author
```
Parashuram V Pawar
GitHub username: Parashuram-V-Pawar
```