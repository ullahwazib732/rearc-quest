---

# Rearc Quest — Data Engineering Pipeline with AWS

This project implements a **data ingestion, processing, and analytics pipeline** using AWS services and Infrastructure as Code. The pipeline ingests public datasets, stores them in Amazon S3, processes them using Python-based analytics, and orchestrates the workflow using AWS CDK and serverless components.

---


# Overview

This project builds a data pipeline that:

1. Synchronizes a public dataset from the **U.S. Bureau of Labor Statistics (BLS)** into Amazon S3.
2. Fetches **population data from the DataUSA API** and stores it as JSON in S3.
3. Performs analytics combining both datasets.
4. Automates the entire process using **AWS CDK**, serverless compute, and event-driven architecture.

The pipeline executes daily and processes analytics whenever new population data is uploaded.

---

# Architecture

High-level architecture:

```
               +----------------------+
               |  EventBridge (Daily) |
               +----------+-----------+
                          |
                          v
                +--------------------+
                | Step Functions     |
                | State Machine      |
                +---------+----------+
                          |
              +-----------+------------+
              |                        |
              v                        v
      +--------------+         +--------------+
      | Lambda Step1 |         | Lambda Step2 |
      | BLS Dataset  |         | Population   |
      | Sync         |         | API Fetch    |
      +------+-------+         +------+-------+
             |                        |
             |                        |
             v                        v
                +------------------+
                |   Amazon S3      |
                | Dataset Storage  |
                +---------+--------+
                          |
                          | (JSON Upload Event)
                          v
                   +-------------+
                   |   Amazon SQS |
                   +------+------+
                          |
                          v
                   +-------------+
                   | Lambda Step3|
                   | Analytics   |
                   +-------------+
```
#### NOTE: The local setup bypasses the SQS part but the AWS version inludes it, in local it simple triggers the exection of lambda sequesntially I didnt spend m,uch time perfecting it.

---

# Project Structure

```
rearc-quest/
│
├── rearc_quest/
│   └── rearc_quest_stack.py
│
├── lambda/
│   ├── step_1.py
│   ├── step_2.py
│   └── step_3.py
│
├── lambda_layers/
│   ├── aioboto3/
│   ├── loguru/
│   └── bs4/
│
├── notebooks/
│   └── analysis.ipynb
│
├── requirements.txt
│
└── README.md
```

---

# Part 1 — AWS S3 & Dataset Synchronization

### Objective

Republish the **BLS productivity dataset** into Amazon S3 and keep it synchronized with the source.

Dataset Source:

```
https://download.bls.gov/pub/time.series/pr/
```

### Implementation

A Lambda function (`step_1`) performs:

1. Fetches directory listing of dataset files
2. Downloads dataset files
3. Uploads them into S3
4. Prevents duplicate uploads

### Key Features

* Dynamic file discovery (no hard-coded filenames)
* Handles:
  * Added files
  * Removed files
  * Updated files
* Upload deduplication using S3 checks

### Output

Files are stored in:

```
s3://<bucket-name>/
```

the submission guidelines says to share the S3 url am not sure how to share so I generated a presigned URL which will expire in 12 hours, if you see link is expired please feel to free the check the screenshots attached,. which shows that data is uploaded from the lambda. anyways here is the s3 path [S3 Link](https://rearcqueststack-datasetbucket9be8b15b-kgxqiyvwk1va.s3.us-east-1.amazonaws.com/step_2.json?response-content-disposition=inline&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIQCc%2Feysvt3gYy6h0ubVUvv8PdJkgRyyndjvdGJE2U0jhAIgWjfg3mzJJKRwbQQ1Cj%2FqCg52rR6pjOb6EjvkgbhTERIq%2BQIIdBAAGgwzODU5MzY4NDYwODAiDI6NvDyHg8Ab5O3DAirWArtW9vjy2MQwapi2jfao%2FxovNrRCvNkwv%2F%2Fb%2FwbVP0IuGzAFJ8EMH3R4AyBcOCp7PwYoFunI6YPEXOtf4vwu4ACa6il9gSSOHt6qXHMPGEtaVBp2ZG2cTubMzpOSp3ToFvL5NekVXC2xi8l%2FP6JayLF4z1fZCk5xg%2B29ftlWfiA9amoO%2B4oHYR96gMUC9IbqKyKhlx6kp4L%2FHXlCi%2FY1shHiQAHERqBZweKDNL90tS6wqTZ%2F0K1WIbXSyJasZ1vw50EUIZWLftfOwYW%2Bj9z4f%2BpL1AcJ6i915dFMNdjEpFz1qC39EAk0K7smAIjqwZQxio81%2F7W%2BjquB0xd3Uj2cMN9qDvwF1WnUrzbTX7tqBDzi8ZyhCB76mQv%2F3LH3wGQ60YdleG8Yypm0otCTap%2FgBAVXoUoa5rzvUi4CuoRRyeZRR7asYQhIxY%2BpB5voBwjlCIu2r2%2FtzjC9xcnNBjqPAimWEJpDNs5H4hbUbuGzxUlvyhCZKSendQHCsZpI2n%2BkTKYMj3nwagoixFjsH04ldhxv6w9UbIi4D%2FYBSXCw1%2FE1I22z0vMKlnBoEGf%2ByLyTk6%2BMbqqzj7s4G%2BNudOtglKaLwyblCVBq9KDE7PwUTDkxAJ6c7I56LhKKCSdNpjBWYasMgfA%2FfIINB1Z1qAYG3snx5zrVeGnBx7WsYG0QKrzLI4q%2BpBZU5OH%2F7cZK9ahmAzlbHU1U0DK25SlnYe4V%2F8XczDXjaaA13cZLeRSiSheYWGh2FUKxkFM%2FiuxodFyYOIWVxOSdmNLmWWYuqmsTszMeUzwA8cPYYjZo0A7MlzIontkndmI0LvCkeLAIhCk%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAVTW5A5UADTGVVLA4%2F20260312%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260312T104152Z&X-Amz-Expires=43200&X-Amz-SignedHeaders=host&X-Amz-Signature=03802e4421a092aad987f7fb58047c17d44ab17a18b2ea57250c2bdc65ecdcf5)

---

# Part 2 — API Data Ingestion

### Objective

Fetch population data from the **DataUSA API** and store it in S3.

API Documentation:

```
https://datausa.io/api/
```

### API Endpoint Used

```
https://honolulu-api.datausa.io/tesseract/
```

### Query Parameters

| Parameter  | Value                     |
| ---------- | ------------------------- |
| cube       | acs_yg_total_population_1 |
| drilldowns | Year,Nation               |
| measures   | Population                |

### Implementation

Lambda (`step_2`) performs:

1. Calls the API
2. Fetches population dataset
3. Saves the result as JSON
4. Uploads to S3

### Output

```
s3://<bucket-name>/step_2.json
```

---

# Part 3 — Data Analytics

Analytics are performed using **Pandas**.

### Input Data

Dataset 1:

```
pr.data.0.Current
```

Dataset 2:

```
step_2.json
```

---

### Notebook

All analysis and queries are included in:

```
notebooks/analysis.ipynb
```

---

# Part 4 — Infrastructure as Code & Data Pipeline

Infrastructure is deployed using **AWS CDK (Python)**.

### Pipeline Steps

1️⃣ Lambda `step_1`
Synchronizes BLS dataset to S3

2️⃣ Lambda `step_2`
Fetches population API data and stores JSON

3️⃣ S3 Event Notification
Triggers SQS when JSON file is uploaded

4️⃣ SQS Queue
Buffers processing events

5️⃣ Lambda `step_3`
Processes analytics and logs results

---

# AWS Resources Created

| Resource          | Purpose                    |
| ----------------- | -------------------------- |
| S3 Bucket         | Store datasets             |
| Lambda Functions  | Data ingestion & analytics |
| Lambda Layers     | Dependencies               |
| Step Functions    | Pipeline orchestration     |
| EventBridge       | Daily scheduling           |
| SQS Queue         | Event buffering            |
| Dead Letter Queue | Failure handling           |

---

# Lambda Layers Used

| Layer          | Purpose                |
| -------------- | ---------------------- |
| aioboto3       | Async AWS SDK          |
| AWS SDK Pandas | Pandas / Numpy runtime |
| Loguru         | Structured logging     |
| BeautifulSoup4 | Web scraping           |

---

# Build & Deployment

Here are the screenshots of the successful verification on AWS platform

![s3_path](images/s3.png)
![s3_path](images/step_func.png)
![s3_path](images/step_1.png)
![s3_path](images/step_2.png)
![step 3 execution results](images/step_3.png)

### Install Dependencies

```
pip install -r requirements.txt
```

### Bootstrap CDK

```
cdk bootstrap
cdk synth
```

### Deploy Infrastructure

```
cdk deploy
```

---

# Local Setup

Am a linux user so I only provided the Shell script, but that works in the Mac OS as well, but windows gone case (PS I hate Windows never used it in my life). for local setup I have created the docker compose you can below commands and test it locally

```
docker compose up
```

after doing compose up now we need to run the local run shell script which basically invokes all three lambdas sequentially, if anything fails you can see the failure messages in terminal

```
./local_run.sh
```

---