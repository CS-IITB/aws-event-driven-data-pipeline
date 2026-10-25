# Serverless Event-Driven NYC TLC Mobility Analytics Platform

[![AWS](https://img.shields.io/badge/AWS-Cloud-orange.svg?logo=amazon-aws)](https://aws.amazon.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-Glue_5.1-E25A1C.svg?logo=apachespark)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-blue.svg?logo=python)](https://www.python.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-green.svg)](#medallion-data-lake-architecture)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An end-to-end, event-driven data engineering platform on AWS processing over 35 million NYC Taxi & Limousine Commission (High Volume For-Hire Vehicle / Lyft) records. Built with AWS Glue, S3 Medallion Architecture, AWS Step Functions, AWS Lambda, Amazon DynamoDB, and Amazon Athena.

---

## Architecture & Workflow

```text
       [ NYCTLC CloudFront / External Ingestion ]
                           │
                           ▼
            [ Ingestion Job (Glue Python Shell) ]
     - Probes rolling 6-month historical windows
     - Streaming HTTP multipart upload directly into S3
                           │
                           ▼
                [ S3: raw/nyctlc/ ]
      ├── fhvhv_tripdata/*.parquet (90M-120M records)
      ├── taxi_zone_lookup/*.csv
      └── ingestion.done (Marker File)
                           │
                           ▼ (s3:ObjectCreated:* on ingestion.done)
          [ AWS Lambda: s3-pipeline-trigger ]
                           │
                           ▼ (Starts State Machine Execution)
      [ AWS Step Functions: nyctlc-transform-pipeline ]
                           │
        ┌──────────────────┴──────────────────┐
        ▼ (Distributed PySpark Job)           ▼ (Catch on Failure)
[ Glue: raw-to-curated-etl ]            [ SNS: Topic Alerts ]
 - 10 Data Quality validations                 │
 - Quarantine rejected rows to S3              ▼
 - Join Taxi Zone Lookup tables         [ Email Alert Notification ]
 - Snappy Parquet partitioned by pickup_date
        │
        ▼ (Downstream PySpark Job)
[ Glue: curated-to-aggregated-etl ]
 - Daily Borough Summary (pickup_date, borough)
 - Hourly Pattern Summary (pickup_hour, is_weekend)
 - Route Matrix Summary (pickup/dropoff zone pairs)
        │
        ▼
[ Glue Crawlers (Raw, Curated, Aggregated) ]
        │
        ▼
[ Glue Data Catalog & Amazon Athena (SQL Analytics) ]
        │
        ▼
[ Amazon DynamoDB Audit Log: pipeline-job-audit ]
```

### Key Engineering Decisions
- **Marker File Event Triggering Pattern**: The ingestion job writes 6 parquet files. Rather than triggering Lambda 6 separate times, the ingestion job writes `raw/nyctlc/ingestion.done`. An S3 object event filter targets this suffix, triggering Lambda and Step Functions exactly once.
- **Data Quality & Quarantine Split**: Evaluates 10 business logic checks in a single Spark expression, separating clean rows from rejected records. Rejected records are quarantined to `s3://.../rejected/nyctlc/` for auditing.
- **Glue DPU Rightsizing & Cost Optimization**: Tuned workers to 2x `G.1X` workers ($0.44/DPU-hr) after benchmarking against `G.2X`, saving over 50% in compute costs while comfortably beating SLA deadlines.
- **Automated S3 Lifecycle Rules**: Configured automated transitions to Standard-IA after 30 days and Glacier Flexible Retrieval after 90 days, with automated deletion of quarantined rejected rows after 15 days.

---

## Medallion Data Lake Architecture

| Layer | S3 Prefix | Storage Format | Description |
| :--- | :--- | :--- | :--- |
| **Raw** | `s3://.../raw/nyctlc/` | Parquet / CSV | Ingested trip datasets (~90M-120M rows) and taxi zone dimension table. |
| **Curated** | `s3://.../curated/nyctlc/` | Parquet (Snappy) | Cleaned, validated, and enriched records (~35M Lyft trips) partitioned by `pickup_date`. |
| **Rejected** | `s3://.../rejected/nyctlc/` | Parquet | Quarantined records tagged with exact failure reasons (e.g., `trip_miles <= 0`, `dropoff <= pickup`). |
| **Aggregated** | `s3://.../aggregated/nyctlc/` | Parquet | Three distinct marts: `daily_borough_summary`, `hourly_pattern_summary`, and `route_summary`. |

---

## Repository Structure

```text
├── athena/
│   └── validation_queries.sql       # Validation queries across Raw, Curated, and Aggregated layers
├── configs/
│   └── s3_lifecycle_policy.json     # Storage tiering and auto-cleanup policies
├── glue/
│   ├── nyctlc_ingestion.py          # Glue Python Shell streaming ingestion
│   ├── raw_to_curated.py            # PySpark transformation, 10 DQ checks, zone joins
│   └── curated_to_aggregated.py     # PySpark multi-table aggregation analytics
├── lambda/
│   └── lambda_trigger.py            # Event router starting Step Functions on marker file
├── step_functions/
│   └── state_machine.json           # Step Functions state machine with retries & alerts
├── LICENSE
└── README.md
```

---

## Deployment & Execution Guide

### 1. Provision Storage & Lifecycle Policy
```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="nyc-mobility-lakehouse-${ACCOUNT_ID}"

aws s3 mb s3://$BUCKET_NAME --region us-east-1

aws s3api put-bucket-lifecycle-configuration \
  --bucket $BUCKET_NAME \
  --lifecycle-configuration file://configs/s3_lifecycle_policy.json
```

### 2. Create DynamoDB Audit Tables
```bash
# Job Audit Table
aws dynamodb create-table \
  --table-name pipeline-job-audit \
  --attribute-definitions AttributeName=job_name,AttributeType=S AttributeName=run_timestamp,AttributeType=S \
  --key-schema AttributeName=job_name,KeyType=HASH AttributeName=run_timestamp,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

# Metadata Registry Table
aws dynamodb create-table \
  --table-name pipeline-datalake-metadata \
  --attribute-definitions AttributeName=pipeline_name,AttributeType=S AttributeName=job_name,AttributeType=S \
  --key-schema AttributeName=pipeline_name,KeyType=HASH AttributeName=job_name,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
```

### 3. Deploy Glue Jobs & Crawlers
Upload scripts to `s3://$BUCKET_NAME/scripts/` and configure:
1. `nyctlc-data-ingestion` (Python Shell 3.9, 0.0625 DPU)
2. `nyctlc-raw-to-curated-etl` (Glue 5.1 Spark, 2x G.1X workers)
3. `nyctlc-curated-to-aggregated-etl` (Glue 5.1 Spark, 2x G.1X workers)

### 4. Deploy Step Functions & Lambda Trigger
Deploy the state machine using `step_functions/state_machine.json`, then configure Lambda:
```bash
cd lambda && zip -r function.zip lambda_trigger.py

aws lambda create-function \
  --function-name nyctlc-pipeline-trigger \
  --runtime python3.12 \
  --handler lambda_trigger.lambda_handler \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/nyctlc-lambda-trigger-role" \
  --zip-file fileb://function.zip \
  --environment "Variables={STATE_MACHINE_ARN=arn:aws:states:us-east-1:${ACCOUNT_ID}:stateMachine:nyctlc-transform-pipeline}"
```

Configure S3 Event Notification on prefix `raw/nyctlc/` with suffix `ingestion.done` pointing to `nyctlc-pipeline-trigger`.

---

## Data Validation & Analytics in Athena

```sql
-- 1. Validate Curated Trips Count (~35M records)
SELECT COUNT(*) AS total_curated_trips FROM nyctlc_curated.fhvhv_trips_curated;

-- 2. Trips by Borough
SELECT pickup_borough, COUNT(*) AS trips
FROM nyctlc_curated.fhvhv_trips_curated
GROUP BY pickup_borough
ORDER BY trips DESC;

-- 3. Top Routes by Trip Volume & Average Fare
SELECT pickup_zone, dropoff_zone, total_trips, avg_fare
FROM nyctlc_aggregated.route_summary
ORDER BY total_trips DESC
LIMIT 5;
```
