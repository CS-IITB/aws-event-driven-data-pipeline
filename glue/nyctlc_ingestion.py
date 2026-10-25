"""
AWS Glue Python Shell - NYCTLC HVFHV Data Ingestion
Downloads 6 months of historical trip data and taxi zone lookup tables,
streaming large files directly to S3 via multipart uploads.
"""

import sys
import boto3
import requests
from datetime import datetime, timedelta
import time
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
JOB_NAME = args["JOB_NAME"]
BUCKET_NAME = args["bucket_name"]

CLOUDFRONT_BASE = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_PATH = "/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet"
ZONE_LOOKUP_URL = f"{CLOUDFRONT_BASE}/misc/taxi+_zone_lookup.csv"

S3_RAW_TRIP_PREFIX = "raw/nyctlc/fhvhv_tripdata/"
S3_RAW_ZONE_PREFIX = "raw/nyctlc/taxi_zone_lookup/"

s3_client = boto3.client("s3")
dynamodb = boto3.client("dynamodb")


def write_audit_record(job_name, layer, records_read, records_written, records_rejected, duration, status, error_message=""):
    dynamodb.put_item(
        TableName="pipeline-job-audit",
        Item={
            "job_name": {"S": job_name},
            "run_timestamp": {"S": datetime.now().isoformat()},
            "layer": {"S": layer},
            "records_read": {"N": str(records_read)},
            "records_written": {"N": str(records_written)},
            "records_rejected": {"N": str(records_rejected)},
            "duration_seconds": {"N": str(duration)},
            "status": {"S": status},
            "error_message": {"S": error_message}
        }
    )
    print(f"Audit record written: {job_name} | {status} | {duration}s | {records_written} files")


def find_available_months(num_months=6):
    today = datetime.now()
    available_months = []
    latest_month_offset = None

    for months_back in [2, 3, 4]:
        target_date = today - timedelta(days=30 * months_back)
        year = target_date.year
        month = target_date.month
        url = f"{CLOUDFRONT_BASE}{TRIP_DATA_PATH.format(year=year, month=month)}"
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            latest_month_offset = months_back
            print(f"Latest available month: {year}-{month:02d}")
            break

    if latest_month_offset is None:
        raise RuntimeError("No available NYCTLC HVFHV data located.")

    for i in range(num_months):
        months_back = latest_month_offset + i
        target_date = today - timedelta(days=30 * months_back)
        year = target_date.year
        month = target_date.month
        url = f"{CLOUDFRONT_BASE}{TRIP_DATA_PATH.format(year=year, month=month)}"
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            available_months.append((year, month, url))
            print(f"Available month: {year}-{month:02d}")

    return available_months


def download_and_upload(source_url, s3_key, stream=False):
    print(f"Downloading from {source_url}")
    if not stream:
        resp = requests.get(source_url, timeout=300)
        resp.raise_for_status()
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=resp.content)
        print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")
    else:
        chunk_size = 10 * 1024 * 1024  # 10MB
        resp = requests.get(source_url, timeout=300, stream=True)
        resp.raise_for_status()

        mpu = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=s3_key)
        upload_id = mpu["UploadId"]
        parts = []
        part_num = 1
        buffer = b""

        try:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    buffer += chunk
                    if len(buffer) >= chunk_size:
                        part = s3_client.upload_part(
                            Bucket=BUCKET_NAME, Key=s3_key, PartNumber=part_num, UploadId=upload_id, Body=buffer
                        )
                        parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                        part_num += 1
                        buffer = b""

            if buffer:
                part = s3_client.upload_part(
                    Bucket=BUCKET_NAME, Key=s3_key, PartNumber=part_num, UploadId=upload_id, Body=buffer
                )
                parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

            s3_client.complete_multipart_upload(
                Bucket=BUCKET_NAME, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )
            print(f"Multipart upload complete: s3://{BUCKET_NAME}/{s3_key}")
        except Exception as exc:
            s3_client.abort_multipart_upload(Bucket=BUCKET_NAME, Key=s3_key, UploadId=upload_id)
            raise exc


def main():
    start_time = time.time()
    files_downloaded = 0

    try:
        months = find_available_months(num_months=6)
        for year, month, trip_url in months:
            filename = f"fhvhv_tripdata_{year}-{month:02d}.parquet"
            key = f"{S3_RAW_TRIP_PREFIX}{filename}"
            download_and_upload(trip_url, key, stream=True)
            files_downloaded += 1

        # Zone dimension table
        download_and_upload(ZONE_LOOKUP_URL, f"{S3_RAW_ZONE_PREFIX}taxi_zone_lookup.csv")

        # Write completion marker file
        s3_client.put_object(Bucket=BUCKET_NAME, Key="raw/nyctlc/ingestion.done", Body=b"")
        print(f"Marker file written: s3://{BUCKET_NAME}/raw/nyctlc/ingestion.done")

        duration = int(time.time() - start_time)
        write_audit_record(JOB_NAME, "raw", files_downloaded, files_downloaded, 0, duration, "SUCCESS")

    except Exception as e:
        duration = int(time.time() - start_time)
        write_audit_record(JOB_NAME, "raw", files_downloaded, 0, 0, duration, "FAILED", str(e))
        raise e


if __name__ == "__main__":
    main()
