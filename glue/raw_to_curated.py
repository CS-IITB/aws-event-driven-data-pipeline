"""
AWS Glue PySpark ETL - Raw to Curated
Validates 10 business logic rules, segregates rejected data into S3 quarantine,
enriches features, and joins Taxi Zone Lookup dimensions.
"""

import sys
import time
from datetime import datetime
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, hour, dayofweek, to_date, round as spark_round,
    when, lit, unix_timestamp, broadcast
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
JOB_NAME = args["JOB_NAME"]
BUCKET = args["bucket_name"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

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
    print(f"Audit logged: {job_name} | {status} | {duration}s | Written: {records_written} | Rejected: {records_rejected}")


start_time = time.time()

try:
    print("Step 1: Reading raw trip data...")
    trip_df = spark.read.parquet(f"s3://{BUCKET}/raw/nyctlc/fhvhv_tripdata/")
    
    # Filter for Lyft (HV0005)
    trip_df = trip_df.filter(col("hvfhs_license_num") == "HV0005")

    print("Reading taxi zone lookup table...")
    zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"s3://{BUCKET}/raw/nyctlc/taxi_zone_lookup/")

    print("Step 2: Applying 10 Data Quality filters...")
    tagged_df = trip_df.withColumn(
        "rejection_reason",
        when(col("trip_miles").isNull() | (col("trip_miles") <= 0), lit("trip_miles <= 0"))
        .when(col("trip_miles") >= 200, lit("trip_miles >= 200"))
        .when(col("base_passenger_fare").isNull() | (col("base_passenger_fare") <= 0), lit("base_passenger_fare <= 0"))
        .when(col("base_passenger_fare") >= 500, lit("base_passenger_fare >= 500"))
        .when(col("trip_time").isNull() | (col("trip_time") <= 0), lit("trip_time <= 0"))
        .when(col("trip_time") >= 14400, lit("trip_time >= 14400"))
        .when(col("pickup_datetime").isNull(), lit("pickup_datetime is NULL"))
        .when(col("dropoff_datetime").isNull() | (col("dropoff_datetime") <= col("pickup_datetime")), lit("dropoff <= pickup"))
        .when(col("PULocationID").isNull(), lit("PULocationID is NULL"))
        .when(col("DOLocationID").isNull(), lit("DOLocationID is NULL"))
        .otherwise(None)
    )

    valid_trips = tagged_df.filter(col("rejection_reason").isNull()).drop("rejection_reason")
    rejected_trips = tagged_df.filter(col("rejection_reason").isNotNull())

    # Write quarantined records to rejected/ prefix
    rejected_path = f"s3://{BUCKET}/rejected/nyctlc/fhvhv_trips_rejected/"
    rejected_trips.write.mode("overwrite").parquet(rejected_path)

    print("Step 3: Deriving analytical features...")
    enriched_df = valid_trips.withColumn(
        "trip_duration_minutes", spark_round(col("trip_time") / 60.0, 2)
    ).withColumn(
        "pickup_date", to_date(col("pickup_datetime"))
    ).withColumn(
        "pickup_hour", hour(col("pickup_datetime"))
    ).withColumn(
        "pickup_day_of_week", dayofweek(col("pickup_datetime"))
    ).withColumn(
        "is_weekend", when(dayofweek(col("pickup_datetime")).isin(1, 7), lit(1)).otherwise(lit(0))
    ).withColumn(
        "speed_mph",
        spark_round(when(col("trip_duration_minutes") > 0, col("trip_miles") / (col("trip_duration_minutes") / 60.0)), 2)
    ).withColumn(
        "is_shared_ride", when(col("shared_match_flag") == "Y", lit(1)).otherwise(lit(0))
    ).withColumn(
        "wait_time_minutes",
        spark_round(
            when(
                col("request_datetime").isNotNull() & col("pickup_datetime").isNotNull(),
                (unix_timestamp(col("pickup_datetime")) - unix_timestamp(col("request_datetime"))) / 60.0
            ), 2
        )
    ).filter(
        (col("speed_mph").isNull() | (col("speed_mph") <= 80)) &
        (col("trip_duration_minutes") >= 1) &
        (col("trip_duration_minutes") <= 240)
    )

    print("Step 4: Broadcast joins with Taxi Zone dimensions...")
    pickup_zone = zone_df.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone")
    )
    dropoff_zone = zone_df.select(
        col("LocationID").alias("DOLocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone")
    )

    curated_df = enriched_df         .join(broadcast(pickup_zone), on="PULocationID", how="left")         .join(broadcast(dropoff_zone), on="DOLocationID", how="left")

    print("Step 5: Writing clean Snappy Parquet partitioned by pickup_date...")
    output_path = f"s3://{BUCKET}/curated/nyctlc/fhvhv_trips_curated/"
    curated_df.write.mode("overwrite").partitionBy("pickup_date").parquet(output_path)

    duration = int(time.time() - start_time)
    write_audit_record(JOB_NAME, "curated", 0, 0, 0, duration, "SUCCESS")

    job.commit()

except Exception as e:
    duration = int(time.time() - start_time)
    write_audit_record(JOB_NAME, "curated", 0, 0, 0, duration, "FAILED", str(e))
    raise e
