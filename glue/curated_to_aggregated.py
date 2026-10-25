"""
AWS Glue PySpark ETL - Curated to Aggregated
Generates 3 aggregated analytical marts from curated Parquet tables:
1. Daily Borough Summary
2. Hourly Pattern Summary
3. Route Summary (OD Matrix)
"""

import sys
import time
from datetime import datetime
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round, when

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
    print(f"Audit logged: {job_name} | {status} | {duration}s")


start_time = time.time()

try:
    print("Reading curated HVFHV data...")
    curated_df = spark.read.parquet(f"s3://{BUCKET}/curated/nyctlc/fhvhv_trips_curated/")

    # 1. Daily Borough Summary
    print("Computing Daily Borough Summary...")
    daily_borough = curated_df.groupBy("pickup_date", "pickup_borough").agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("base_passenger_fare"), 2).alias("total_revenue"),
        spark_round(avg("base_passenger_fare"), 2).alias("avg_fare"),
        spark_round(avg("tips"), 2).alias("avg_tip"),
        spark_round(avg("trip_miles"), 2).alias("avg_trip_miles"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        spark_round(avg("speed_mph"), 2).alias("avg_speed_mph")
    )
    daily_borough.write.mode("overwrite").partitionBy("pickup_date").parquet(f"s3://{BUCKET}/aggregated/nyctlc/daily_borough_summary/")

    # 2. Hourly Pattern Summary
    print("Computing Hourly Pattern Summary...")
    hourly_pattern = curated_df.groupBy("pickup_hour", "is_weekend", "pickup_borough").agg(
        count("*").alias("total_trips"),
        spark_round(avg("base_passenger_fare"), 2).alias("avg_fare"),
        spark_round(avg("trip_miles"), 2).alias("avg_trip_miles"),
        spark_round(avg(when(col("base_passenger_fare") > 0, col("tips") / col("base_passenger_fare") * 100)), 2).alias("avg_tip_percentage"),
        spark_round(avg("speed_mph"), 2).alias("avg_speed_mph"),
        spark_round(avg("wait_time_minutes"), 2).alias("avg_wait_time_minutes")
    )
    hourly_pattern.write.mode("overwrite").parquet(f"s3://{BUCKET}/aggregated/nyctlc/hourly_pattern_summary/")

    # 3. Route Summary
    print("Computing Route Summary Matrix...")
    route_summary = curated_df.groupBy("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone").agg(
        count("*").alias("total_trips"),
        spark_round(avg("base_passenger_fare"), 2).alias("avg_fare"),
        spark_round(avg("trip_miles"), 2).alias("avg_trip_miles"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
    ).filter(col("total_trips") >= 10)
    route_summary.write.mode("overwrite").parquet(f"s3://{BUCKET}/aggregated/nyctlc/route_summary/")

    duration = int(time.time() - start_time)
    write_audit_record(JOB_NAME, "aggregated", 0, 0, 0, duration, "SUCCESS")

    job.commit()

except Exception as e:
    duration = int(time.time() - start_time)
    write_audit_record(JOB_NAME, "aggregated", 0, 0, 0, duration, "FAILED", str(e))
    raise e
