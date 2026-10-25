"""
Lambda Trigger: Ingestion Marker File Event Listener
Invoked when S3 detects the upload of raw/nyctlc/ingestion.done.
Dispatches execution to the Step Functions transformation state machine.
"""

import json
import logging
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sfn_client = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    if not STATE_MACHINE_ARN:
        raise ValueError("Environment variable STATE_MACHINE_ARN is missing.")

    for record in event.get("Records", []):
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]

        logger.info(f"Marker file detected: s3://{bucket_name}/{object_key}")

        execution_input = {
            "trigger_source": "s3_event",
            "bucket": bucket_name,
            "marker_key": object_key,
            "event_time": record.get("eventTime"),
        }

        response = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=f"run-marker-{context.aws_request_id[:8]}",
            input=json.dumps(execution_input),
        )

        logger.info(f"Started Step Function execution: {response['executionArn']}")

    return {
        "statusCode": 200,
        "message": "Step Function execution dispatched successfully."
    }
