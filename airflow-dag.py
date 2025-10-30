# $AIRFLOW_HOME/dags/airflow_dag.py
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import requests
import boto3
import time

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vxm2ek"
submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

@task
def populate_queue():
    # get_run_logger() analogue
    log = get_current_context()["ti"].log

    log.info("Sending post request to API")
    response = requests.post(url, timeout=20)
    response.raise_for_status()
    log.info("Response received from API")

    queue_url = response.json()["sqs_url"]
    log.info(f"SQS URL: {queue_url} (21 delayed messages scheduled)")

    return queue_url

@task
def monitor_queue(queue_url: str, expected: int = 21, timeout: int = 930, interval: int = 15):
    log = get_current_context()["ti"].log
    sqs = boto3.client("sqs")
    start = time.time()

    while time.time() - start < timeout:
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"]
            )["Attributes"]
            visible = int(attrs.get("ApproximateNumberOfMessages", "0"))
            log.info(f"{visible} visible")
            if visible >= expected:
                log.info("Expected 21 messages visible. Proceeding.")
                return True
        except Exception as e:
            log.warning(f"Monitor read failed (continuing): {e}")
        time.sleep(interval)

    log.warning("Monitor timeout.")
    return True

@task
def reassemble_and_submit(queue_url: str, expected: int = 21, uvaid: str = "vxm2ek", platform: str = "airflow"):
    log = get_current_context()["ti"].log
    sqs = boto3.client("sqs")

    # Receive messages from the queue
    log.info("Receiving messages from queue")
    message_data = []

    # Receive messages in batches
    empty_receives = 0

    while len(message_data) < expected:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=20,
                VisibilityTimeout=60
            )

            # error handling case where response might not have 'Messages' key
            messages = response.get('Messages', [])
            if not messages:
                empty_receives += 1
                if empty_receives >= 3:
                    break
                continue

            # Reset counter if we got messages
            empty_receives = 0

        except Exception as e:
            log.error(f"Error receiving messages: {e}")
            log.warning("Continuing to poll")
            time.sleep(2)
            continue

        # Build batch deletion to avoid dangling messages
        delete_entries = []
        batch_idx = 0

        # extract order_no from messages
        for msg in messages:
            try:
                attributes = msg.get('MessageAttributes', {})
                # error handling case where message missing attributes
                if not attributes:
                    log.warning("Message missing attributes, skipping")
                    continue

                # cast order no to int
                order_no = int(attributes['order_no']['StringValue'])
                # get word from message
                word = attributes.get('word', {}).get('StringValue', '')

                message_data.append({
                    'order_no': order_no,
                    'word': word,
                    'uvaid': uvaid,
                    'platform': platform
                })

                # Add receipt handle to deletion batch
                delete_entries.append({
                    'Id': str(batch_idx),
                    'ReceiptHandle': msg.get('ReceiptHandle')
                })
                batch_idx += 1
            except (ValueError, KeyError, TypeError) as e:
                log.warning(f"Error parsing message: {e}, skipping")
                continue

        # Delete received messages immediately
        if delete_entries:
            try:
                sqs.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=delete_entries
                )
                log.info(f"Deleted {len(delete_entries)} messages")
            except Exception as e:
                log.error(f"Failed to delete messages: {e}")

        log.info(f"Received {len(message_data)} messages so far")

    # Sort by order_no
    message_data.sort(key=lambda x: x['order_no'])

    # Reassemble messages
    phrase = " ".join(msg['word'] for msg in message_data)

    log.info(f"Reassembled phrase: {phrase}")

    # Submit to submission queue
    log.info("Submitting solution to submission queue")

    response = sqs.send_message(
        QueueUrl=submission_url,
        MessageBody=phrase,
        MessageAttributes={
            'uvaid': {
                'DataType': 'String',
                'StringValue': uvaid
            },
            'phrase': {
                'DataType': 'String',
                'StringValue': phrase
            },
            'platform': {
                'DataType': 'String',
                'StringValue': platform
            }
        }
    )
    status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', 500)
    if status_code != 200:
        log.error(f"Failed to submit solution: {status_code}")
        raise Exception(f"Failed to submit solution: {status_code}")

    log.info(f"Submission response: {response}")
    return phrase

@dag(
    dag_id="dp2_airflow_flow",
    start_date=datetime(2023, 1, 1),
    schedule=None,         
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["dp2", "sqs", "aws"]
)
def dp2_flow():
    
    queue_url = populate_queue()
    monitor = monitor_queue(queue_url)
    monitor >> reassemble_and_submit(queue_url)

# Instantiate the DAG
dp2_flow()
