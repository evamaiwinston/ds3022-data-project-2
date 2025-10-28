from prefect import flow, task, get_run_logger
import requests
import boto3
import time

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vxm2ek" # 

@task
def populate_queue():
    logger = get_run_logger()

    logger.info("Sending post request to API")
    response = requests.post(url, timeout=20)
    response.raise_for_status()
    logger.info("Response received from API")

    queue_url = response.json()["sqs_url"]   
    logger.info(f"SQS URL: {queue_url} (21 delayed messages scheduled)")

    return queue_url

@task
def monitor_queue(queue_url: str, expected: int = 21, timeout: int = 900, interval: int = 15):
    logger = get_run_logger()
    sqs = boto3.client("sqs")
    start = time.time()

    while time.time() - start < timeout:
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"]
            )["Attributes"]
            visible = int(attrs.get("ApproximateNumberOfMessages", "0"))
            logger.info(f"{visible} visible")
            if visible >= expected:
                logger.info("Expected 21 messages visible. Proceeding.")
                return True
        except Exception as e:
            logger.warning(f"Monitor read failed (continuing): {e}")
        time.sleep(interval)

    logger.warning("Monitor timeout.")
    return True



@task
def reassemble_and_submit(queue_url: str):
    logger = get_run_logger()
    sqs = boto3.client("sqs")
    
    # Receive messages from the queue 
    logger.info("Receiving messages from queue")
    message_data = []
    
    # Receive messages in batches
    empty_receives = 0
    
    while len(message_data) < 21:
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
            logger.error(f"Error receiving messages: {e}")
            logger.warning("Continuing to poll")
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
                    logger.warning("Message missing attributes, skipping")
                    continue

                # cast order no to int    
                order_no = int(attributes['order_no']['StringValue'])
                # get word from message
                word = attributes.get('word', {}).get('StringValue', '')
                uvaid = attributes.get('uvaid', {}).get('StringValue', '')
                platform = attributes.get('platform', {}).get('StringValue', '')
                
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
                logger.warning(f"Error parsing message: {e}, skipping")
                continue
        
        # Delete received messages immediately
        if delete_entries:
            try:
                sqs.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=delete_entries
                )
                logger.info(f"Deleted {len(delete_entries)} messages")
            except Exception as e:
                logger.error(f"Failed to delete messages: {e}")
        
        logger.info(f"Received {len(message_data)} messages so far")
    
    # Sort by order_no
    message_data.sort(key=lambda x: x['order_no'])
    
    # Reassemble messages
    phrase = " ".join(msg['word'] for msg in message_data)
    
    # uvaid and platform should be the same for all messages
    uvaid = message_data[0]['uvaid'] if message_data else ''
    platform = message_data[0]['platform'] if message_data else ''
    
    logger.info(f"Reassembled phrase: {phrase[:50]}...")
    
    # Submit to submission queue
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    logger.info("Submitting solution to submission queue")
    
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
        logger.error(f"Failed to submit solution: {status_code}")
        raise Exception(f"Failed to submit solution: {status_code}")
    
    logger.info(f"Submission response: {response}")
    return phrase


@flow
def main():
    logger = get_run_logger()
    queue_url = populate_queue()
    monitor_queue(queue_url)
    reassemble_and_submit(queue_url)
    logger.info("Flow completed successfully")

if __name__ == "__main__":
    main()

