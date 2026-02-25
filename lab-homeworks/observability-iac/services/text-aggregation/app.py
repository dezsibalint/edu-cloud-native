#!/usr/bin/env python3
"""
Text Aggregation Service
Aggregates OCR results from Kafka and saves final document to MinIO.
"""

import os
import io
import json
import time
import redis
from collections import defaultdict
from kafka import KafkaConsumer
from minio import Minio

SERVICE_NAME = "text_aggregation"

# Environment configuration
KAFKA_BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
KAFKA_GROUP = os.environ['KAFKA_GROUP_ID']
MINIO_HOST = os.environ['MINIO_HOST']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
MINIO_BUCKET = os.environ['MINIO_BUCKET']
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))

# In-memory storage for aggregating pages
job_pages = defaultdict(dict)
job_total_pages = {}

# Initialize clients
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def save_to_minio(job_id, text):
    """Save aggregated text to MinIO."""
    output_path = f"final-text/{job_id}/document.txt"
    
    text_bytes = text.encode('utf-8')
    minio_client.put_object(
        MINIO_BUCKET,
        output_path,
        io.BytesIO(text_bytes),
        length=len(text_bytes),
        content_type='text/plain'
    )
    
    print(f"Job {job_id}: Saved to MinIO: {output_path}")
    
    # Cleanup memory
    if job_id in job_pages:
        del job_pages[job_id]
    if job_id in job_total_pages:
        del job_total_pages[job_id]


def process_message(message):
    """Process a single Kafka message."""
    data = message.value
    
    job_id = data['job_id']
    page_num = data['page_number']
    total = data['total_pages']
    text = data['text']
    
    print(f"Job {job_id}: Received page {page_num}/{total}")
    start_t = time.perf_counter()
    
    try:
        # Store page
        job_pages[job_id][page_num] = text
        job_total_pages[job_id] = total
        
        # Check if complete
        if len(job_pages[job_id]) == total:
            print(f"Job {job_id}: All {total} pages received, aggregating...")
            
            # Sort pages and concatenate
            sorted_pages = sorted(job_pages[job_id].items())
            full_text = "\n\n".join(
                f"=== PAGE {num} ===\n{text}" for num, text in sorted_pages
            )
            
            save_to_minio(job_id, full_text)
            print(f"Job {job_id}: Complete!")
            
            # End-to-end metric from FileGrab start timestamp
            try:
                start_ts = float(redis_client.get(f"job:{job_id}:start_ts") or 0)
                if start_ts > 0:
                    total_duration = time.time() - start_ts
                    TOTAL_LAST_DURATION.set(total_duration)
            except Exception:
                pass
    except Exception as e:
        print(f"Failed to process message: {e}")
        raise


def main():
    """Main consumer loop."""
    print("Text Aggregation service starting...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"MinIO: {MINIO_HOST}")
    
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )
    
    print(f"Connected to Kafka - waiting for messages...")
    
    # Consume messages
    for message in consumer:
        try:
            process_message(message)
            consumer.commit()
        except Exception as e:
            print(f"Failed to process message: {e}")


if __name__ == '__main__':
    main()
