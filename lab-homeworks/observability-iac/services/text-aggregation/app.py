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
from prometheus_client import Histogram, start_http_server

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
METRICS_PORT = int(os.environ.get('METRICS_PORT', '8000'))

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

COMPONENT_EXECUTION_SECONDS = Histogram(
    'component_execution_seconds',
    'Time spent processing a single component request'
)
DOCUMENT_UPLOAD_TO_FINISH_SECONDS = Histogram(
    'document_upload_to_finish_seconds',
    'Time elapsed from file upload until the component finished processing'
)
DOCUMENT_PAGE_COUNT = Histogram(
    'document_page_count',
    'Number of pages processed for a document'
)
DOCUMENT_TOTAL_WORK_SECONDS = Histogram(
    'document_total_work_seconds',
    'Total processing work time accumulated for a document'
)


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
    start_t = time.perf_counter()
    data = message.value
    
    job_id = data['job_id']
    page_num = data['page_number']
    total = data['total_pages']
    text = data['text']
    upload_ts = float(data.get('upload_ts') or redis_client.hget(f"job:{job_id}", 'upload_ts') or time.time())
    
    print(f"Job {job_id}: Received page {page_num}/{total}")
    
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

            total_work_seconds = float(redis_client.hget(f"job:{job_id}", 'work_seconds') or 0.0)
            DOCUMENT_PAGE_COUNT.observe(total)
            DOCUMENT_TOTAL_WORK_SECONDS.observe(total_work_seconds)
            DOCUMENT_UPLOAD_TO_FINISH_SECONDS.observe(time.time() - upload_ts)
            
            save_to_minio(job_id, full_text)
            print(f"Job {job_id}: Complete!")
    except Exception as e:
        print(f"Failed to process message: {e}")
        raise
    finally:
        COMPONENT_EXECUTION_SECONDS.observe(time.perf_counter() - start_t)


def main():
    """Main consumer loop."""
    print("Text Aggregation service starting...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"MinIO: {MINIO_HOST}")
    start_http_server(METRICS_PORT)
    
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
