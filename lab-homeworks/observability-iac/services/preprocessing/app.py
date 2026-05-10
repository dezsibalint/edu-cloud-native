#!/usr/bin/env python3
"""
Image Preprocessing Service
Preprocesses images for OCR and publishes to RabbitMQ.
Uses Redis Streams with consumer groups for parallel processing.
"""

import os
import io
import json
import time
import redis
import cv2
import numpy as np
import pika
from minio import Minio
from prometheus_client import Counter, Histogram, start_http_server

SERVICE_NAME = "preprocessing"

# Environment configuration
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
MINIO_HOST = os.environ['MINIO_HOST']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
MINIO_BUCKET = os.environ['MINIO_BUCKET']
RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_USER = os.environ['RABBITMQ_USER']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']

PREPROCESSING_STREAM = 'preprocessing_stream'
CONSUMER_GROUP = 'preprocessors'
OCR_QUEUE = 'ocr_queue'
METRICS_PORT = int(os.environ.get('METRICS_PORT', '8000'))

COMPONENT_EXECUTION_SECONDS = Histogram(
    'component_execution_seconds',
    'Time spent processing a single component request'
)
COMPONENT_SUCCESS_TOTAL = Counter(
    'component_success_total',
    'Total number of successfully completed processing tasks'
)
COMPONENT_FAILURES_TOTAL = Counter(
    'component_failures_total',
    'Total number of failed processing tasks'
)
DOCUMENT_UPLOAD_TO_FINISH_SECONDS = Histogram(
    'document_upload_to_finish_seconds',
    'Time elapsed from file upload until the component finished processing'
)

# Initialize clients
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# RabbitMQ connection parameters
rabbitmq_params = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
    heartbeat=600,
    blocked_connection_timeout=300,
    connection_attempts=3,
    retry_delay=2
)

# Global RabbitMQ connection and channel
rabbitmq_connection = None
rabbitmq_channel = None


def preprocess_image(image):
    """Apply grayscale conversion and Otsu binarization for better OCR accuracy."""
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return binary


def publish_to_rabbitmq(job_id, page_num, total, preprocessed_path, upload_ts):
    """
    Publish to RabbitMQ using persistent channel.
    """
    global rabbitmq_channel
    
    message = {
        'job_id': job_id,
        'page_number': page_num,
        'total_pages': total,
        'image_path': preprocessed_path,
        'upload_ts': upload_ts,
    }
    
    rabbitmq_channel.basic_publish(
        exchange='',
        routing_key=OCR_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    
    print(f"Job {job_id}: Published page {page_num}/{total} to RabbitMQ")


def process_message(message_id, message_data):
    """Process a single Redis Stream message."""
    job_id = message_data['job_id']
    page_num = int(message_data['page_number'])
    total = int(message_data['total_pages'])
    image_path = message_data['image_path']
    upload_ts = float(
        message_data.get('upload_ts')
        or redis_client.get(f"job:{job_id}:start_ts")
        or redis_client.hget(f"job:{job_id}", 'upload_ts')
        or time.time()
    )
    
    print(f"Job {job_id}: Processing page {page_num}/{total}")
    start_t = time.perf_counter()

    try:
        # Download image from MinIO
        response = minio_client.get_object(MINIO_BUCKET, image_path)
        image_data = response.read()
        response.close()
        response.release_conn()
        
        # Decode and preprocess
        img_array = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        
        preprocessed = preprocess_image(img)
        
        # Encode to PNG
        success, buffer = cv2.imencode('.png', preprocessed)
        if not success:
            raise Exception("Failed to encode preprocessed image")
        
        # Upload to MinIO
        preprocessed_path = f"preprocessed/{job_id}/page_{page_num}.png"
        minio_client.put_object(
            MINIO_BUCKET,
            preprocessed_path,
            io.BytesIO(buffer.tobytes()),
            length=len(buffer),
            content_type='image/png'
        )
        
        print(f"Job {job_id}: Saved preprocessed page {page_num}")
        
        # Publish to RabbitMQ
        publish_to_rabbitmq(job_id, page_num, total, preprocessed_path, upload_ts)
        
        # ACK Redis message
        processing_duration = time.perf_counter() - start_t
        redis_client.incrbyfloat(f"job:{job_id}:processing_sum", processing_duration)
        redis_client.hincrbyfloat(f"job:{job_id}", 'work_seconds', processing_duration)
        redis_client.xack(PREPROCESSING_STREAM, CONSUMER_GROUP, message_id)
        COMPONENT_SUCCESS_TOTAL.inc()
        print(f"Job {job_id}: Page {page_num}/{total} complete")
    except Exception as e:
        print(f"Job {job_id}: Page {page_num} failed - {e}")
        COMPONENT_FAILURES_TOTAL.inc()
    finally:
        duration = time.perf_counter() - start_t
        COMPONENT_EXECUTION_SECONDS.observe(duration)
        DOCUMENT_UPLOAD_TO_FINISH_SECONDS.observe(time.time() - upload_ts)


def consume_messages():
    """Main consumer loop."""
    global rabbitmq_connection, rabbitmq_channel
    
    print("Preprocessing service starting...")
    
    print(f"Connecting to RabbitMQ: {RABBITMQ_HOST}")
    start_http_server(METRICS_PORT)
    rabbitmq_connection = pika.BlockingConnection(rabbitmq_params)
    rabbitmq_channel = rabbitmq_connection.channel()
    rabbitmq_channel.queue_declare(queue=OCR_QUEUE, durable=True)
    print("RabbitMQ connection established")
    
    # Create consumer group if does not exist
    try:
        redis_client.xgroup_create(
            PREPROCESSING_STREAM,
            CONSUMER_GROUP,
            id='0',
            mkstream=True
        )
        print(f"Created consumer group: {CONSUMER_GROUP}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print(f"Consumer group already exists: {CONSUMER_GROUP}")
    
    consumer_name = f"preprocessor-{os.getpid()}"
    print(f"Consumer name: {consumer_name}")
    print(f"Listening on stream: {PREPROCESSING_STREAM}")

    while True:
        try:
            # Process RabbitMQ events to maintain heartbeat
            rabbitmq_connection.process_data_events(time_limit=0)
            
            messages = redis_client.xreadgroup(
                CONSUMER_GROUP,
                consumer_name,
                {PREPROCESSING_STREAM: '>'},
                count=1,
                block=5000
            )
            
            if messages:
                for stream, message_list in messages:
                    for message_id, message_data in message_list:
                        process_message(message_id, message_data)
            
            # Cleanup old messages
            redis_client.xtrim(PREPROCESSING_STREAM, maxlen=1000, approximate=True)
            
        except Exception as e:
            print(f"Consumer loop error: {e}")
            time.sleep(5)


if __name__ == '__main__':
    consume_messages()
