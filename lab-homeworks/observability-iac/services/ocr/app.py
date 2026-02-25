#!/usr/bin/env python3
"""
OCR Service
Performs OCR on preprocessed images using Tesseract and publishes results to Kafka.
"""

import os
import io
import json
import time
import cv2
import numpy as np
import pika
import pytesseract
from kafka import KafkaProducer
from minio import Minio

SERVICE_NAME = "ocr"

# Environment configuration
MINIO_HOST = os.environ['MINIO_HOST']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
MINIO_BUCKET = os.environ['MINIO_BUCKET']
RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_USER = os.environ['RABBITMQ_USER']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
KAFKA_BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']

OCR_QUEUE = 'ocr_queue'

# Initialize clients
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1
)


def extract_text(image):
    """Extract text using Tesseract OCR with optimized config."""
    config = '--oem 3 --psm 6'
    text = pytesseract.image_to_string(image, lang='eng', config=config)
    return text.strip()


def process_message(ch, method, _, body):
    """Process a single OCR message from RabbitMQ."""
    start_t = time.perf_counter()

    data = json.loads(body)
    job_id = data['job_id']
    page_num = data['page_number']
    total = data['total_pages']
    image_path = data['image_path']
    
    print(f"Job {job_id}: Processing page {page_num}/{total}")
    
    try:
        # Download preprocessed image from MinIO
        response = minio_client.get_object(MINIO_BUCKET, image_path)
        image_data = response.read()
        response.close()
        response.release_conn()
        
        # Decode image
        img_array = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_GRAYSCALE)
        
        # Extract text via OCR
        text = extract_text(img)
        print(f"Job {job_id}: Extracted {len(text)} characters from page {page_num}")
        
        # Save text to MinIO for debugging
        text_path = f"ocr-results/{job_id}/page_{page_num}.txt"
        minio_client.put_object(
            MINIO_BUCKET,
            text_path,
            io.BytesIO(text.encode('utf-8')),
            length=len(text.encode('utf-8')),
            content_type='text/plain'
        )
        print(f"Job {job_id}: Saved text to {text_path}")
        
        # Publish to Kafka
        message = {
            'job_id': job_id,
            'page_number': page_num,
            'total_pages': total,
            'text': text
        }
        kafka_producer.send(KAFKA_TOPIC, message)
        kafka_producer.flush()
        
        print(f"Job {job_id}: Published page {page_num}/{total}")
        
        # Acknowledge RabbitMQ message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Job {job_id}: Page {page_num} failed - {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    """Main processing loop - consume from RabbitMQ."""
    print("OCR service starting...")
    print(f"MinIO: {MINIO_HOST}")
    print(f"RabbitMQ: {RABBITMQ_HOST}")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Setup RabbitMQ connection
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=OCR_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    
    # Start consuming messages
    channel.basic_consume(queue=OCR_QUEUE, on_message_callback=process_message)
    
    print("Waiting for messages...")
    channel.start_consuming()


if __name__ == '__main__':
    main()
