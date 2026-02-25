#!/usr/bin/env python3
"""
PDF-to-Image Service
Converts PDF pages to images and publishes to Redis Streams for parallel processing.
"""

import os
import io
import json
import time
import cv2
import numpy as np
import redis
import fitz
from minio import Minio

SERVICE_NAME = "pdf_to_image"


# Environment configuration
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
MINIO_HOST = os.environ['MINIO_HOST']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
MINIO_BUCKET = os.environ['MINIO_BUCKET']
DPI = int(os.environ.get('DPI', '300'))

PREPROCESSING_STREAM = 'preprocessing_stream'
CONSUMER_GROUP = 'preprocessors'

# Initialize clients
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


def init_consumer_group():
    """Create consumer group for preprocessing workers if it doesn't exist."""
    try:
        redis_client.xgroup_create(
            name=PREPROCESSING_STREAM,
            groupname=CONSUMER_GROUP,
            id='0',
            mkstream=True
        )
        print(f"Created consumer group: {CONSUMER_GROUP}")
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def pdf_to_images(pdf_data, job_id):
    """Convert PDF pages to images."""
    doc = fitz.open(stream=pdf_data, filetype="pdf")
    total_pages = len(doc)
    pages = []
    
    print(f"Job {job_id}: Converting {total_pages} page(s)")
    
    for page_num in range(total_pages):
        pix = doc[page_num].get_pixmap(dpi=DPI)
        img_array = np.frombuffer(pix.samples, dtype=np.uint8)
        img = img_array.reshape(pix.height, pix.width, pix.n)
        
        # Convert to BGR format for OpenCV
        img = cv2.cvtColor(img, cv2.COLOR_RGBA2BGR if pix.n == 4 else cv2.COLOR_RGB2BGR)
        pages.append((img, page_num + 1, total_pages))
    
    doc.close()
    return pages


def image_to_page(image_data, job_id):
    """Convert single image file to page format."""
    print(f"Job {job_id}: Processing single image")
    
    img_array = np.frombuffer(image_data, np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
    
    if img is None:
        raise ValueError("Failed to decode image")
    
    return [(img, 1, 1)]


def upload_page_to_minio(image, job_id, page_number):
    """Upload page image to MinIO storage."""
    _, buffer = cv2.imencode('.png', image)
    image_path = f"pdf-pages/{job_id}/page_{page_number}.png"
    
    minio_client.put_object(
        MINIO_BUCKET,
        image_path,
        io.BytesIO(buffer.tobytes()),
        length=len(buffer)
    )
    
    return image_path


def publish_to_stream(job_id, page_number, total_pages, image_path):
    """Publish page information to Redis Stream for preprocessing workers."""
    message = {
        'job_id': job_id,
        'page_number': str(page_number),
        'total_pages': str(total_pages),
        'image_path': image_path
    }
    redis_client.xadd(PREPROCESSING_STREAM, message)


def process_job(job_data):
    """Process a single job from the queue."""
    job_id = job_data['job_id']
    file_path = job_data['file_path']
    file_type = job_data['file_type']
    
    print(f"Job {job_id}: Started processing")
    start_t = time.perf_counter()
    try:
        # Download file from MinIO
        response = minio_client.get_object(MINIO_BUCKET, file_path)
        file_data = response.read()
        response.close()
        response.release_conn()
        
        # Convert to pages based on file type
        if file_type.lower() == '.pdf':
            pages = pdf_to_images(file_data, job_id)
        else:
            pages = image_to_page(file_data, job_id)
        
        # Upload each page and publish to stream
        for image, page_num, total in pages:
            image_path = upload_page_to_minio(image, job_id, page_num)
            publish_to_stream(job_id, page_num, total, image_path)
            print(f"Job {job_id}: Published page {page_num}/{total}")
        
        print(f"Job {job_id}: Completed ({len(pages)} page(s))")
    except Exception as e:
        print(f"Job {job_id}: Processing failed - {e}")


def main():
    """Main processing loop - wait for jobs and process them."""
    print("PDF-to-Image service starting...")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"MinIO: {MINIO_HOST}")
    print(f"DPI: {DPI}")
    
    init_consumer_group()
    
    print("Waiting for jobs...")
    
    while True:
        try:
            # Block and wait for job from queue
            message = redis_client.brpop('pdf_to_image_queue', timeout=5)
            
            if message:
                job_data = json.loads(message[1])
                process_job(job_data)
                
        except KeyboardInterrupt:
            print("Shutting down...")
            break
        except Exception as e:
            print(f"Main loop error: {e}")


if __name__ == '__main__':
    main()
