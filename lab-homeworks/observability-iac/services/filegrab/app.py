#!/usr/bin/env python3
"""
FileGrab Service
Accepts file uploads via HTTP, stores them in MinIO, and queues jobs for processing.
"""

import os
import io
import uuid
import json
import time
from pathlib import Path
from flask import Flask, request, jsonify
import redis
from minio import Minio
from prometheus_client import Histogram, generate_latest, CONTENT_TYPE_LATEST

SERVICE_NAME = "filegrab"

# Environment configuration
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
MINIO_HOST = os.environ['MINIO_HOST']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
MINIO_BUCKET = os.environ['MINIO_BUCKET']

SUPPORTED_FORMATS = {'.pdf', '.png', '.jpg', '.jpeg'}

COMPONENT_EXECUTION_SECONDS = Histogram(
    'component_execution_seconds',
    'Time spent processing a single component request'
)

app = Flask(__name__)

# Initialize clients
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


def ensure_bucket_exists():
    """Create MinIO bucket if it doesn't exist."""
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "filegrab"}), 200


@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Upload file endpoint.
    Accepts PDF or image files, stores in MinIO, and queues for processing.
    """
    start_t = time.perf_counter()
    upload_ts = time.time()
    try:
        # Validate file presence
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        
        if not file.filename:
            return jsonify({"error": "Empty filename"}), 400
        
        # Validate file type
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in SUPPORTED_FORMATS:
            return jsonify({"error": f"Unsupported file type. Use: {', '.join(SUPPORTED_FORMATS)}"}), 400
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        file_path = f"uploads/{job_id}{file_ext}"
        
        # Store file in MinIO
        file_data = file.read()
        minio_client.put_object(
            MINIO_BUCKET,
            file_path,
            io.BytesIO(file_data),
            length=len(file_data)
        )
        
        print(f"Job {job_id}: File uploaded to MinIO: {file_path}")
        
        # Record upload metadata for downstream timing metrics
        redis_client.hset(
            f"job:{job_id}",
            mapping={
                'upload_ts': upload_ts,
                'file_path': file_path,
                'file_type': file_ext,
                'work_seconds': 0.0,
            },
        )
        
        # Queue job for PDF-to-Image service
        job_message = {
            'job_id': job_id,
            'file_path': file_path,
            'file_type': file_ext,
            'upload_ts': upload_ts,
        }
        redis_client.lpush('pdf_to_image_queue', json.dumps(job_message))
        
        print(f"Job {job_id}: Queued for processing")
        
        return jsonify({
            "status": "success",
            "job_id": job_id,
            "message": "File uploaded and queued for processing"
        }), 202
    except Exception as e:
        print(f"Upload failed: {e}")
        return jsonify({"error": "Upload failed"}), 500
    finally:
        COMPONENT_EXECUTION_SECONDS.observe(time.perf_counter() - start_t)


if __name__ == '__main__':
    print("FileGrab service starting...")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"MinIO: {MINIO_HOST}")
    
    ensure_bucket_exists()
    
    print("FileGrab service ready")
    app.run(host='0.0.0.0', port=5000, debug=False)
