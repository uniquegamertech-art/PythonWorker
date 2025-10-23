#!/usr/bin/env python3
# send_job.py
# Send PDF conversion job to unified worker (supports .docx and .pptx)
import os
import sys
import json
import pika
from dotenv import load_dotenv
from pathlib import Path

# ---------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------
load_dotenv()

url = os.getenv('CLOUDAMQP_URL')
if not url:
    print("CLOUDAMQP_URL not set!")
    sys.exit(1)

# ---------------------------------------------------------------------
# Parse command-line args: send_job.py [docx|pptx] [input.pdf] [output]
# ---------------------------------------------------------------------
if len(sys.argv) < 4:
    print("Usage: python send_job.py <docx|pptx> <inputKey> <outputKey> [bucket]")
    print("Example: python send_job.py pptx uploads/slides.pdf downloads/slides.pptx")
    sys.exit(1)

format_type = sys.argv[1].lower()
input_key = sys.argv[2]
output_key = sys.argv[3]
bucket = sys.argv[4] if len(sys.argv) > 4 else "pdf-converter-files"

# Validate format
if format_type not in ("docx", "pptx"):
    print("Error: format must be 'docx' or 'pptx'")
    sys.exit(1)

# Ensure output has correct extension
expected_ext = f".{format_type}"
if not output_key.lower().endswith(expected_ext):
    print(f"Warning: outputKey should end with {expected_ext}")
    # Optionally auto-fix:
    output_key = str(Path(output_key).with_suffix(expected_ext))

# ---------------------------------------------------------------------
# Connect to RabbitMQ
# ---------------------------------------------------------------------
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Single unified queue
QUEUE_NAME = "pdf-conversion-queue"
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# ---------------------------------------------------------------------
# Job payload
# ---------------------------------------------------------------------
job_data = {
    "bucket": bucket,
    "inputKey": input_key,
    "outputKey": output_key
}

# ---------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------
channel.basic_publish(
    exchange='',
    routing_key=QUEUE_NAME,
    body=json.dumps(job_data).encode(),
    properties=pika.BasicProperties(delivery_mode=2)  # persistent
)

print(f"[{format_type.upper()}] Job sent to {QUEUE_NAME}")
print(f"   Input : {input_key}")
print(f"   Output: {output_key}")
print(f"   Bucket: {bucket}")

# ---------------------------------------------------------------------
# Clean shutdown
# ---------------------------------------------------------------------
connection.close()
