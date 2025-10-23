#!/usr/bin/env python3
import os
import sys
import json
import logging
import signal
from pathlib import Path
from dotenv import load_dotenv
from botocore.config import Config as BConfig
import boto3
from pdf2docx import Converter
from pdf2pptx import convert_pdf2pptx
import pika

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# S3 Client
# ---------------------------------------------------------------------
load_dotenv()

s3_endpoint = os.getenv("AWS_S3_ENDPOINT", "https://aooujpoztxhj.ap-northeast-1.clawcloudrun.com")
use_ssl = os.getenv("AWS_S3_USE_SSL", "true").lower() == "true"
verify_env = os.getenv("AWS_S3_VERIFY", "true")
verify_tls = verify_env.lower() not in ("false", "0", "no")
if verify_env and os.path.exists(verify_env):
    verify_tls = verify_env

try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name=os.getenv("AWS_REGION", "ap-northeast-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        use_ssl=use_ssl,
        verify=verify_tls,
        config=BConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    logger.info("S3 client initialized.")
except Exception as e:
    logger.error(f"S3 init failed: {e}")
    sys.exit(1)

# ---------------------------------------------------------------------
# Conversion Handlers
# ---------------------------------------------------------------------
def convert_to_docx(input_path: str, output_path: str):
    logger.info("Converting PDF to DOCX")
    cv = Converter(input_path)
    try:
        cv.convert(output_path)
    finally:
        cv.close()

def convert_to_pptx(input_path: str, output_path: str):
    logger.info("Converting PDF to PPTX")
    convert_pdf2pptx(
        pdf_file=input_path,
        output_file=output_path,
        resolution=200,
        start_page=0,
        page_count=None,
        quiet=False
    )

# Map file extension → converter + MIME type
CONVERTERS = {
    ".docx": {
        "func": convert_to_docx,
        "mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    },
    ".pptx": {
        "func": convert_to_pptx,
        "mime": "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    }
}

# ---------------------------------------------------------------------
# Job Processor
# ---------------------------------------------------------------------
def process_job(ch, method, data):
    delivery_tag = method.delivery_tag
    logger.info(f"Starting job {delivery_tag}")

    input_key = data["inputKey"]
    output_key = data["outputKey"]
    bucket = data["bucket"]

    input_path = f"/tmp/{Path(input_key).name}"
    output_path = f"/tmp/{Path(output_key).name}"

    # Determine output format
    ext = Path(output_key).suffix.lower()
    converter_info = CONVERTERS.get(ext)

    if not converter_info:
        logger.error(f"Unsupported output format: {ext}")
        ch.basic_nack(delivery_tag=delivery_tag, requeue=False)
        return

    try:
        # Download input
        s3_client.head_object(Bucket=bucket, Key=input_key)
        logger.info(f"Downloading {input_key}")
        s3_client.download_file(bucket, input_key, input_path)

        # Convert
        converter_info["func"](input_path, output_path)

        # Upload result
        logger.info(f"Uploading {output_key}")
        s3_client.upload_file(
            output_path, bucket, output_key,
            ExtraArgs={"ContentType": converter_info["mime"]}
        )

        # Cleanup
        for path in (input_path, output_path):
            if os.path.exists(path):
                os.remove(path)

        logger.info(f"Job {delivery_tag} completed ({ext})")
        ch.basic_ack(delivery_tag=delivery_tag)

    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error(f"Input file not found: {input_key} — Dropping job.")
            ch.basic_ack(delivery_tag=delivery_tag)
        else:
            logger.error(f"S3 error: {e}")
            ch.basic_nack(delivery_tag=delivery_tag, requeue=False)
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        ch.basic_nack(delivery_tag=delivery_tag, requeue=False)

# ---------------------------------------------------------------------
# Callback
# ---------------------------------------------------------------------
def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())
    except json.JSONDecodeError:
        logger.error("Invalid JSON payload")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    process_job(ch, method, data)

# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
def main():
    url = os.getenv('CLOUDAMQP_URL')
    if not url:
        logger.error("CLOUDAMQP_URL not set")
        sys.exit(1)

    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    queue_name = "pdf-conversion-queue"
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    logger.info("Unified PDF Worker started – supports .docx and .pptx")

    def shutdown(sig, frame):
        logger.info("Shutting down gracefully...")
        channel.stop_consuming()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            connection.close()
        except:
            pass
        logger.info("Worker stopped.")

# ---------------------------------------------------------------------
# RUN
# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
