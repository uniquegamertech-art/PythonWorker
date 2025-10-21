import os
import sys
import asyncio
import logging
from dotenv import load_dotenv
from botocore.config import Config as BConfig
import boto3
from pdf2docx import Converter
from bullmq import Worker

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Environment and S3 setup
# ---------------------------------------------------------------------
load_dotenv()

s3_endpoint = os.getenv('AWS_S3_ENDPOINT', 'https://aooujpoztxhj.ap-northeast-1.clawcloudrun.com')
use_ssl = os.getenv('AWS_S3_USE_SSL', 'true').lower() == 'true'
verify_env = os.getenv('AWS_S3_VERIFY', 'true')
verify_tls = False if verify_env.lower() in ('false', '0', 'no') else (verify_env if os.path.exists(verify_env) else True)

logger.info(f"S3 endpoint: {s3_endpoint}")
logger.info(f"SSL Enabled: {use_ssl}")
logger.info(f"Verify TLS: {verify_tls}")

try:
    s3_client = boto3.client(
        's3',
        endpoint_url=s3_endpoint,
        region_name=os.getenv('AWS_REGION', 'ap-northeast-1'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        use_ssl=use_ssl,
        verify=verify_tls,
        config=BConfig(signature_version='s3v4', s3={'addressing_style': 'path'})
    )
    logger.info("✅ S3 client initialized successfully.")
except Exception as e:
    logger.error(f"❌ Failed to initialize S3 client: {e}")
    sys.exit(1)

# ---------------------------------------------------------------------
# Job processor
# ---------------------------------------------------------------------
async def process_job(job):
    logger.info(f"🚀 Starting job {job.id} of type '{job.name}'")

    try:
        job_type = job.name
        input_key = job.data['inputKey']
        output_key = job.data['outputKey']
        bucket = job.data['bucket']

        input_path = f"/tmp/{os.path.basename(input_key)}"
        output_path = f"/tmp/{os.path.basename(output_key)}"

        logger.info(f"⬇️  Downloading '{input_key}' from bucket '{bucket}'")
        s3_client.download_file(bucket, input_key, input_path)
        logger.info(f"✅ Download complete: {input_path}")

        if job_type == 'convert-to-docx':
            logger.info("🧩 Converting PDF to DOCX...")
            cv = Converter(input_path)
            cv.convert(output_path, start=0, end=None)
            cv.close()
        else:
            raise ValueError(f"Unknown job type: {job_type}")

        content_type = (
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            if job_type == 'convert-to-docx'
            else 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
        )

        logger.info(f"⬆️  Uploading converted file to '{bucket}/{output_key}'")
        s3_client.upload_file(
            output_path,
            bucket,
            output_key,
            ExtraArgs={'ContentType': content_type}
        )

        os.remove(input_path)
        os.remove(output_path)

        logger.info(f"✅ Job {job.id} completed successfully: {output_key}")

    except Exception as e:
        logger.exception(f"❌ Error processing job {job.id}: {str(e)}")
        raise

# ---------------------------------------------------------------------
# Worker setup
# ---------------------------------------------------------------------
async def main():
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_password = os.getenv('REDIS_PASSWORD')

    logger.info(f"Connecting to Redis at {redis_host}:{redis_port}...")

    worker = Worker(
        'pdf-conversion-queue',
        process_job,
        {
            'connection': {
                'host': redis_host,
                'port': redis_port,
                'password': redis_password
            }
        }
    )

    logger.info("👷 Worker started. Waiting for jobs...")
    await worker.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("🛑 Worker manually stopped by user.")
