from dotenv import load_dotenv
load_dotenv()
import os
from botocore.config import Config as BConfig
import sys
from pdf2docx import Converter

import boto3
from bullmq import Worker
import asyncio

# AWS S3 Client
s3_endpoint = os.getenv('AWS_S3_ENDPOINT', 'https://aooujpoztxhj.ap-northeast-1.clawcloudrun.com')
use_ssl = os.getenv('AWS_S3_USE_SSL', 'true').lower() == 'true'
verify_env = os.getenv('AWS_S3_VERIFY', 'true')
verify_tls = False if verify_env.lower() in ('false', '0', 'no') else (verify_env if os.path.exists(verify_env) else True)

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

async def process_job(job):
    try:
        job_type = job.name
        input_key = job.data['inputKey']
        output_key = job.data['outputKey']
        bucket = job.data['bucket']

        # Download PDF from S3
        input_path = f"/tmp/{os.path.basename(input_key)}"
        s3_client.download_file(bucket, input_key, input_path)

        # Perform conversion
        if job_type == 'convert-to-docx':
            output_path = f"/tmp/{os.path.basename(output_key)}"
            cv = Converter(input_path)
            cv.convert(output_path, start=0, end=None)
            cv.close()
        else:
            raise ValueError(f"Unknown job type: {job_type}")
        

        content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' \
        if job_type == 'convert-to-docx' else 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
        # Upload result to S3
        s3_client.upload_file(
            output_path,
            bucket,
            output_key,
            ExtraArgs={'ContentType': content_type}
        )

        # Clean up
        os.remove(input_path)
        os.remove(output_path)

        print(f"Conversion successful: {output_key}")
    except Exception as e:
        print(f"Error processing job {job.id}: {str(e)}", file=sys.stderr)
        raise

async def main():
    worker = Worker(
        'pdf-conversion-queue',
        process_job,
        {
            'connection': {
                'host': os.getenv('REDIS_HOST', 'redis'),
                'port': os.getenv('REDIS_PORT', 6379),
                'password': os.getenv('REDIS_PASSWORD')
            }
        }
    )
    print("Worker started, waiting for jobs...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())