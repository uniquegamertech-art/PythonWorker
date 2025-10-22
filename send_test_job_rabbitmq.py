import os
import json
import pika
from dotenv import load_dotenv

load_dotenv()

url = os.getenv('CLOUDAMQP_URL')
if not url:
    print("CLOUDAMQP_URL not set!")
    exit(1)

params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue="pdf-conversion-queue", durable=True)

job_data = {
    "bucket": "pdf-converter-files",
    "inputKey": "uploads/test.pdf",
    "outputKey": "downloads/test.docx"
}

channel.basic_publish(
    exchange='',
    routing_key="pdf-conversion-queue",
    body=json.dumps(job_data).encode(),
    properties=pika.BasicProperties(delivery_mode=2)  # Persistent
)

print(f"Job sent: {job_data}")
connection.close()
