import asyncio
import io
import json
import os
from tokenize import RSQB
from typing import Any
from urllib.parse import urljoin

import aioboto3
import aiohttp
from loguru import logger

# URL related params from environment as per the documentation
BASE_URL = os.getenv("BASE_URL", "https://honolulu-api.datausa.io/tesseract/")
# format lets give an option but lets also choose JSON as default
FORMAT = os.getenv("FORMAT", "jsonrecords")
PARAM_CUBES = os.getenv("PARAM_CUBES", "acs_yg_total_population_1")
PARAM_DRILLDOWN = os.getenv("PARAM_DRILLDOWN", "Year,Nation")
PARAM_MEASURES = os.getenv("PARAM_MEASURES", "Population")

BUCKET_NAME = os.getenv("BUCKET_NAME", "rearc-quest")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def generate_url():
    base_url = BASE_URL
    # we handle the trailing slash and ensure the URL is properly formatted
    base_url = base_url.rstrip("/")
    url = urljoin(f"{base_url}/", f"data.{FORMAT}")
    return url


async def fetch_data() -> dict[Any, Any]:
    url = generate_url()
    params = {
        "cube": PARAM_CUBES,
        "drilldowns": PARAM_DRILLDOWN,
        "measures": PARAM_MEASURES,
    }
    response_data = {}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={}, params=params) as response:
            # Read response text
            response_data = await response.json()
    return response_data


async def hit_api_and_upload_to_s3(s3_client: aioboto3.Session, filename: str) -> bool:
    try:
        logger.info(f"Fetching data from API")
        response = await fetch_data()
        data = io.BytesIO(json.dumps(response).encode("utf-8"))
        data.seek(0)
        logger.info("fetching from API completed")
        logger.info(f"Uploading {filename} to S3")
        await s3_client.upload_fileobj(Fileobj=data, Bucket=BUCKET_NAME, Key=filename)
        logger.info(f"File {filename} uploaded successfully")
        return True
    except Exception as err:
        logger.error(err)
        return False


async def step_2():

    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    async with session.client("s3", endpoint_url=AWS_ENDPOINT_URL) as s3_client:
        file_name = "step_2.json"
        await hit_api_and_upload_to_s3(s3_client, file_name)
        logger.info(f"File {file_name} uploaded successfully")


def handler(event, context):
    print("Event received:", event)

    # bucket = event.get("bucket")
    # key = event.get("key")

    # obj = s3.get_object(Bucket=bucket, Key=key)
    # data = obj["Body"].read().decode()

    # print("File content:", data)
    asyncio.run(step_2())

    return {"statusCode": 200, "body": json.dumps({"message": "File processed"})}
