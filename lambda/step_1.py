import asyncio
import io
import json
import os
from urllib.parse import urljoin, urlparse, urlunparse

import aioboto3
import aiohttp
from bs4 import BeautifulSoup
from loguru import logger

EXECUTION_LEVEL = os.getenv("EXECUTION_LEVEL", "local") # if local then fall back to local execution

# might change get them from environment
BASE_URL = os.getenv("BASE_URL", "https://download.bls.gov/pub/time.series/pr/")
BUCKET_NAME = os.getenv("BUCKET_NAME", "rearc-quest")

if EXECUTION_LEVEL == "local":
    AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
else:
    AWS_ENDPOINT_URL = None
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# default to the program doesnt change
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
}


def parse_url_extract_slugs(url: str) -> tuple[str, str]:
    parsed_url = urlparse(url)
    path = parsed_url.path
    base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, "", "", "", ""))
    return base_url, path


async def get_file_links(session):
    async with session.get(BASE_URL) as resp:
        html = await resp.text()
    soup = BeautifulSoup(html, "html.parser")
    base_url, slug = parse_url_extract_slugs(BASE_URL)
    links = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if href and href.startswith(slug):
            links.append(urljoin(base_url, href))
    return links


async def download_file_and_upload_to_s3(
    session: aiohttp.ClientSession, s3_client: aioboto3.Session, url: str
) -> bool:
    try:
        filename = url.split("/")[-1]
        logger.info(f"Downloading {filename}")
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = io.BytesIO(await resp.read())
        data.seek(0)
        logger.info(f"Uploading {filename} to S3")
        await s3_client.upload_fileobj(Fileobj=data, Bucket=BUCKET_NAME, Key=filename)
        logger.info(f"File {filename} uploaded successfully")
        return True
    except Exception as err:
        logger.error(err)
        return False


async def step_1():

    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    async with session.client("s3", endpoint_url=AWS_ENDPOINT_URL) as s3_client:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            links = await get_file_links(session)
            logger.info(f"Found {len(links)} links")
            logger.info("Starting download and upload")
            tasks = [
                download_file_and_upload_to_s3(session, s3_client, url) for url in links
            ]
            await asyncio.gather(*tasks)
            logger.info("All files uploaded successfully")


def handler(event, context):
    print("Event received:", event)

    # bucket = event.get("bucket")
    # key = event.get("key")

    # obj = s3.get_object(Bucket=bucket, Key=key)
    # data = obj["Body"].read().decode()

    # print("File content:", data)
    asyncio.run(step_1())

    return {"statusCode": 200, "body": json.dumps({"message": "File processed"})}
