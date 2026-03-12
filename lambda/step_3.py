import asyncio
import io
import json
import os

import aioboto3
import pandas as pd
from loguru import logger

BUCKET_NAME = os.getenv("BUCKET_NAME", "rearc-quest")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

FILE_NAME_1 = os.getenv("FILE_NAME_1", "pr.data.0.Current")
FILE_NAME_2 = os.getenv("FILE_NAME_2", "step_2.json")


async def download_from_s3():

    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    file_1_buffer = io.BytesIO()
    file_2_buffer = io.BytesIO()
    async with session.client("s3", endpoint_url=AWS_ENDPOINT_URL) as s3_client:
        logger.info("Files downloading started")
        await s3_client.download_fileobj(
            Bucket=BUCKET_NAME, Key=FILE_NAME_1, Fileobj=file_1_buffer
        )
        await s3_client.download_fileobj(
            Bucket=BUCKET_NAME, Key=FILE_NAME_2, Fileobj=file_2_buffer
        )
        file_1_buffer.seek(0)
        file_2_buffer.seek(0)
        logger.info("Files downloading completed successfully")

    return file_1_buffer, file_2_buffer


async def download_and_convert_to_dataframe() -> tuple[pd.DataFrame, pd.DataFrame]:
    file_1_buffer, file_2_buffer = await download_from_s3()
    logger.info("Files downloaded successfully")
    df1 = pd.read_csv(file_1_buffer, sep="\t")
    # dataframe 1 needs column name renaming
    columns = df1.columns.tolist()
    df1.columns = [col.strip() for col in columns]
    # file 2 is little trickier
    df2 = pd.DataFrame(json.load(file_2_buffer).get("data", []))
    return df1, df2


def question_1(df: pd.DataFrame):
    filtered = df[(df["Year"] >= 2013) & (df["Year"] <= 2018)]

    mean_population = filtered["Population"].mean()
    std_population = filtered["Population"].std()

    logger.info(f"Mean Population: {mean_population}")
    logger.info(f"Standard Deviation: {std_population}")


def question_2(df: pd.DataFrame):
    # Step 1: yearly sum per series
    yearly = df.groupby(["series_id", "year"], as_index=False)["value"].sum()
    # Step 2: find best year per series
    result = yearly.loc[yearly.groupby("series_id")["value"].idxmax()]
    print(result.head())
    return result


def question_3(df_1: pd.DataFrame, df_2: pd.DataFrame, series_id: str, period: str):
    # Step 1: filter data based on series_id and period
    df_1["period"] = df_1["period"].str.strip()
    df_1["series_id"] = df_1["series_id"].str.strip()
    filtered = df_1[(df_1["series_id"] == series_id) & (df_1["period"] == period)]
    filtered = filtered[["series_id", "period", "value", "year"]]

    # step2 : select needed columns from df_2
    df_2 = df_2[["Population", "Year"]].rename(columns={"Year": "year"})

    # Step 3: merge dataframes and filter the notna values
    merged = pd.merge(filtered, df_2, on="year", how="left")
    merged = merged[merged["Population"].notna()]
    print(merged.head())
    return merged


async def main():
    df1, df2 = await download_and_convert_to_dataframe()
    question_1(df2)
    question_2(df1)
    question_3(df1, df2, "PRS30006032", "Q01")


def handler(event, context):
    print("Event received:", event)

    # bucket = event.get("bucket")
    # key = event.get("key")

    # obj = s3.get_object(Bucket=bucket, Key=key)
    # data = obj["Body"].read().decode()

    # print("File content:", data)
    asyncio.run(main())

    return {"statusCode": 200, "body": json.dumps({"message": "File processed"})}