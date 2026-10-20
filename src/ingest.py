"""Ingest: sumber -> Bronze (MinIO). Disimpan apa adanya, tanpa transformasi."""
import argparse
import json
import os
import tempfile

import boto3
import requests
from botocore.exceptions import ClientError

# --- Konfigurasi (dari environment, dengan default untuk lokal) ---
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET = "bronze"

TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
)


def object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise  # error lain (auth, jaringan) jangan disembunyikan


def ingest_taxi(ym: str, force: bool = False) -> None:
    year, month = ym.split("-")
    key = f"taxi/year={year}/month={month}/yellow_tripdata_{ym}.parquet"
    if object_exists(key) and not force:
        print(f"[skip] {key} sudah ada")
        return

    # Stream ke file sementara agar file besar tidak memenuhi RAM
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        with requests.get(TAXI_URL.format(ym=ym), stream=True, timeout=60) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                tmp.write(chunk)
        tmp.flush()
        s3.upload_file(tmp.name, BUCKET, key)
    print(f"[ok]   {key}")


def ingest_weather(ym: str, force: bool = False) -> None:
    import calendar

    year, month = ym.split("-")
    key = f"weather/year={year}/month={month}/weather_{ym}.json"
    if object_exists(key) and not force:
        print(f"[skip] {key} sudah ada")
        return

    last_day = calendar.monthrange(int(year), int(month))[1]
    params = {
        "latitude": 40.71,
        "longitude": -74.01,
        "start_date": f"{ym}-01",
        "end_date": f"{ym}-{last_day:02d}",
        "hourly": "temperature_2m,precipitation,rain,snowfall,weather_code",
        "timezone": "America/New_York",
    }
    r = requests.get(WEATHER_URL, params=params, timeout=60)
    r.raise_for_status()

    # Simpan respons apa adanya (belum diratakan, itu tugas Silver)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=r.content,
        ContentType="application/json",
    )
    print(f"[ok]   {key}")


def ingest_zone_lookup(force: bool = False) -> None:
    key = "reference/taxi_zone_lookup.csv"
    if object_exists(key) and not force:
        print(f"[skip] {key} sudah ada")
        return
    r = requests.get(ZONE_URL, timeout=60)
    r.raise_for_status()
    s3.put_object(Bucket=BUCKET, Key=key, Body=r.content, ContentType="text/csv")
    print(f"[ok]   {key}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", required=True, help="contoh: 2025-01 2025-02")
    parser.add_argument("--force", action="store_true", help="timpa file yang sudah ada")
    args = parser.parse_args()

    ingest_zone_lookup(args.force)
    for ym in args.months:
        ingest_taxi(ym, args.force)
        ingest_weather(ym, args.force)


if __name__ == "__main__":
    main()