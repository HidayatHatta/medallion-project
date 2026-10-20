"""Clean: Bronze -> Silver. Bersihkan taxi, ratakan cuaca, join per jam."""
import argparse
import os

import duckdb

RAW_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ENDPOINT = RAW_ENDPOINT.replace("http://", "").replace("https://", "")
S3_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")


def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{S3_ENDPOINT}';
        SET s3_access_key_id='{S3_KEY}';
        SET s3_secret_access_key='{S3_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con


def clean_month(con, ym: str) -> None:
    year, month = ym.split("-")
    taxi_path = f"s3://bronze/taxi/year={year}/month={month}/yellow_tripdata_{ym}.parquet"
    weather_path = f"s3://bronze/weather/year={year}/month={month}/weather_{ym}.json"
    zone_path = "s3://bronze/reference/taxi_zone_lookup.csv"
    out_path = f"s3://silver/trips_weather/year={year}/month={month}/data.parquet"

    con.execute(f"""
        COPY (
            WITH weather AS (
                SELECT
                    CAST(t AS TIMESTAMP) AS weather_hour,
                    temp AS temperature_c,
                    precip AS precipitation_mm,
                    rain AS rain_mm,
                    snow AS snowfall_cm,
                    code AS weather_code
                FROM (
                    SELECT
                        unnest(hourly.time) AS t,
                        unnest(hourly.temperature_2m) AS temp,
                        unnest(hourly.precipitation) AS precip,
                        unnest(hourly.rain) AS rain,
                        unnest(hourly.snowfall) AS snow,
                        unnest(hourly.weather_code) AS code
                    FROM read_json_auto('{weather_path}')
                )
            ),
            zones AS (
                SELECT LocationID, Borough, Zone FROM read_csv_auto('{zone_path}')
            ),
            taxi AS (
                SELECT DISTINCT
                    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance, PULocationID, DOLocationID,
                    payment_type, fare_amount, tip_amount, total_amount
                FROM read_parquet('{taxi_path}')
                WHERE tpep_pickup_datetime >= DATE '{ym}-01'
                  AND tpep_pickup_datetime < DATE '{ym}-01' + INTERVAL 1 MONTH
                  AND tpep_dropoff_datetime > tpep_pickup_datetime
                  AND trip_distance > 0
                  AND fare_amount > 0
                  AND tip_amount >= 0
                  AND (passenger_count IS NULL OR passenger_count >= 0)
            )
            SELECT
                t.*,
                date_trunc('hour', t.tpep_pickup_datetime) AS pickup_hour,
                z.Borough AS pickup_borough,
                z.Zone AS pickup_zone,
                w.temperature_c, w.precipitation_mm, w.rain_mm,
                w.snowfall_cm, w.weather_code
            FROM taxi t
            LEFT JOIN weather w
                ON date_trunc('hour', t.tpep_pickup_datetime) = w.weather_hour
            LEFT JOIN zones z
                ON t.PULocationID = z.LocationID
        ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    print(f"[ok] {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", required=True)
    args = parser.parse_args()
    con = get_connection()
    for ym in args.months:
        clean_month(con, ym)


if __name__ == "__main__":
    main()