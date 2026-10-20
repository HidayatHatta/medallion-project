"""Quality: ukur Silver vs Bronze, simpan ke silver/_quality, gagalkan pipeline jika melanggar."""
import argparse
import calendar
import os
import sys

from clean import get_connection

MAX_DROPPED_PCT = float(os.getenv("MAX_DROPPED_PCT", "15"))


def measure_month(con, ym: str) -> dict:
    year, month = ym.split("-")
    taxi = f"s3://bronze/taxi/year={year}/month={month}/yellow_tripdata_{ym}.parquet"
    weather = f"s3://bronze/weather/year={year}/month={month}/weather_{ym}.json"
    silver = f"s3://silver/trips_weather/year={year}/month={month}/data.parquet"
    out = f"s3://silver/_quality/year={year}/month={month}/data.parquet"
    expected_hours = calendar.monthrange(int(year), int(month))[1] * 24

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE q AS
        WITH b AS (
            SELECT
                count(*) AS bronze_rows,
                count(*) FILTER (WHERE tpep_pickup_datetime < DATE '{ym}-01'
                    OR tpep_pickup_datetime >= DATE '{ym}-01' + INTERVAL 1 MONTH) AS out_of_month,
                count(*) FILTER (WHERE tpep_dropoff_datetime <= tpep_pickup_datetime) AS bad_dropoff,
                count(*) FILTER (WHERE trip_distance <= 0) AS bad_distance,
                count(*) FILTER (WHERE fare_amount <= 0) AS bad_fare,
                count(*) FILTER (WHERE tip_amount < 0) AS bad_tip
            FROM read_parquet('{taxi}')
        ),
        s AS (
            SELECT
                count(*) AS silver_rows,
                count(*) FILTER (WHERE temperature_c IS NULL) AS no_weather,
                count(*) FILTER (WHERE tip_amount < 0) AS negative_tip_in_silver,
                count(*) FILTER (WHERE passenger_count IS NULL) AS null_passenger
            FROM read_parquet('{silver}')
        ),
        w AS (
            SELECT len(hourly.time) AS weather_hours FROM read_json_auto('{weather}')
        )
        SELECT
            '{ym}' AS year_month,
            b.bronze_rows,
            s.silver_rows,
            b.bronze_rows - s.silver_rows AS dropped_rows,
            round((b.bronze_rows - s.silver_rows) * 100.0 / b.bronze_rows, 2) AS dropped_pct,
            b.out_of_month, b.bad_dropoff, b.bad_distance, b.bad_fare, b.bad_tip,
            s.no_weather, s.negative_tip_in_silver, s.null_passenger,
            w.weather_hours, {expected_hours} AS expected_weather_hours,
            now() AS checked_at
        FROM b, s, w
    """)
    con.execute(f"COPY q TO '{out}' (FORMAT PARQUET)")
    cur = con.execute("SELECT * FROM q")
    return dict(zip([d[0] for d in cur.description], cur.fetchone()))


def check(m: dict) -> list[str]:
    """Kembalikan daftar pelanggaran blocking. List kosong = lolos."""
    failures = []
    if m["silver_rows"] == 0:
        failures.append("Silver kosong")
    if m["dropped_pct"] > MAX_DROPPED_PCT:
        failures.append(f"baris dibuang {m['dropped_pct']}% > batas {MAX_DROPPED_PCT}%")
    if m["no_weather"] > 0:
        failures.append(f"{m['no_weather']} baris tanpa cuaca (join gagal)")
    if m["negative_tip_in_silver"] > 0:
        failures.append(f"{m['negative_tip_in_silver']} tip negatif lolos ke Silver")
    if m["weather_hours"] != m["expected_weather_hours"]:
        failures.append(f"jam cuaca {m['weather_hours']} != {m['expected_weather_hours']}")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", required=True)
    args = parser.parse_args()

    con = get_connection()
    failed = False
    for ym in args.months:
        m = measure_month(con, ym)
        problems = check(m)
        info = f"dibuang {m['dropped_pct']}%, null_passenger {m['null_passenger']}"
        if problems:
            failed = True
            print(f"[FAIL] {ym}: " + "; ".join(problems))
        else:
            print(f"[ok]   {ym}: {info}")

    if failed:
        sys.exit(1)  # exit code != 0 -> Airflow menandai task gagal


if __name__ == "__main__":
    main()