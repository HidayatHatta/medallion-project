"""Aggregate: Silver -> Gold. Pola write-audit-publish:
hitung semua tabel (temp) -> audit -> baru tulis ke MinIO."""
import sys

from clean import get_connection
from gold_checks import audit

SILVER = "s3://silver/trips_weather/year=*/month=*/data.parquet"

WEATHER_CASE = """
    CASE
        WHEN rain_mm > 0 THEN 'hujan'
        WHEN snowfall_cm > 0 THEN 'salju'
        ELSE 'kering'
    END
"""

PERIOD_CASE = """
    CASE WHEN hour(pickup_hour) BETWEEN 7 AND 9 THEN 'pagi sibuk'
         WHEN hour(pickup_hour) BETWEEN 17 AND 19 THEN 'sore sibuk'
         WHEN hour(pickup_hour) >= 22 OR hour(pickup_hour) < 5 THEN 'malam'
         ELSE 'lainnya' END
"""

QUERIES = {
    "daily_trips_by_weather": f"""
        SELECT
            CAST(pickup_hour AS DATE) AS trip_date,
            {WEATHER_CASE} AS weather_condition,
            count(*) AS trips,
            count(DISTINCT pickup_hour) AS hours_observed,
            round(count(*) * 1.0 / count(DISTINCT pickup_hour), 0) AS trips_per_hour,
            round(avg(fare_amount), 2) AS avg_fare
        FROM '{SILVER}'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "tip_by_weather": f"""
        SELECT
            {WEATHER_CASE} AS weather_condition,
            count(*) AS card_trips,
            round(avg(tip_amount), 3) AS avg_tip,
            round(sum(tip_amount) / sum(fare_amount) * 100, 2) AS avg_tip_pct
        FROM '{SILVER}'
        WHERE payment_type = 1
        GROUP BY 1
        ORDER BY 1
    """,
    "tip_by_period_weather": f"""
        SELECT
            {PERIOD_CASE} AS periode,
            CASE WHEN rain_mm > 0 THEN 'hujan' ELSE 'kering' END AS cuaca,
            count(*) AS card_trips,
            round(sum(tip_amount) / sum(fare_amount) * 100, 2) AS tip_pct
        FROM '{SILVER}'
        WHERE payment_type = 1
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "busiest_zones": f"""
        SELECT
            pickup_borough,
            pickup_zone,
            count(*) AS trips,
            round(avg(fare_amount), 2) AS avg_fare
        FROM '{SILVER}'
        WHERE pickup_zone IS NOT NULL
        GROUP BY 1, 2
        ORDER BY trips DESC
    """,
}


def main() -> None:
    con = get_connection()

    # 1. WRITE (sementara): hitung semua tabel di memori
    for name, query in QUERIES.items():
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS {query}")

    # 2. AUDIT: gagal = tidak ada satu pun tabel Gold yang disentuh
    errors = audit(con)
    if errors:
        for e in errors:
            print(f"[FAIL] {e}")
        sys.exit(1)

    # 3. PUBLISH: tulis ke Gold hanya jika audit lolos
    for name in QUERIES:
        out = f"s3://gold/{name}/data.parquet"
        con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"[ok] {out}")


if __name__ == "__main__":
    main()