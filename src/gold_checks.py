"""Audit tabel Gold (sebagai TEMP TABLE) sebelum dipublikasikan ke MinIO."""
import os

MAX_TIP_PCT = float(os.getenv("MAX_TIP_PCT", "50"))

SILVER_ROWS_SQL = (
    "SELECT sum(silver_rows) FROM 's3://silver/_quality/year=*/month=*/data.parquet'"
)

# Grain: kombinasi kolom yang harus unik (primary key logis) per tabel Gold
GRAIN = {
    "daily_trips_by_weather": ["trip_date", "weather_condition"],
    "tip_by_weather": ["weather_condition"],
    "tip_by_period_weather": ["periode", "cuaca"],
    "busiest_zones": ["pickup_borough", "pickup_zone"],
}


def scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


def audit(con) -> list[str]:
    """Kembalikan daftar pelanggaran. List kosong = Gold boleh dipublikasikan."""
    errors: list[str] = []

    # 1. Completeness: total Gold harus cocok dengan Silver
    silver_total = scalar(con, SILVER_ROWS_SQL)
    daily_total = scalar(con, "SELECT sum(trips) FROM daily_trips_by_weather")
    if daily_total != silver_total:
        errors.append(f"daily_trips_by_weather: {daily_total} != total Silver {silver_total}")

    zones_total = scalar(con, "SELECT sum(trips) FROM busiest_zones")
    if not (0.99 * silver_total <= zones_total <= silver_total):
        errors.append(f"busiest_zones: {zones_total} di luar 99-100% dari Silver {silver_total}")

    tip_a = scalar(con, "SELECT sum(card_trips) FROM tip_by_weather")
    tip_b = scalar(con, "SELECT sum(card_trips) FROM tip_by_period_weather")
    if tip_a != tip_b:
        errors.append(f"jumlah perjalanan kartu tidak konsisten: {tip_a} vs {tip_b}")

    # 2. Primary key: grain unik dan tidak ada NULL pada kolom kunci
    for table, cols in GRAIN.items():
        keys = ", ".join(cols)
        dups = scalar(
            con,
            f"SELECT count(*) FROM (SELECT {keys} FROM {table} GROUP BY {keys} HAVING count(*) > 1)",
        )
        if dups:
            errors.append(f"{table}: {dups} kombinasi ({keys}) duplikat")
        null_cond = " OR ".join(f"{c} IS NULL" for c in cols)
        nulls = scalar(con, f"SELECT count(*) FROM {table} WHERE {null_cond}")
        if nulls:
            errors.append(f"{table}: {nulls} baris dengan kolom kunci NULL")

    # 3. Anomaly: rentang nilai yang masuk akal
    for table, col in [("tip_by_weather", "avg_tip_pct"), ("tip_by_period_weather", "tip_pct")]:
        bad = scalar(
            con, f"SELECT count(*) FROM {table} WHERE {col} < 0 OR {col} > {MAX_TIP_PCT}"
        )
        if bad:
            errors.append(f"{table}.{col}: {bad} nilai di luar 0-{MAX_TIP_PCT}%")

    bad_hours = scalar(
        con,
        "SELECT count(*) FROM daily_trips_by_weather "
        "WHERE hours_observed < 1 OR hours_observed > 24 OR trips_per_hour <= 0",
    )
    if bad_hours:
        errors.append(f"daily_trips_by_weather: {bad_hours} baris dengan jam/laju tidak valid")

    bad_fare = scalar(con, "SELECT count(*) FROM busiest_zones WHERE avg_fare <= 0")
    if bad_fare:
        errors.append(f"busiest_zones: {bad_fare} zona dengan avg_fare <= 0")

    return errors