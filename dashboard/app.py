"""Dashboard: membaca layer Gold (MinIO) lewat DuckDB. Tidak menyentuh Bronze/Silver."""
import sys
from pathlib import Path
import altair as alt

import streamlit as st

# Pakai koneksi S3 yang sama dengan pipeline (satu sumber konfigurasi)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from clean import get_connection  # noqa: E402

st.set_page_config(page_title="NYC Taxi x Cuaca", layout="wide")


@st.cache_resource
def connection():
    return get_connection()


@st.cache_data(ttl=300)
def gold(table: str):
    return connection().sql(f"SELECT * FROM 's3://gold/{table}/data.parquet'").df()


@st.cache_data(ttl=300)
def quality():
    return connection().sql(
        "SELECT * FROM 's3://silver/_quality/year=*/month=*/data.parquet'"
    ).df()


st.title("NYC Yellow Taxi x Cuaca")
st.caption("Januari-Maret 2025 | Pipeline: MinIO + DuckDB + Airflow | Sumber: NYC TLC dan Open-Meteo")

daily = gold("daily_trips_by_weather")
zones = gold("busiest_zones")
tip_weather = gold("tip_by_weather")
tip_period = gold("tip_by_period_weather")
q = quality()

# --- KPI ---
total_trips = int(daily["trips"].sum())
bronze = int(q["bronze_rows"].sum())
dropped_pct = (bronze - int(q["silver_rows"].sum())) * 100 / bronze

c1, c2, c3 = st.columns(3)
c1.metric("Total perjalanan (bersih)", f"{total_trips:,}")
c2.metric("Baris dibuang saat pembersihan", f"{dropped_pct:.2f}%")
c3.metric("Zona pickup", f"{zones['pickup_zone'].nunique()}")

# --- 1. Tren harian ---

# st.subheader("Rata-rata perjalanan per jam menurut kondisi cuaca")
# chart = (
#     alt.Chart(daily)
#     .mark_line(point=True)
#     .encode(
#         x=alt.X("trip_date:T", title="Tanggal"),
#         y=alt.Y("trips_per_hour:Q", title="Perjalanan per jam"),
#         color=alt.Color("weather_condition:N", title="Cuaca"),
#         tooltip=["trip_date", "weather_condition", "trips_per_hour", "hours_observed"],
#     )
# )
# st.altair_chart(chart, width="stretch")

st.subheader("Perjalanan per hari menurut kondisi cuaca")
pivot = daily.pivot(index="trip_date", columns="weather_condition", values="trips_per_hour")
st.line_chart(pivot)
st.caption(
    "**Catatan Visualisasi:** Grafik di atas menampilkan metrik **rata-rata perjalanan per jam (trips_per_hour)**, "
    "bukan total perjalanan harian. Hal ini dilakukan agar perbandingan antar cuaca tetap adil, mengingat durasi hujan "
    "dalam sehari seringkali jauh lebih singkat daripada durasi cuaca kering. Kolom raw 'trips' dan 'hours_observed' "
    "tetap tersedia di dataset bagi pengguna yang ingin memverifikasi kalkulasinya."
)
st.caption(
    "Kategori cuaca diturunkan dari data per jam: 'hujan' jika rain > 0, 'salju' jika snowfall > 0, "
    "selain itu 'kering' (termasuk berawan). Sebuah hari bisa memuat beberapa kategori."
)

# --- 2. Tip hujan vs kering ---
st.subheader("Tip: hujan vs kering (pembayaran kartu)")
left, right = st.columns(2)
with left:
    st.markdown("**Gabungan (belum dikontrol)**")
    st.dataframe(
        tip_weather.rename(columns={"avg_tip_pct": "tip_pct (sum tip / sum tarif)"}),
        hide_index=True,
    )
with right:
    st.markdown("**Per periode hari (dikontrol)**")
    st.dataframe(
        tip_period.pivot(index="periode", columns="cuaca", values="tip_pct"),
    )
st.info(
    "Setelah dikelompokkan per periode, selisih tip hujan vs kering nol atau sedikit negatif "
    "di semua periode. Tidak ada bukti efek hujan yang konsisten. Analisis ini belum "
    "mengontrol zona, jarak, atau musim lain."
)

# --- 3. Zona tersibuk ---
st.subheader("10 zona pickup tersibuk")
top = zones.sort_values("trips", ascending=False).head(10)
st.bar_chart(top.set_index("pickup_zone")["trips"], horizontal=True)
st.dataframe(top, hide_index=True)

# --- 4. Kualitas data ---
with st.expander("Kualitas data per bulan (dari pipeline)"):
    st.dataframe(q.sort_index(axis=1), hide_index=True)