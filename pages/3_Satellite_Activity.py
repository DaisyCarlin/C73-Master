import html
import math
from datetime import datetime, timezone
from pathlib import Path

import folium
import pandas as pd
import requests
import streamlit as st
from folium.features import DivIcon
from folium.plugins import Fullscreen, MousePosition
from requests.adapters import HTTPAdapter
from sgp4 import omm
from sgp4.api import Satrec, jday
from streamlit_folium import st_folium
from urllib3.util.retry import Retry

st.set_page_config(page_title="Strategic Space Watch", layout="wide")

SPACE_TRACK_LOGIN_URL = "https://www.space-track.org/ajaxauth/login"
SPACE_TRACK_GP_URL = (
    "https://www.space-track.org/basicspacedata/query/"
    "class/gp/"
    "decay_date/null-val/"
    "epoch/%3Enow-10/"
    "orderby/norad_cat_id/"
    "format/json"
)

REQUEST_TIMEOUT_SECONDS = 12
QUERY_CACHE_TTL_SECONDS = 3600
DISK_CACHE_FILE = "spacetrack_gp_cache.pkl"
AUTO_REFRESH_SECONDS = 3600

MAP_THEMES = {
    "Light": {"tiles": "CartoDB positron", "attr": None},
    "Radar": {
        "tiles": "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        "attr": "&copy; OpenStreetMap contributors &copy; CARTO",
    },
    "Dark": {"tiles": "CartoDB dark_matter", "attr": None},
}

GROUP_COLORS = {
    "Military / Intel": "#ff5f6d",
    "Chinese Strategic": "#ff9e3d",
    "Russian Strategic": "#58a6ff",
    "Navigation": "#39d98a",
    "Other Strategic": "#c084fc",
}
DEFAULT_MARKER_COLOR = "#ff5f6d"


def inject_styles():
    st.markdown(
        """
        <style>
            :root {
                --bg-0:#07111f;
                --bg-1:#0d1b2a;
                --stroke:rgba(130,161,191,.22);
                --text-main:#e8f1fb;
                --text-soft:#91a9c3;
                --panel-bg:linear-gradient(180deg, rgba(10,23,37,.9), rgba(14,31,49,.82));
                --hero-bg:linear-gradient(145deg, rgba(10,21,35,.92), rgba(15,31,49,.86));
                --metric-bg:linear-gradient(180deg, rgba(12,24,39,.9), rgba(14,32,50,.76));
            }

            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(56,189,248,.16), transparent 28%),
                    radial-gradient(circle at top right, rgba(88,166,255,.12), transparent 26%),
                    linear-gradient(180deg, var(--bg-0) 0%, var(--bg-1) 100%);
                color:var(--text-main);
                font-family:"Aptos","Segoe UI",sans-serif;
            }

            [data-testid="stSidebar"] {
                background:linear-gradient(180deg, rgba(9,19,32,.97), rgba(9,19,32,.92));
                border-right:1px solid var(--stroke);
            }

            [data-testid="stSidebar"] * { color:var(--text-main); }

            .hero-card,.panel-card,.metric-card,.insight-card {
                border:1px solid var(--stroke);
                border-radius:22px;
                box-shadow:0 12px 28px rgba(4,9,18,.22);
            }

            .hero-card {
                background:var(--hero-bg);
                padding:1.4rem 1.6rem;
                margin-bottom:1rem;
            }

            .panel-card {
                background:var(--panel-bg);
                padding:1rem 1rem .85rem 1rem;
                margin-bottom:.75rem;
            }

            .metric-card {
                background:var(--metric-bg);
                padding:1rem 1rem .95rem 1rem;
                min-height:120px;
            }

            .insight-card {
                background:var(--panel-bg);
                padding:1rem 1rem .95rem 1rem;
                min-height:132px;
                margin-bottom:.7rem;
            }

            .hero-kicker {
                letter-spacing:.16rem;
                font-size:.72rem;
                font-weight:700;
                color:#84d7ff;
                margin-bottom:.4rem;
                text-transform:uppercase;
            }

            .hero-title {
                font-size:2.45rem;
                line-height:1.04;
                font-weight:750;
                margin:0;
                color:var(--text-main);
            }

            .hero-copy,.panel-copy,.metric-detail,.insight-copy {
                color:var(--text-soft);
                font-size:.96rem;
                line-height:1.55;
            }

            .metric-label {
                font-size:.8rem;
                text-transform:uppercase;
                letter-spacing:.08rem;
                color:var(--text-soft);
                margin-bottom:.45rem;
            }

            .metric-value {
                font-size:2rem;
                font-weight:740;
                line-height:1;
                margin-bottom:.35rem;
                color:var(--text-main);
            }

            .accent-bar {
                width:54px;
                height:4px;
                border-radius:999px;
                margin-bottom:.8rem;
            }

            .panel-title, .insight-title {
                font-size:1rem;
                font-weight:700;
                margin-bottom:.25rem;
                color:var(--text-main);
            }

            .chip-row {
                display:flex;
                flex-wrap:wrap;
                gap:.45rem;
                margin-top:.45rem;
            }

            .chip {
                display:inline-block;
                padding:.28rem .6rem;
                border-radius:999px;
                font-size:.76rem;
                font-weight:700;
                color:#dff4ff;
                background:rgba(56,189,248,.14);
                border:1px solid rgba(56,189,248,.24);
            }

            .stDataFrame, div[data-testid="stTable"] {
                border-radius:18px;
                overflow:hidden;
                border:1px solid var(--stroke);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def inject_hourly_refresh(enabled: bool):
    if not enabled:
        return

    st.markdown(
        f"""
        <script>
            (function() {{
                if (window.spaceRadarHourlyRefreshSet) return;
                window.spaceRadarHourlyRefreshSet = true;
                setTimeout(function() {{
                    window.location.reload();
                }}, {AUTO_REFRESH_SECONDS * 1000});
            }})();
        </script>
        """,
        unsafe_allow_html=True,
    )


def render_metric_card(title, value, detail, accent):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="accent-bar" style="background:{accent};"></div>
            <div class="metric-label">{html.escape(str(title))}</div>
            <div class="metric-value">{html.escape(str(value))}</div>
            <div class="metric-detail">{html.escape(str(detail))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_insight_card(title, value, copy):
    st.markdown(
        f"""
        <div class="insight-card">
            <div class="insight-title">{html.escape(str(title))}</div>
            <div style="font-size:1.7rem; font-weight:760; margin:.18rem 0 .38rem 0;">{html.escape(str(value))}</div>
            <div class="insight-copy">{html.escape(str(copy))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def safe_str(value):
    return "" if value is None else str(value).strip()


def format_time(value):
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    return "Unknown" if pd.isna(parsed) else parsed.strftime("%Y-%m-%d %H:%M UTC")


def orbit_regime(altitude_km):
    if pd.isna(altitude_km):
        return "Unknown"
    if altitude_km < 2000:
        return "LEO"
    if altitude_km < 30000:
        return "MEO"
    if altitude_km <= 37000:
        return "GEO"
    return "HEO"


def strategic_country_display(code: str) -> str:
    code = safe_str(code).upper()
    mapping = {
        "US": "United States",
        "RU": "Russia",
        "CN": "China",
        "JP": "Japan",
        "IN": "India",
        "FR": "France",
        "GB": "United Kingdom",
        "KZ": "Kazakhstan",
        "CIS": "CIS / Russia-led systems",
        "ITSO": "ITSO",
    }
    return mapping.get(code, code if code else "Unknown")


def top_country_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"label": "None", "count": 0, "share": 0.0}

    counts = df.groupby("country").size().sort_values(ascending=False)
    code = str(counts.index[0])
    count = int(counts.iloc[0])
    total = max(int(counts.sum()), 1)
    share = count / total * 100.0
    return {"label": strategic_country_display(code), "count": count, "share": share}


def top_group_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"label": "None", "count": 0, "share": 0.0}

    counts = df.groupby("strategic_group").size().sort_values(ascending=False)
    label = str(counts.index[0])
    count = int(counts.iloc[0])
    total = max(int(counts.sum()), 1)
    share = count / total * 100.0
    return {"label": label, "count": count, "share": share}


def top_orbit_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"label": "None", "count": 0, "share": 0.0}

    counts = df.groupby("orbit_regime").size().sort_values(ascending=False)
    label = str(counts.index[0])
    count = int(counts.iloc[0])
    total = max(int(counts.sum()), 1)
    share = count / total * 100.0
    return {"label": label, "count": count, "share": share}


def build_top_signal(df: pd.DataFrame) -> str:
    if df.empty:
        return "No signal is available for the current filters."

    country = top_country_stats(df)
    group = top_group_stats(df)
    orbit = top_orbit_stats(df)

    return (
        f"{country['label']} is the biggest footprint in this view "
        f"({country['share']:.0f}% of tracked assets). "
        f"The main group is {group['label']}, and the main orbit is {orbit['label']}."
    )


def is_strategic_asset(name: str) -> bool:
    text = safe_str(name).upper()
    keywords = [
        "NROL", "USA ", "KH-", "SBIRS", "AEHF", "MUOS", "MILSTAR", "DSP", "GSSAP",
        "TRUMPET", "LACROSSE", "ONYX", "YAOGAN", "TIANHUI", "GAOFEN", "COSMOS",
        "GLONASS", "NAVSTAR", "GPS", "BEIDOU", "GALILEO", "IRNSS", "NAVIC",
        "QZSS", "DEFENCE", "DEFENSE", "INTEL",
    ]
    return any(keyword in text for keyword in keywords)


def strategic_group(name: str) -> str:
    text = safe_str(name).upper()

    if any(k in text for k in ["NROL", "USA ", "KH-", "SBIRS", "AEHF", "MUOS", "MILSTAR", "DSP", "GSSAP", "TRUMPET", "LACROSSE", "ONYX"]):
        return "Military / Intel"
    if any(k in text for k in ["YAOGAN", "TIANHUI", "GAOFEN"]):
        return "Chinese Strategic"
    if any(k in text for k in ["COSMOS", "GLONASS"]):
        return "Russian Strategic"
    if any(k in text for k in ["NAVSTAR", "GPS", "BEIDOU", "GALILEO", "IRNSS", "NAVIC", "QZSS"]):
        return "Navigation"
    return "Other Strategic"


def build_session():
    session = requests.Session()
    retry = Retry(
        total=1,
        connect=1,
        read=1,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=6, pool_maxsize=6)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "Console7-SpaceRadar/1.0"})
    return session


def to_julian(dt):
    return jday(
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second + dt.microsecond / 1_000_000,
    )


def sidereal_angle(jd_full):
    t = (jd_full - 2451545.0) / 36525.0
    gmst_deg = (
        280.46061837
        + 360.98564736629 * (jd_full - 2451545.0)
        + 0.000387933 * (t**2)
        - (t**3) / 38710000.0
    )
    return math.radians(gmst_deg % 360.0)


def eci_to_latlonalt(position_km, jd_full):
    x, y, z = position_km
    theta = sidereal_angle(jd_full)

    x_ecef = x * math.cos(theta) + y * math.sin(theta)
    y_ecef = -x * math.sin(theta) + y * math.cos(theta)
    z_ecef = z

    a = 6378.137
    f = 1 / 298.257223563
    e2 = f * (2 - f)

    lon = math.atan2(y_ecef, x_ecef)
    r = math.hypot(x_ecef, y_ecef)
    lat = math.atan2(z_ecef, r)

    for _ in range(6):
        n = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
        alt = r / max(math.cos(lat), 1e-9) - n
        lat = math.atan2(z_ecef, r * (1 - e2 * n / (n + alt)))

    n = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
    alt = r / max(math.cos(lat), 1e-9) - n

    return math.degrees(lat), ((math.degrees(lon) + 180) % 360) - 180, alt


def propagate_from_record(record, dt):
    try:
        sat = Satrec()
        omm.initialize(sat, record)
        jd, fr = to_julian(dt)
        error, position_km, velocity_kms = sat.sgp4(jd, fr)
        if error != 0:
            return None

        lat, lon, alt = eci_to_latlonalt(position_km, jd + fr)
        speed = math.sqrt(sum(v * v for v in velocity_kms))
        return lat, lon, alt, speed
    except Exception:
        return None


def search_blob(row):
    return " ".join(
        [
            safe_str(row.get("name")),
            safe_str(row.get("norad_id")),
            safe_str(row.get("country")),
            safe_str(row.get("orbit_regime")),
            safe_str(row.get("strategic_group")),
            safe_str(row.get("object_type")),
        ]
    ).lower()


def save_cache(full_df, loaded_at_iso, error_message):
    payload = {"df": full_df, "loaded_at_iso": loaded_at_iso, "error_message": error_message}
    pd.to_pickle(payload, DISK_CACHE_FILE)


def load_cache():
    path = Path(DISK_CACHE_FILE)
    if not path.exists():
        return None

    try:
        payload = pd.read_pickle(path)
        df = payload.get("df")
        if df is None or df.empty:
            return None
        return df, payload.get("loaded_at_iso"), payload.get("error_message")
    except Exception:
        return None


@st.cache_data(ttl=QUERY_CACHE_TTL_SECONDS, show_spinner=False)
def fetch_spacetrack_gp(_identity, _password):
    session = build_session()

    login_response = session.post(
        SPACE_TRACK_LOGIN_URL,
        data={"identity": _identity, "password": _password},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    login_response.raise_for_status()

    response = session.get(SPACE_TRACK_GP_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("Space-Track returned no GP records.")

    return payload


def build_full_dataset(identity, password):
    now_utc = datetime.now(timezone.utc)
    raw_records = fetch_spacetrack_gp(identity, password)

    rows = []
    for record in raw_records:
        name = record.get("OBJECT_NAME") or f"NORAD {record.get('NORAD_CAT_ID', 'Unknown')}"

        if not is_strategic_asset(name):
            continue

        state = propagate_from_record(record, now_utc)
        if not state:
            continue

        lat, lon, alt, speed = state
        group = strategic_group(name)

        rows.append(
            {
                "name": name,
                "norad_id": str(record.get("NORAD_CAT_ID", "")),
                "category": "Strategic asset",
                "strategic_group": group,
                "object_type": record.get("OBJECT_TYPE", ""),
                "country": (record.get("COUNTRY_CODE", "") or "Unknown").upper(),
                "launch_date": record.get("LAUNCH_DATE", ""),
                "epoch": record.get("EPOCH", ""),
                "latitude": lat,
                "longitude": lon,
                "altitude_km": alt,
                "speed_kms": speed,
                "orbit_regime": orbit_regime(alt),
                "marker_color": GROUP_COLORS.get(group, DEFAULT_MARKER_COLOR),
            }
        )

    if not rows:
        raise RuntimeError("No usable strategic satellite positions were produced from the current Space-Track response.")

    df = pd.DataFrame(rows)
    df["search_blob"] = df.apply(search_blob, axis=1)

    loaded_at_iso = now_utc.isoformat()
    save_cache(df, loaded_at_iso, None)
    return df.reset_index(drop=True), loaded_at_iso


def sample_visual_dataset(full_df, limit_count):
    if full_df.empty:
        return full_df.iloc[0:0].copy()

    groups = sorted(full_df["strategic_group"].dropna().unique().tolist())
    if not groups:
        return full_df.head(limit_count).copy()

    per_group = max(1, limit_count // len(groups))
    frames = []

    for group in groups:
        subset = full_df[full_df["strategic_group"] == group].copy()
        subset = subset.sort_values(["country", "name"]).head(per_group)
        if not subset.empty:
            frames.append(subset)

    sampled = pd.concat(frames, ignore_index=True) if frames else full_df.iloc[0:0].copy()

    if len(sampled) < limit_count:
        already = set(sampled["norad_id"].tolist()) if "norad_id" in sampled.columns else set()
        remainder = full_df[~full_df["norad_id"].isin(already)].sort_values(["country", "name"]).head(limit_count - len(sampled))
        sampled = pd.concat([sampled, remainder], ignore_index=True)

    return sampled.head(limit_count).reset_index(drop=True)


def load_dataset(identity, password, limit_count):
    try:
        full_df, loaded_at_iso = build_full_dataset(identity, password)
        visual_df = sample_visual_dataset(full_df, limit_count)
        return full_df, visual_df, loaded_at_iso, "live", None
    except Exception as error:
        cached = load_cache()
        if cached is not None:
            cached_full_df, cached_loaded_at, _ = cached
            visual_df = sample_visual_dataset(cached_full_df, limit_count)
            return cached_full_df, visual_df, cached_loaded_at, "cached_live", str(error)
        return pd.DataFrame(), pd.DataFrame(), None, "unavailable", str(error)


def apply_filters(df, search_query, regimes):
    filtered = df.copy()

    if search_query and "search_blob" in filtered.columns:
        filtered = filtered[filtered["search_blob"].str.contains(search_query.lower(), na=False)]

    if regimes and "orbit_regime" in filtered.columns:
        filtered = filtered[filtered["orbit_regime"].isin(regimes)]

    return filtered.reset_index(drop=True)


def popup_html(row):
    return f"""
        <div style="min-width:280px; font-family:Segoe UI,sans-serif;">
            <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; margin-bottom:8px;">
                <div>
                    <div style="font-size:15px; font-weight:700; color:#09111f;">{html.escape(safe_str(row.get("name") or "Unknown object"))}</div>
                    <div style="font-size:12px; color:#5a6d85;">NORAD {html.escape(safe_str(row.get("norad_id") or "Unknown"))}</div>
                </div>
                <div style="background:{row.get("marker_color", DEFAULT_MARKER_COLOR)}; color:#fff; font-size:11px; font-weight:700; border-radius:999px; padding:5px 8px;">
                    {html.escape(safe_str(row.get("strategic_group") or "Strategic"))}
                </div>
            </div>
            <table style="width:100%; border-collapse:collapse; font-size:12px;">
                <tr><td style="padding:4px 0; color:#5a6d85;">Type</td><td style="padding:4px 0;">{html.escape(safe_str(row.get("object_type") or "Unknown"))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Country</td><td style="padding:4px 0;">{html.escape(strategic_country_display(row.get("country") or "Unknown"))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Orbit</td><td style="padding:4px 0;">{html.escape(safe_str(row.get("orbit_regime") or "Unknown"))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Group</td><td style="padding:4px 0;">{html.escape(safe_str(row.get("strategic_group") or "Unknown"))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Altitude</td><td style="padding:4px 0;">{float(row.get("altitude_km")):,.0f} km</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Velocity</td><td style="padding:4px 0;">{float(row.get("speed_kms")):.2f} km/s</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Epoch</td><td style="padding:4px 0;">{html.escape(format_time(row.get("epoch")))}</td></tr>
            </table>
        </div>
    """


def satellite_icon_html(row, show_label):
    color = row.get("marker_color", DEFAULT_MARKER_COLOR)
    label_html = ""

    if show_label:
        label = html.escape((safe_str(row.get("name")) or "Satellite")[:16])
        label_html = f'<div style="margin-top:3px; padding:2px 7px; border-radius:999px; background:rgba(7,17,31,.9); color:#f4f9ff; font-size:10px; font-weight:700; text-align:center; white-space:nowrap;">{label}</div>'

    return f"""
        <div style="position:relative; width:38px; height:38px; transform:translate(-19px,-19px);">
            <div style="width:38px; height:38px; border-radius:999px; background:rgba(8,18,30,.84); box-shadow:0 0 0 1px rgba(255,255,255,.14), 0 12px 26px {color}55; display:flex; align-items:center; justify-content:center;">
                <svg viewBox="0 0 32 32" width="24" height="24">
                    <circle cx="16" cy="16" r="5.5" fill="{color}" stroke="#ffffff" stroke-width="1"></circle>
                    <ellipse cx="16" cy="16" rx="11.5" ry="5.5" fill="none" stroke="#ffffff" stroke-width="1.1" opacity="0.85"></ellipse>
                    <path d="M7 16h3.5M21.5 16H25M16 5v3M16 24v3" stroke="#ffffff" stroke-width="1" opacity="0.72"></path>
                </svg>
            </div>
            {label_html}
        </div>
    """


def create_map(df, map_theme, show_labels):
    coords = df.dropna(subset=["latitude", "longitude"]).copy()
    if coords.empty:
        return None, False

    satellite_map = folium.Map(location=[18, 0], zoom_start=2, control_scale=True, prefer_canvas=True, tiles=None)

    for theme_name, theme_config in MAP_THEMES.items():
        folium.TileLayer(
            tiles=theme_config["tiles"],
            attr=theme_config["attr"],
            name=theme_name,
            show=theme_name == map_theme,
        ).add_to(satellite_map)

    Fullscreen(position="topright").add_to(satellite_map)
    MousePosition(position="bottomright", separator=" | ", lng_first=False, num_digits=3, prefix="Lat / Lon").add_to(satellite_map)

    marker_layer = folium.FeatureGroup(name="Strategic assets", show=True)
    effective_labels = show_labels and len(coords) <= 60

    for _, row in coords.iterrows():
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            tooltip=f"{safe_str(row.get('name'))} | {safe_str(row.get('strategic_group'))}",
            popup=folium.Popup(popup_html(row), max_width=360),
            icon=DivIcon(html=satellite_icon_html(row, effective_labels)),
        ).add_to(marker_layer)

    marker_layer.add_to(satellite_map)
    folium.LayerControl(collapsed=True).add_to(satellite_map)
    return satellite_map, effective_labels


def build_takeaways(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return ["No strategic satellites match the current filters."]

    country_counts = df.groupby("country").size().sort_values(ascending=False)
    group_counts = df.groupby("strategic_group").size().sort_values(ascending=False)
    orbit_counts = df.groupby("orbit_regime").size().sort_values(ascending=False)

    total_count = max(int(country_counts.sum()), 1)

    top_country_code = str(country_counts.index[0])
    top_country_name = strategic_country_display(top_country_code)
    top_country_count = int(country_counts.iloc[0])
    top_country_share = top_country_count / total_count * 100.0

    top_group_name = str(group_counts.index[0])
    top_group_share = int(group_counts.iloc[0]) / total_count * 100.0

    top_orbit_name = str(orbit_counts.index[0])
    top_orbit_share = int(orbit_counts.iloc[0]) / total_count * 100.0

    top_three_share = float(country_counts.head(3).sum() / total_count * 100.0)

    signals = [
        f"{top_country_name} has the biggest footprint in this view with {top_country_count:,} satellites ({top_country_share:.0f}%).",
        f"{top_group_name} is the biggest group in the current set ({top_group_share:.0f}%).",
        f"{top_orbit_name} is the main orbit in view ({top_orbit_share:.0f}%).",
        f"The top three country groupings make up about {top_three_share:.0f}% of the filtered footprint.",
    ]

    if "Navigation" in group_counts.index:
        nav_count = int(group_counts["Navigation"])
        nav_share = nav_count / total_count * 100.0
        signals.append(
            f"Navigation systems make up about {nav_share:.0f}% of the visible set."
        )

    return signals[:5]


inject_styles()

identity = st.secrets.get("SPACE_TRACK_IDENTITY")
password = st.secrets.get("SPACE_TRACK_PASSWORD")

if not identity or not password:
    st.error("Missing Space-Track credentials in Streamlit secrets.")
    st.code(
        'Create ".streamlit/secrets.toml" with:\n\nSPACE_TRACK_IDENTITY = "your_email"\nSPACE_TRACK_PASSWORD = "your_password"'
    )
    st.stop()

with st.sidebar:
    st.markdown("### Strategic Watch Filters")
    limit_count = st.slider("Live map sample", min_value=20, max_value=300, value=100, step=10)
    search_query = st.text_input("Search strategic assets", placeholder="Object, NORAD, country, group, or type").strip()
    regimes = st.multiselect(
        "Orbit regimes",
        options=["LEO", "MEO", "GEO", "HEO"],
        default=["LEO", "MEO", "GEO", "HEO"],
    )

    st.markdown("### Map")
    map_theme = st.selectbox("Map theme", options=list(MAP_THEMES.keys()), index=1)
    show_labels = st.toggle("Show labels", value=False)

    st.markdown("### Refresh")
    auto_refresh_hourly = st.toggle("Auto-refresh every hour", value=True)
    manual_refresh = st.button("Refresh now", use_container_width=True)

if manual_refresh:
    fetch_spacetrack_gp.clear()
    st.rerun()

inject_hourly_refresh(auto_refresh_hourly)

with st.spinner("Loading strategic orbital data..."):
    full_satellites_df, satellites_df, loaded_at_iso, data_source, data_error = load_dataset(
        identity,
        password,
        limit_count,
    )

filtered_full_df = apply_filters(full_satellites_df, search_query, regimes)
filtered_df = apply_filters(satellites_df, search_query, regimes)

hero_top_signal = build_top_signal(filtered_full_df)

st.markdown(
    f"""
    <div class="hero-card">
        <div class="hero-kicker">Strategic Orbital Watch</div>
        <h1 class="hero-title">Strategic Space Watch</h1>
        <p class="hero-copy">
            Track state-linked satellites and turn live public catalogue data into a simple picture of who has the biggest footprint,
            what kind of systems dominate, and which orbit matters most.
        </p>
        <div class="chip-row" style="margin-top:.85rem;">
            <span class="chip">Top signal</span>
        </div>
        <div style="margin-top:.55rem; font-size:1.02rem; line-height:1.6; color:var(--text-main); font-weight:600;">
            {html.escape(hero_top_signal)}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

country_count = filtered_full_df["country"].nunique() if not filtered_full_df.empty else 0
military_intel_df = (
    filtered_full_df[filtered_full_df["strategic_group"] == "Military / Intel"].copy()
    if not filtered_full_df.empty else pd.DataFrame()
)

status_label = {
    "live": "Live",
    "cached_live": "Cached Live",
    "unavailable": "Unavailable",
}.get(data_source, "Unknown")

status_detail = format_time(loaded_at_iso) if loaded_at_iso else "No live orbital data available"
status_color = {
    "live": "#39d98a",
    "cached_live": "#f59e0b",
    "unavailable": "#ff5f6d",
}.get(data_source, "#7dd3fc")

country_stats = top_country_stats(filtered_full_df)
group_stats = top_group_stats(filtered_full_df)
orbit_stats = top_orbit_stats(filtered_full_df)

metric_columns = st.columns(5)
with metric_columns[0]:
    render_metric_card(
        "Tracked assets",
        f"{len(filtered_full_df):,}",
        "Strategic satellites matching your filters",
        "#38bdf8",
    )
with metric_columns[1]:
    render_metric_card(
        "Map sample",
        f"{len(filtered_df):,}",
        "Satellites shown on the live map",
        "#7dd3fc",
    )
with metric_columns[2]:
    render_metric_card(
        "Countries",
        f"{country_count:,}",
        "Countries or groupings in view",
        "#58a6ff",
    )
with metric_columns[3]:
    render_metric_card(
        "Military / Intel",
        f"{len(military_intel_df):,}",
        "Military or intelligence systems in view",
        "#ff5f6d",
    )
with metric_columns[4]:
    render_metric_card("Feed status", status_label, status_detail, status_color)

st.markdown(
    f"""
    <div class="chip-row">
        <span class="chip">Source: Space-Track GP JSON</span>
        <span class="chip">Refresh: {"Hourly" if auto_refresh_hourly else "Manual only"}</span>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("")

takeaways = build_takeaways(filtered_full_df)

st.markdown(
    """
    <div class="panel-card">
        <div class="panel-title">Key signals</div>
        <div class="panel-copy">
            The main points from the current filtered satellite picture.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

signal_html = "".join(
    [
        f"""
        <div class="insight-card" style="min-height:auto;">
            <div class="insight-copy" style="color:var(--text-main); font-size:1rem; line-height:1.65; font-weight:600;">
                {html.escape(signal)}
            </div>
        </div>
        """
        for signal in takeaways
    ]
)

st.markdown(signal_html, unsafe_allow_html=True)

st.markdown("")

insight_cols = st.columns(3)
with insight_cols[0]:
    render_insight_card(
        "Biggest footprint",
        country_stats["label"],
        f"{country_stats['share']:.0f}% of tracked assets in the current view.",
    )
with insight_cols[1]:
    render_insight_card(
        "Main group",
        group_stats["label"],
        f"{group_stats['share']:.0f}% of tracked assets in the current view.",
    )
with insight_cols[2]:
    render_insight_card(
        "Main orbit",
        orbit_stats["label"],
        f"{orbit_stats['share']:.0f}% of tracked assets in the current view.",
    )

st.markdown("")

map_col, side_col = st.columns([3.1, 1.1], gap="large")

with map_col:
    st.markdown(
        """
        <div class="panel-card">
            <div class="panel-title">Orbital map</div>
            <div class="panel-copy">
                A live sample of the current filtered satellite picture.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if filtered_df.empty:
        if full_satellites_df.empty:
            st.error("No strategic satellite positions could be computed from the current Space-Track response.")
            if data_error:
                st.code(str(data_error))
        else:
            st.info("No strategic assets match the current search and orbit filters.")
    else:
        orbital_map, labels_used = create_map(filtered_df, map_theme, show_labels)
        if orbital_map is None:
            st.info("No orbital positions are available for the current filtered view.")
        else:
            st_folium(
                orbital_map,
                use_container_width=True,
                height=680,
                returned_objects=[],
                key="strategic_space_radar_map",
            )
            if show_labels and not labels_used:
                st.caption("Labels were turned down automatically because too many satellites are visible.")

with side_col:
    st.markdown(
        f"""
        <div class="panel-card">
            <div class="panel-title">Current view</div>
            <div class="panel-copy">
                Status: {html.escape(status_label)}<br>
                Loaded at: {html.escape(status_detail)}<br>
                Full matched set: {len(filtered_full_df):,}<br>
                Map sample: {len(filtered_df):,}<br>
                Countries: {country_count:,}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="panel-card">
            <div class="panel-title">How assets are picked</div>
            <div class="panel-copy">
                This page uses public naming patterns like NROL, SBIRS, AEHF, MUOS, Yaogan, Cosmos, GLONASS, GPS, BeiDou,
                Galileo, IRNSS, NavIC, and QZSS to find state-linked or strategic satellites.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="panel-card">
            <div class="panel-title">Why it matters</div>
            <div class="panel-copy">
                These systems support reconnaissance, secure communications, missile warning, and navigation.
                Read together, they show which states have the biggest visible orbital footprint.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if data_source == "cached_live" and data_error:
        st.warning("Live login or query failed. Showing the most recent cached real dataset instead.")
        st.code(str(data_error))
    elif data_source == "unavailable" and data_error:
        st.error("No live Space-Track data could be loaded, and no cached real dataset exists yet.")
        st.code(str(data_error))

st.markdown("---")
st.caption(
    f"The current matched set contains {len(filtered_full_df):,} propagated assets from {status_label.lower()}, "
    f"while {len(filtered_df):,} are shown in the live map sample for performance."
)
