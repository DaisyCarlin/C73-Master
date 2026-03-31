
import time
from datetime import datetime, timezone

import pandas as pd
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

st.set_page_config(
    page_title="Signal Console",
    page_icon="◉",
    layout="wide",
)

PAGE_PATHS = {
    "launch": "pages/1_Orbital_Launch_Monitor.py",
    "satellite": "pages/3_Satellite_Activity.py",
    "strategic": "pages/Strategic_Insights.py",
}

# ----------------------------
# CONFIG
# ----------------------------

LAUNCH_UPCOMING_LIMIT = 50
LAUNCH_REQUEST_TIMEOUT = 45
SPACE_TRACK_REQUEST_TIMEOUT = 15
REQUEST_RETRIES = 3
CACHE_TTL_SECONDS = 3600

LAUNCH_API_URL = f"https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit={LAUNCH_UPCOMING_LIMIT}&mode=detailed"
SPACE_TRACK_LOGIN_URL = "https://www.space-track.org/ajaxauth/login"
SPACE_TRACK_GP_URL = (
    "https://www.space-track.org/basicspacedata/query/"
    "class/gp/"
    "decay_date/null-val/"
    "epoch/%3Enow-10/"
    "orderby/norad_cat_id/"
    "format/json"
)

SENSITIVE_KEYWORDS = [
    "government",
    "national security",
    "military",
    "reconnaissance",
    "surveillance",
    "classified",
    "nrol",
    "nro",
    "ussf-",
    "gps",
    "wgs",
    "gssap",
    "missile warning",
    "missile tracking",
    "tracking layer",
    "satcom",
]

WATCHED_PROVIDERS = [
    "united launch alliance",
    "spacex",
    "rocket lab",
    "northrop grumman",
]

# ----------------------------
# STYLES
# ----------------------------

st.markdown(
    """
<style>
    :root {
        --bg-0: #07111f;
        --bg-1: #0d1b2a;
        --stroke: rgba(130, 161, 191, 0.22);
        --text-main: #e8f1fb;
        --text-soft: #91a9c3;
        --blue: #58a6ff;
        --cyan: #38bdf8;
        --amber: #ff9e3d;
        --red: #ff5f6d;
    }

    .stApp {
        background:
            radial-gradient(circle at top left, rgba(56,189,248,.16), transparent 28%),
            radial-gradient(circle at top right, rgba(88,166,255,.12), transparent 26%),
            linear-gradient(180deg, var(--bg-0) 0%, var(--bg-1) 100%);
        color: var(--text-main);
        font-family: "Aptos", "Segoe UI", sans-serif;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(9,19,32,.97), rgba(9,19,32,.92));
        border-right: 1px solid var(--stroke);
    }

    [data-testid="stSidebar"] * {
        color: var(--text-main);
    }

    .hero-card,
    .highlight-card,
    .purpose-card {
        border: 1px solid var(--stroke);
        border-radius: 22px;
        background: linear-gradient(180deg, rgba(10,23,37,.92), rgba(14,31,49,.84));
        box-shadow: 0 16px 34px rgba(4,9,18,.22);
    }

    .hero-card {
        padding: 1.45rem 1.6rem;
        margin-bottom: 1rem;
    }

    .hero-kicker {
        letter-spacing: .16rem;
        font-size: .72rem;
        font-weight: 700;
        color: #84d7ff;
        margin-bottom: .45rem;
        text-transform: uppercase;
    }

    .hero-title {
        font-size: 2.45rem;
        line-height: 1.02;
        font-weight: 760;
        margin: 0;
        color: var(--text-main);
    }

    .hero-copy,
    .section-copy,
    .purpose-copy {
        color: var(--text-soft);
        font-size: .96rem;
        line-height: 1.6;
    }

    .section-wrap {
        margin-top: 1.2rem;
        margin-bottom: .8rem;
    }

    .section-title {
        color: var(--text-main);
        font-size: 1.08rem;
        font-weight: 730;
        margin-bottom: .18rem;
    }

    .highlight-card {
        padding: 1rem;
        min-height: 285px;
    }

    .highlight-eyebrow {
        font-size: .76rem;
        font-weight: 760;
        text-transform: uppercase;
        letter-spacing: .12rem;
        margin-bottom: .55rem;
    }

    .highlight-title {
        font-size: 1.05rem;
        font-weight: 760;
        color: var(--text-main);
        line-height: 1.45;
        margin-bottom: .7rem;
    }

    .highlight-line {
        color: #c2d4e6;
        font-size: .92rem;
        line-height: 1.55;
        margin-bottom: .45rem;
    }

    .tone-blue .highlight-eyebrow { color: var(--cyan); }
    .tone-amber .highlight-eyebrow { color: var(--amber); }
    .tone-red .highlight-eyebrow { color: var(--red); }

    .accent-bar {
        width: 58px;
        height: 4px;
        border-radius: 999px;
        margin-bottom: .8rem;
    }

    .tone-blue .accent-bar { background: var(--cyan); }
    .tone-amber .accent-bar { background: var(--amber); }
    .tone-red .accent-bar { background: var(--red); }

    .purpose-card {
        padding: 1rem;
        margin-top: 1.2rem;
    }

    .purpose-label {
        font-size: .78rem;
        font-weight: 760;
        letter-spacing: .1rem;
        text-transform: uppercase;
        color: #84d7ff;
        margin-bottom: .4rem;
    }

    .chip-row {
        display: flex;
        flex-wrap: wrap;
        gap: .45rem;
        margin-top: .9rem;
    }

    .chip {
        display: inline-block;
        padding: .28rem .62rem;
        border-radius: 999px;
        font-size: .76rem;
        font-weight: 700;
        color: #dff4ff;
        background: rgba(56,189,248,.14);
        border: 1px solid rgba(56,189,248,.24);
    }

    div[data-testid="stPageLink"] a {
        width: 100%;
        display: inline-block;
        padding: .72rem .95rem;
        border-radius: 14px;
        border: 1px solid rgba(88,166,255,.24);
        background: rgba(88,166,255,.12);
        color: #e8f1fb !important;
        text-decoration: none !important;
        font-weight: 700;
        margin-top: .65rem;
    }

    div[data-testid="stPageLink"] a:hover {
        border-color: rgba(88,166,255,.42);
        background: rgba(88,166,255,.18);
    }
</style>
""",
    unsafe_allow_html=True,
)

# ----------------------------
# HELPERS
# ----------------------------

def safe_text(value):
    return "" if value is None else str(value).strip()


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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "SignalConsoleHome/1.0"})
    return session


def fetch_json_with_retry(url: str, timeout: int, retries: int = REQUEST_RETRIES):
    last_error = None
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            last_error = error
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    raise last_error


def country_label(code: str) -> str:
    code = safe_text(code).upper()
    mapping = {
        "US": "U.S.",
        "USA": "U.S.",
        "CN": "China",
        "CHN": "China",
        "RU": "Russia",
        "RUS": "Russia",
        "NZL": "New Zealand",
        "JPN": "Japan",
        "KAZ": "Kazakhstan",
        "GUF": "French Guiana",
        "PRC": "China",
        "CIS": "Russia-linked systems",
        "UK": "U.K.",
        "GB": "U.K.",
        "FR": "France",
        "ITSO": "ITSO",
    }
    return mapping.get(code, code if code else "Unknown")


def looks_sensitive_launch(name: str, subcategory: str, source: str) -> bool:
    text = " ".join([safe_text(name).lower(), safe_text(subcategory).lower(), safe_text(source).lower()])

    if any(keyword in text for keyword in SENSITIVE_KEYWORDS):
        return True

    provider = safe_text(source).lower()
    if any(provider_name in provider for provider_name in WATCHED_PROVIDERS):
        watched_pattern = (
            "government",
            "military",
            "national security",
            "reconnaissance",
            "surveillance",
            "classified",
            "nrol",
            "nro",
            "gps",
            "wgs",
            "gssap",
            "missile",
        )
        return any(token in text for token in watched_pattern)

    return False


def classify_satellite(name: str) -> str:
    text = safe_text(name).upper()

    if any(k in text for k in ["ISS", "TIANGONG", "CSS", "CREW", "SOYUZ", "PROGRESS"]):
        return "Stations"
    if any(k in text for k in ["GPS", "GALILEO", "GLONASS", "BEIDOU", "NAVSTAR", "QZSS", "IRNSS", "NAVIC"]):
        return "Navigation"
    if any(k in text for k in ["NOAA", "GOES", "METEOR", "HIMAWARI", "FENGYUN", "METOP", "DMSP"]):
        return "Weather"
    if any(k in text for k in ["LANDSAT", "SENTINEL", "TERRA", "AQUA", "WORLDVIEW", "PLEIADES", "SPOT", "KOMPSAT", "RESURS", "GAOFEN"]):
        return "Earth Observation"
    if any(k in text for k in ["STARLINK", "ONEWEB", "IRIDIUM", "INTELSAT", "SES", "EUTELSAT", "INMARSAT", "VIASAT", "TDRS", "O3B"]):
        return "Communications"
    if any(k in text for k in ["NROL", "USA ", "COSMOS", "YAOGAN", "KH-", "SBIRS", "AEHF", "MUOS", "MILSTAR"]):
        return "Military"
    if any(k in text for k in ["HUBBLE", "JWST", "XMM", "CHANDRAYAAN", "MARS", "LUNAR", "GAIA", "KEPLER"]):
        return "Science"
    return "Other"


def satellite_is_sensitive(name: str, category: str, country: str) -> bool:
    text = " ".join([safe_text(name).upper(), safe_text(category).upper(), safe_text(country).upper()])

    if category == "Military":
        return True

    if any(k in text for k in ["NROL", "USA ", "YAOGAN", "SBIRS", "AEHF", "MUOS", "MILSTAR", "KH-"]):
        return True

    return False


# ----------------------------
# LIVE DATA LOAD
# ----------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_upcoming_launches_df() -> pd.DataFrame:
    raw = fetch_json_with_retry(LAUNCH_API_URL, timeout=LAUNCH_REQUEST_TIMEOUT)
    results = raw.get("results", [])
    rows = []

    for item in results:
        mission = item.get("mission") or {}
        provider = item.get("launch_service_provider") or {}
        pad = item.get("pad") or {}
        location = pad.get("location") or {}

        name = safe_text(item.get("name")) or "Unknown launch"
        subcategory = safe_text(mission.get("type")) or "orbital_launch"
        source = safe_text(provider.get("name")) or "Unknown"
        country = safe_text(location.get("country_code")) or "Unknown"
        net = item.get("net")

        rows.append(
            {
                "name": name,
                "subcategory": subcategory,
                "source": source,
                "country": country,
                "timestamp": pd.to_datetime(net, utc=True, errors="coerce"),
                "sensitive": looks_sensitive_launch(name, subcategory, source),
            }
        )

    return pd.DataFrame(rows)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_live_satellite_df(identity: str, password: str) -> pd.DataFrame:
    session = build_session()

    login_response = session.post(
        SPACE_TRACK_LOGIN_URL,
        data={"identity": identity, "password": password},
        timeout=SPACE_TRACK_REQUEST_TIMEOUT,
    )
    login_response.raise_for_status()

    response = session.get(SPACE_TRACK_GP_URL, timeout=SPACE_TRACK_REQUEST_TIMEOUT)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("Space-Track returned no GP records.")

    rows = []
    for record in payload:
        name = safe_text(record.get("OBJECT_NAME")) or f"NORAD {safe_text(record.get('NORAD_CAT_ID'))}"
        category = classify_satellite(name)
        country = safe_text(record.get("COUNTRY_CODE")) or "Unknown"

        rows.append(
            {
                "name": name,
                "country": country,
                "subcategory": category,
                "sensitive": satellite_is_sensitive(name, category, country),
            }
        )

    return pd.DataFrame(rows)


# ----------------------------
# HIGHLIGHT BUILDERS
# ----------------------------

def build_launch_highlight(launch_df: pd.DataFrame) -> dict:
    if launch_df.empty:
        return {
            "eyebrow": "Launch Signal",
            "title": "No live launch signal available right now.",
            "lines": [
                "The upcoming launch feed did not return usable data.",
                "Open the launch page for a deeper check.",
            ],
            "tone": "amber",
            "key": "launch",
            "button": "Open Launch Intelligence",
        }

    sensitive_df = launch_df[launch_df["sensitive"] == True].copy()

    if sensitive_df.empty:
        total_upcoming = int(launch_df.shape[0])
        return {
            "eyebrow": "Launch Signal",
            "title": f"{total_upcoming} upcoming launches are visible, but none are strongly flagged sensitive under the current logic.",
            "lines": [
                "That suggests the near-term launch picture looks more routine than overtly state-linked.",
                "Open the launch page to inspect the full queue and mission labels.",
            ],
            "tone": "blue",
            "key": "launch",
            "button": "Open Launch Intelligence",
        }

    country_counts = sensitive_df.groupby("country").size().sort_values(ascending=False)
    top_country_code = str(country_counts.index[0])
    top_country = country_label(top_country_code)
    top_count = int(country_counts.iloc[0])

    top_types = (
        sensitive_df[sensitive_df["country"] == top_country_code]
        .groupby("subcategory")
        .size()
        .sort_values(ascending=False)
    )
    top_type = safe_text(top_types.index[0]).lower() if not top_types.empty else "state-linked"

    return {
        "eyebrow": "Launch Signal",
        "title": f"{top_country} has {top_count} upcoming sensitive-linked launches in the current watch window.",
        "lines": [
            f"Public naming and mission tags link these launches most clearly to {top_type} activity.",
            "That suggests the upcoming launch picture is not purely routine commercial traffic.",
        ],
        "tone": "amber",
        "key": "launch",
        "button": "Open Launch Intelligence",
    }


def build_satellite_highlight(sat_df: pd.DataFrame) -> dict:
    if sat_df.empty:
        return {
            "eyebrow": "Satellite Signal",
            "title": "No live satellite signal available right now.",
            "lines": [
                "The orbital catalogue did not return usable data.",
                "Open the satellite page for a deeper check.",
            ],
            "tone": "red",
            "key": "satellite",
            "button": "Open Satellite Watch",
        }

    sensitive_df = sat_df[sat_df["sensitive"] == True].copy()

    if sensitive_df.empty:
        total_sat = int(sat_df.shape[0])
        return {
            "eyebrow": "Satellite Signal",
            "title": f"{total_sat:,} tracked satellites are visible, but none are currently marked sensitive by the live logic.",
            "lines": [
                "That likely means the current page logic is reading the footprint as broad rather than sharply strategic.",
                "Open the satellite page to inspect the full category mix.",
            ],
            "tone": "blue",
            "key": "satellite",
            "button": "Open Satellite Watch",
        }

    country_counts = sensitive_df.groupby("country").size().sort_values(ascending=False)
    top_country_code = str(country_counts.index[0])
    top_country = country_label(top_country_code)
    top_count = int(country_counts.iloc[0])

    top_categories = (
        sensitive_df[sensitive_df["country"] == top_country_code]
        .groupby("subcategory")
        .size()
        .sort_values(ascending=False)
    )
    top_category = safe_text(top_categories.index[0]).lower() if not top_categories.empty else "strategic"

    return {
        "eyebrow": "Satellite Signal",
        "title": f"{top_country} holds {top_count:,} sensitive-linked satellites in the current live footprint.",
        "lines": [
            f"The visible sensitive layer is led mainly by {top_category} systems.",
            "That points to a concentrated strategic orbital layer rather than a widely distributed one.",
        ],
        "tone": "red",
        "key": "satellite",
        "button": "Open Satellite Watch",
    }


def build_strategic_highlight(launch_df: pd.DataFrame, sat_df: pd.DataFrame) -> dict:
    if launch_df.empty and sat_df.empty:
        return {
            "eyebrow": "Strategic Signal",
            "title": "No combined strategic signal is available right now.",
            "lines": [
                "Both live feeds need to load for the strongest homepage readout.",
                "Open the individual pages to inspect them directly.",
            ],
            "tone": "blue",
            "key": "strategic",
            "button": "Open Strategic Insights",
        }

    launch_sensitive = launch_df[launch_df["sensitive"] == True].copy() if not launch_df.empty else pd.DataFrame()
    sat_sensitive = sat_df[sat_df["sensitive"] == True].copy() if not sat_df.empty else pd.DataFrame()

    launch_country = None
    sat_country = None

    if not launch_sensitive.empty:
        launch_country = country_label(str(launch_sensitive.groupby("country").size().sort_values(ascending=False).index[0]))

    if not sat_sensitive.empty:
        sat_country = country_label(str(sat_sensitive.groupby("country").size().sort_values(ascending=False).index[0]))

    if launch_country and sat_country and launch_country != sat_country:
        return {
            "eyebrow": "Strategic Signal",
            "title": f"{launch_country} leads the sensitive launch picture, while {sat_country} leads the sensitive orbital footprint.",
            "lines": [
                "That split suggests short-term launch movement and long-term orbital presence are being led by different actors.",
                "",
            ],
            "tone": "blue",
            "key": "strategic",
            "button": "Open Strategic Insights",
        }

    if launch_country and sat_country and launch_country == sat_country:
        return {
            "eyebrow": "Strategic Signal",
            "title": f"{launch_country} appears at the top of both the sensitive launch picture and the sensitive orbital layer.",
            "lines": [
                "That suggests strength in both near-term mission tempo and wider orbital presence.",
                "Open Strategic Insights to see the fuller cross-page read.",
            ],
            "tone": "amber",
            "key": "strategic",
            "button": "Open Strategic Insights",
        }

    if sat_country:
        return {
            "eyebrow": "Strategic Signal",
            "title": f"{sat_country} dominates the clearest sensitive orbital signal visible on the platform right now.",
            "lines": [
                "The satellite footprint is currently giving the stronger strategic read than the launch queue.",
                "That may point to persistent infrastructure mattering more than immediate launch movement.",
            ],
            "tone": "blue",
            "key": "strategic",
            "button": "Open Strategic Insights",
        }

    return {
        "eyebrow": "Strategic Signal",
        "title": "The launch queue is currently giving the strongest homepage signal.",
        "lines": [
            "Sensitive-linked launch activity is standing out more clearly than the live orbital layer right now.",
            "Open the launch and strategic pages for the full breakdown.",
        ],
        "tone": "amber",
        "key": "strategic",
        "button": "Open Strategic Insights",
    }


# ----------------------------
# PAGE
# ----------------------------

current_time = datetime.now(timezone.utc).strftime("%d %b %Y • %H:%M UTC")

st.markdown(
    f"""
<div class="hero-card">
    <div class="hero-kicker">Command Home</div>
    <div class="hero-title">Signal Console</div>
    <div class="hero-copy" style="margin-top:.75rem;">
        Open-source orbital intelligence workspace focused on launches, satellite activity,
        and the sensitive signals that matter most.
    </div>
    <div class="chip-row">
        <span class="chip">System Time: {current_time}</span>
        <span class="chip">Live homepage highlights</span>
        <span class="chip">Sensitive-focused readout</span>
    </div>
</div>
""",
    unsafe_allow_html=True,
)

identity = st.secrets.get("SPACE_TRACK_IDENTITY")
password = st.secrets.get("SPACE_TRACK_PASSWORD")

if not identity or not password:
    st.error("Missing Space-Track credentials in Streamlit secrets.")
    st.code(
        'Create ".streamlit/secrets.toml" with:\n\nSPACE_TRACK_IDENTITY = "your_email"\nSPACE_TRACK_PASSWORD = "your_password"'
    )
    st.stop()

try:
    with st.spinner("Loading live homepage signals..."):
        launch_df = get_upcoming_launches_df()
        sat_df = get_live_satellite_df(identity, password)
except Exception as error:
    st.error(f"Could not load live homepage data: {error}")
    st.stop()

highlight_cards = [
    build_launch_highlight(launch_df),
    build_satellite_highlight(sat_df),
    build_strategic_highlight(launch_df, sat_df),
]

# remove any accidental None values
highlight_cards = [card for card in highlight_cards if card is not None]

st.markdown(
    """
<div class="section-wrap">
    <div class="section-title">Today’s Key Signals</div>
    <div class="section-copy">
        Real sensitive signals pulled from the live launch queue, the live satellite footprint, and the combined strategic read.
    </div>
</div>
""",
    unsafe_allow_html=True,
)

if not highlight_cards:
    st.info("No strong signals are available right now.")
else:
    highlight_cols = st.columns(len(highlight_cards), gap="large")

    for col, card in zip(highlight_cols, highlight_cards):
        with col:
            lines_html = "".join(
                f'<div class="highlight-line">{line}</div>'
                for line in card["lines"]
                if safe_text(line)
            )

            st.markdown(
                f"""
            <div class="highlight-card tone-{card['tone']}">
                <div class="accent-bar"></div>
                <div class="highlight-eyebrow">{card['eyebrow']}</div>
                <div class="highlight-title">{card['title']}</div>
                {lines_html}
            </div>
            """,
                unsafe_allow_html=True,
            )

            st.page_link(
                PAGE_PATHS[card["key"]],
                label=card["button"],
            )

st.markdown(
    """
<div class="purpose-card">
    <div class="purpose-label">Platform Purpose</div>
    <div class="purpose-copy">
        Signal Console is designed to show the most striking live orbital signal first, then route the user into the right page for deeper analysis.
    </div>
</div>
""",
    unsafe_allow_html=True,
)
