import html
import re
import time

import folium
import pandas as pd
import requests
import streamlit as st
from folium.features import DivIcon
from folium.plugins import Fullscreen, MousePosition
from streamlit_folium import st_folium

from utils.event_logger import log_event

st.set_page_config(page_title="Orbital Launch Monitor", layout="wide")

UPCOMING_LIMIT = 15
RECENT_LIMIT = 60
REQUEST_TIMEOUT = 45
REQUEST_RETRIES = 3
CACHE_TTL_SECONDS = 300

MAP_THEMES = {
    "Light": {
        "tiles": "CartoDB positron",
        "attr": None,
    },
    "Radar": {
        "tiles": "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        "attr": "&copy; OpenStreetMap contributors &copy; CARTO",
    },
    "Dark": {
        "tiles": "CartoDB dark_matter",
        "attr": None,
    },
}

STATUS_COLORS = {
    "Upcoming": "#38bdf8",
    "Recent failure": "#ffb347",
    "Sensitive": "#ff5f6d",
    "Healthy": "#39d98a",
}

OFFICIAL_SOURCES = {
    # ---------------------------
    # UNITED STATES
    # ---------------------------
    "us_nro_launch": {
        "title": "NRO launch overview",
        "url": "https://www.nro.gov/Launch/",
        "summary": "NRO says its satellites support intelligence, global coverage, research, and disaster relief, and that launch systems are selected for the mission orbit and payload profile.",
        "country_group": "United States",
    },
    "us_nrol_101": {
        "title": "NRO NROL-101 official launch page",
        "url": "https://www.nro.gov/Launches/launch-nrol-101/",
        "summary": "NRO says NROL-101 carried a national security payload supporting overhead reconnaissance and intelligence support to policymakers, the Intelligence Community, and DoD.",
        "country_group": "United States",
    },
    "us_nrol_82": {
        "title": "NRO NROL-82 official launch page",
        "url": "https://www.nro.gov/Launches/launch-nrol-82/",
        "summary": "NRO says NROL-82 carried a national security payload and flew on Delta IV Heavy.",
        "country_group": "United States",
    },
    "us_nrol_151": {
        "title": "NRO NROL-151 official launch page",
        "url": "https://www.nro.gov/Launches/launch-nrol-151/",
        "summary": "NRO says Electron provides dedicated access to orbit for small satellites and describes Rocket Lab's U.S. launch capability for government missions.",
        "country_group": "United States",
    },
    "us_nrol_87": {
        "title": "NRO NROL-87 official launch page",
        "url": "https://www.nro.gov/Launches/launch-nrol-87/",
        "summary": "NRO says NROL-87 carried a national security payload aboard Falcon 9 under a National Security Space Launch mission.",
        "country_group": "United States",
    },
    "us_gps": {
        "title": "U.S. Space Force GPS fact sheet",
        "url": "https://www.spaceforce.mil/About-Us/Fact-Sheets/Article/2197765/global-positioning-system/",
        "summary": "The Space Force says GPS provides global positioning, navigation, and timing for military and civil users.",
        "country_group": "United States",
    },
    "us_milcom_pnt": {
        "title": "SSC Military Communications and PNT office",
        "url": "https://www.ssc.spaceforce.mil/Program-Offices/Military-Communications-and-Positioning",
        "summary": "Space Systems Command says MILCOM and PNT delivers military SATCOM and more secure, jam-resistant PNT capability.",
        "country_group": "United States",
    },
    "us_vulcan_nssl": {
        "title": "SSC Vulcan NSSL certification release",
        "url": "https://www.ssc.spaceforce.mil/Newsroom/Article/4136016/u-s-space-force-ussf-certifies-united-launch-alliance-ula-vulcan-for-national-s",
        "summary": "SSC says NSSL certification supports the launch of critical national security space systems with resiliency and flexibility.",
        "country_group": "United States",
    },
    "us_missile_tracking": {
        "title": "SSC missile warning and tracking launch award release",
        "url": "https://www.ssc.spaceforce.mil/Newsroom/Article/4374896/space-systems-command-awards-task-orders-to-launch-missile-warning-and-missile",
        "summary": "SSC says these launch awards support missile warning and missile tracking payloads.",
        "country_group": "United States",
    },
    "us_ussf_87": {
        "title": "SSC USSF-87 mission preparation release",
        "url": "https://www.ssc.spaceforce.mil/Newsroom/Article/4403552/space-systems-command-mission-partners-prepares-ussf-87-for-national-space-secu",
        "summary": "SSC says the USSF-87 primary payload, GSSAP, supports U.S. Space Command space surveillance operations.",
        "country_group": "United States",
    },
    "us_space_capabilities": {
        "title": "U.S. Space Force space capabilities overview",
        "url": "https://www.spaceforce.mil/About-Us/About-Space-Force/Space-Capabilities/",
        "summary": "The Space Force says military space capabilities include communications, navigation, threat warning, surveillance, and launch support.",
        "country_group": "United States",
    },

    # ---------------------------
    # CHINA
    # ---------------------------
    "cn_programme": {
        "title": "CNSA: China's Space Program: A 2021 Perspective",
        "url": "https://www.cnsa.gov.cn/english/n6465645/n6465648/c6813088/content.html",
        "summary": "CNSA says China's space program serves scientific development, national rights and interests, and national security alongside peaceful use of outer space.",
        "country_group": "China",
    },
    "cn_english_home": {
        "title": "CNSA English portal",
        "url": "https://www.cnsa.gov.cn/english/",
        "summary": "Official English CNSA portal with mission and launch coverage.",
        "country_group": "China",
    },

    # ---------------------------
    # INDIA
    # ---------------------------
    "in_launch_missions": {
        "title": "ISRO Launch Missions",
        "url": "https://www.isro.gov.in/LaunchMissions.html",
        "summary": "Official ISRO launch mission page covering missions, launchers, satellites, and programme activity.",
        "country_group": "India",
    },
    "in_spacecraft_missions": {
        "title": "ISRO Spacecraft Missions",
        "url": "https://www.isro.gov.in/SpacecraftMissions.html",
        "summary": "Official ISRO page listing spacecraft and satellite missions.",
        "country_group": "India",
    },
    "in_launchers": {
        "title": "ISRO Launchers",
        "url": "https://www.isro.gov.in/Launchers.html",
        "summary": "ISRO says launch vehicles carry spacecraft to space and describes PSLV, GSLV, and LVM3.",
        "country_group": "India",
    },
    "in_pslv_c62": {
        "title": "ISRO PSLV-C62 / EOS-N1 Mission",
        "url": "https://www.isro.gov.in/Mission_PSLV_C62.html",
        "summary": "Official ISRO mission page for a recent PSLV Earth observation mission.",
        "country_group": "India",
    },

    # ---------------------------
    # JAPAN
    # ---------------------------
    "jp_missions": {
        "title": "JAXA Our Missions",
        "url": "https://global.jaxa.jp/",
        "summary": "Official JAXA English missions portal.",
        "country_group": "Japan",
    },
    "jp_h3": {
        "title": "JAXA H3 Launch Vehicle",
        "url": "https://global.jaxa.jp/projects/rockets/h3/",
        "summary": "JAXA describes H3 as Japan's new mainstay launch vehicle.",
        "country_group": "Japan",
    },
    "jp_tanegashima": {
        "title": "JAXA Tanegashima Space Center",
        "url": "https://global.jaxa.jp/about/centers/tnsc/index.html",
        "summary": "JAXA describes Tanegashima as Japan's major rocket launch complex.",
        "country_group": "Japan",
    },
    "jp_satellite_topics": {
        "title": "JAXA satellite mission topics",
        "url": "https://global.jaxa.jp/projects/sat/topics.html",
        "summary": "Official JAXA English satellite mission material.",
        "country_group": "Japan",
    },

    # ---------------------------
    # EUROPE / ESA
    # ---------------------------
    "eu_secure_comms": {
        "title": "ESA Pacis 3 – Secure Communications",
        "url": "https://www.esa.int/Applications/Connectivity_and_Secure_Communications/Pacis_3_Secure_Communications",
        "summary": "ESA says Pacis 3 works toward secure communications services for governments in Europe.",
        "country_group": "Europe",
    },
    "eu_iris2_support": {
        "title": "ESA support for EU secure communication satellites system",
        "url": "https://www.esa.int/About_Us/Corporate_news/ESA_to_support_the_development_of_EU_s_secure_communication_satellites_system",
        "summary": "ESA says the planned secure communications constellation will deliver resilient and secure communications for EU governments and others.",
        "country_group": "Europe",
    },
    "eu_govsatcom": {
        "title": "ESA pooling and sharing for secure government satcoms",
        "url": "https://www.esa.int/Applications/Connectivity_and_Secure_Communications/Pooling_and_sharing_for_secure_government_satcoms",
        "summary": "ESA describes secure government satcom arrangements for public-sector users.",
        "country_group": "Europe",
    },
    "eu_spainsat_ng": {
        "title": "ESA SpainSat NG secure communications article",
        "url": "https://www.esa.int/Applications/Connectivity_and_Secure_Communications/SpainSat_NG_programme_completed_as_second_secure_communications_satellite_launches",
        "summary": "ESA says SpainSat NG will serve the Spanish Armed Forces and allied government users.",
        "country_group": "Europe",
    },

    # ---------------------------
    # UNITED KINGDOM / ALLIED
    # ---------------------------
    "uk_space_command": {
        "title": "UK Space Command",
        "url": "https://www.gov.uk/government/organisations/uk-space-command",
        "summary": "Official UK government space command organisation page.",
        "country_group": "United Kingdom",
    },

    # ---------------------------
    # RUSSIA
    # ---------------------------
    "ru_roscosmos": {
        "title": "Roscosmos official portal",
        "url": "https://www.roscosmos.ru/",
        "summary": "Official Roscosmos portal covering Russian launch and space activity.",
        "country_group": "Russia",
    },
}

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
    "secure communications",
    "govsatcom",
    "defence",
    "defense",
    "space domain awareness",
    "space surveillance",
    "warning",
    "yaogan",
    "gaofen",
    "beidou",
    "qzss",
    "quasi-zenith",
    "irnss",
    "navic",
    "kosmos",
    "soyuz",
]

WATCHED_PROVIDERS = [
    "united launch alliance",
    "spacex",
    "rocket lab",
    "northrop grumman",
    "arianespace",
    "mitsubishi heavy industries",
    "isro",
    "china aerospace science and technology",
    "casc",
    "expace",
    "roscosmos",
]

COUNTRY_CODE_TO_GROUP = {
    "US": "United States",
    "CN": "China",
    "IN": "India",
    "JP": "Japan",
    "FR": "Europe",
    "DE": "Europe",
    "IT": "Europe",
    "ES": "Europe",
    "GB": "United Kingdom",
    "UK": "United Kingdom",
    "RU": "Russia",
    "KZ": "Russia",
}

COUNTRY_NAME_HINTS = {
    "United States": [
        "united states",
        "usa",
        "cape canaveral",
        "vandenberg",
        "kennedy",
        "wallops",
        "nrol",
        "ussf",
        "nro",
        "gssap",
        "gps",
        "wgs",
    ],
    "China": [
        "china",
        "chinese",
        "taiyuan",
        "jiuquan",
        "wenchang",
        "xichang",
        "long march",
        "yaogan",
        "gaofen",
        "beidou",
        "shijian",
        "tjs",
    ],
    "India": [
        "india",
        "indian",
        "isro",
        "sriharikota",
        "satish dhawan",
        "pslv",
        "gslv",
        "lvm3",
        "navic",
        "irnss",
        "eos",
        "risat",
        "cartosat",
        "gsat",
    ],
    "Japan": [
        "japan",
        "japanese",
        "jaxa",
        "tanegashima",
        "uchinoura",
        "h3",
        "h-iia",
        "h-iib",
        "qzss",
        "quasi-zenith",
        "ibuki",
        "himawari",
    ],
    "Europe": [
        "europe",
        "esa",
        "ariane",
        "vega",
        "arianespace",
        "kourou",
        "guiana space centre",
        "govsatcom",
        "iris2",
        "spainsat",
    ],
    "United Kingdom": [
        "united kingdom",
        "britain",
        "british",
        "space command",
        "skynet",
    ],
    "Russia": [
        "russia",
        "russian",
        "soyuz",
        "kosmos",
        "roscosmos",
        "baikonur",
        "plesetsk",
        "angara",
        "progress",
    ],
}

GLOBAL_ROLE_PATTERNS = {
    "Reconnaissance or remote sensing mission": [
        "reconnaissance",
        "surveillance",
        "yaogan",
        "gaofen",
        "earth observation",
        "remote sensing",
        "cartosat",
        "risat",
        "kosmos",
    ],
    "Positioning, navigation, and timing mission": [
        "gps",
        "beidou",
        "navic",
        "irnss",
        "qzss",
        "quasi-zenith",
        "positioning",
        "navigation",
        "timing",
        "pnt",
    ],
    "Protected communications mission": [
        "satcom",
        "communications",
        "communication",
        "secure communications",
        "govsatcom",
        "skynet",
        "gsat",
        "wgs",
    ],
    "Missile warning or tracking mission": [
        "missile warning",
        "missile tracking",
        "tracking layer",
        "warning",
    ],
    "Space surveillance or space domain awareness mission": [
        "space surveillance",
        "space domain awareness",
        "gssap",
        "situational awareness",
    ],
    "Government or national security mission": [
        "government",
        "national security",
        "military",
        "defence",
        "defense",
        "classified",
    ],
}


def inject_styles():
    st.markdown(
        """
        <style>
            :root {
                --bg-0: #07111f;
                --bg-1: #0d1b2a;
                --stroke: rgba(130, 161, 191, 0.22);
                --text-main: #e8f1fb;
                --text-soft: #91a9c3;
                --cyan: #38bdf8;
                --amber: #ffb347;
                --red: #ff5f6d;
                --green: #39d98a;
                --blue-glow: rgba(56, 189, 248, 0.16);
            }

            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(56, 189, 248, 0.16), transparent 28%),
                    radial-gradient(circle at top right, rgba(88, 166, 255, 0.12), transparent 26%),
                    linear-gradient(180deg, var(--bg-0) 0%, var(--bg-1) 100%);
                color: var(--text-main);
                font-family: "Aptos", "Segoe UI", sans-serif;
            }

            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, rgba(9, 19, 32, 0.97), rgba(9, 19, 32, 0.92));
                border-right: 1px solid var(--stroke);
            }

            [data-testid="stSidebar"] * {
                color: var(--text-main);
            }

            .hero-card,
            .panel-card,
            .signal-banner,
            .signal-card,
            .event-card {
                border: 1px solid var(--stroke);
                background: linear-gradient(145deg, rgba(10, 21, 35, 0.92), rgba(15, 31, 49, 0.86));
                border-radius: 22px;
                box-shadow: 0 18px 40px rgba(4, 9, 18, 0.26);
            }

            .hero-card {
                padding: 1.35rem 1.5rem;
                margin-bottom: 1rem;
            }

            .hero-kicker {
                letter-spacing: 0.16rem;
                font-size: 0.72rem;
                font-weight: 700;
                color: #84d7ff;
                margin-bottom: 0.4rem;
            }

            .hero-title {
                font-size: 2.2rem;
                line-height: 1.05;
                font-weight: 700;
                margin: 0;
                color: var(--text-main);
            }

            .hero-copy {
                margin: 0.55rem 0 0 0;
                max-width: 60rem;
                color: var(--text-soft);
                font-size: 0.98rem;
            }

            .signal-banner {
                padding: 1rem 1.2rem;
                margin-bottom: 1rem;
                background:
                    linear-gradient(145deg, rgba(9, 24, 40, 0.96), rgba(14, 34, 54, 0.9)),
                    radial-gradient(circle at right top, rgba(56, 189, 248, 0.16), transparent 40%);
            }

            .signal-banner-label {
                font-size: 0.76rem;
                font-weight: 800;
                letter-spacing: 0.13rem;
                text-transform: uppercase;
                color: #8fdcff;
                margin-bottom: 0.35rem;
            }

            .signal-banner-text {
                font-size: 1.02rem;
                font-weight: 700;
                color: var(--text-main);
                line-height: 1.45;
            }

            .signal-card {
                min-height: 120px;
                padding: 1rem 1rem 0.95rem 1rem;
                margin-bottom: 0.35rem;
            }

            .signal-card-label {
                font-size: 0.76rem;
                text-transform: uppercase;
                letter-spacing: 0.08rem;
                color: #84d7ff;
                margin-bottom: 0.45rem;
                font-weight: 700;
            }

            .signal-card-text {
                font-size: 0.96rem;
                color: var(--text-main);
                line-height: 1.45;
                font-weight: 600;
            }

            .metric-card {
                border: 1px solid var(--stroke);
                background: linear-gradient(180deg, rgba(12, 24, 39, 0.9), rgba(14, 32, 50, 0.76));
                border-radius: 20px;
                padding: 1rem 1rem 0.95rem 1rem;
                min-height: 122px;
                box-shadow: 0 12px 28px rgba(4, 9, 18, 0.24);
            }

            .metric-label {
                font-size: 0.8rem;
                text-transform: uppercase;
                letter-spacing: 0.08rem;
                color: var(--text-soft);
                margin-bottom: 0.45rem;
            }

            .metric-value {
                font-size: 2rem;
                font-weight: 700;
                line-height: 1;
                margin-bottom: 0.35rem;
                color: var(--text-main);
            }

            .metric-detail {
                font-size: 0.92rem;
                color: var(--text-soft);
            }

            .accent-bar {
                width: 54px;
                height: 4px;
                border-radius: 999px;
                margin-bottom: 0.8rem;
            }

            .panel-card {
                padding: 1rem 1rem 0.8rem 1rem;
                box-shadow: 0 12px 28px rgba(4, 9, 18, 0.22);
            }

            .panel-title {
                font-size: 1rem;
                font-weight: 700;
                margin-bottom: 0.2rem;
                color: var(--text-main);
            }

            .panel-copy {
                color: var(--text-soft);
                font-size: 0.92rem;
                margin-bottom: 0.8rem;
                line-height: 1.5;
            }

            .event-card {
                padding: 1rem 1rem 0.95rem 1rem;
                margin-bottom: 0.9rem;
            }

            .event-kicker {
                color: #8fdcff;
                font-size: 0.72rem;
                text-transform: uppercase;
                letter-spacing: 0.1rem;
                font-weight: 800;
                margin-bottom: 0.45rem;
            }

            .event-title {
                font-size: 1.08rem;
                font-weight: 800;
                color: var(--text-main);
                line-height: 1.35;
                margin-bottom: 0.5rem;
            }

            .event-copy {
                color: var(--text-soft);
                font-size: 0.92rem;
                line-height: 1.55;
            }

            .badge {
                display: inline-block;
                padding: 0.28rem 0.58rem;
                border-radius: 999px;
                font-size: 0.72rem;
                font-weight: 800;
                margin-right: 0.35rem;
                margin-bottom: 0.35rem;
                border: 1px solid transparent;
            }

            .badge-red {
                background: rgba(255, 95, 109, 0.16);
                color: #ffd7dc;
                border-color: rgba(255, 95, 109, 0.32);
            }

            .badge-blue {
                background: rgba(56, 189, 248, 0.15);
                color: #dcf5ff;
                border-color: rgba(56, 189, 248, 0.3);
            }

            .badge-amber {
                background: rgba(255, 179, 71, 0.15);
                color: #ffe9c2;
                border-color: rgba(255, 179, 71, 0.3);
            }

            .badge-green {
                background: rgba(57, 217, 138, 0.15);
                color: #d9ffe9;
                border-color: rgba(57, 217, 138, 0.3);
            }

            .stTabs [data-baseweb="tab-list"] {
                gap: 0.6rem;
            }

            .stTabs [data-baseweb="tab"] {
                border-radius: 999px;
                background: rgba(15, 31, 49, 0.7);
                border: 1px solid var(--stroke);
                color: var(--text-main);
                padding-left: 1rem;
                padding-right: 1rem;
            }

            .stDataFrame, div[data-testid="stTable"] {
                border-radius: 18px;
                overflow: hidden;
                border: 1px solid var(--stroke);
            }

            .source-chip {
                display: inline-block;
                padding: 0.25rem 0.55rem;
                border-radius: 999px;
                font-size: 0.76rem;
                font-weight: 700;
                color: #dff4ff;
                background: rgba(56, 189, 248, 0.16);
                border: 1px solid rgba(56, 189, 248, 0.28);
                margin-right: 0.35rem;
                margin-bottom: 0.35rem;
                text-decoration: none;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def safe_text(value):
    return "" if value is None else str(value).strip()


def clean_time_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if not df.empty and col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def format_time(value):
    if pd.isna(value):
        return "Unknown"
    return pd.to_datetime(value, utc=True).strftime("%Y-%m-%d %H:%M UTC")


def fetch_json_with_retry(url: str, timeout: int = REQUEST_TIMEOUT, retries: int = REQUEST_RETRIES):
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


def build_launch_rows(raw_results) -> pd.DataFrame:
    rows = []
    for item in raw_results:
        pad = item.get("pad") or {}
        location = pad.get("location") or {}
        mission = item.get("mission") or {}
        rocket = item.get("rocket") or {}
        configuration = rocket.get("configuration") or {}
        provider = item.get("launch_service_provider") or {}
        status = item.get("status") or {}

        rows.append(
            {
                "name": item.get("name"),
                "net": item.get("net"),
                "status": status.get("name"),
                "provider": provider.get("name"),
                "rocket": configuration.get("name"),
                "mission_type": mission.get("type"),
                "mission_description": mission.get("description"),
                "location_name": location.get("name"),
                "pad_name": pad.get("name"),
                "country_code": location.get("country_code"),
                "lat": pd.to_numeric(pad.get("latitude"), errors="coerce"),
                "lon": pd.to_numeric(pad.get("longitude"), errors="coerce"),
            }
        )
    return pd.DataFrame(rows)


def add_source(source_keys, key):
    if key in OFFICIAL_SOURCES and key not in source_keys:
        source_keys.append(key)


def source_objects(source_keys):
    return [OFFICIAL_SOURCES[key] for key in source_keys if key in OFFICIAL_SOURCES]


def source_links_html(source_keys):
    links = []
    for source in source_objects(source_keys):
        links.append(
            f'<a class="source-chip" href="{source["url"]}" target="_blank">{html.escape(source["title"])}</a>'
        )
    return "".join(links)


def text_contains_word(text: str, phrase: str) -> bool:
    phrase = safe_text(phrase).lower()
    text = safe_text(text).lower()
    if not phrase or not text:
        return False
    return re.search(rf"\b{re.escape(phrase)}\b", text) is not None


def infer_country_group(row: pd.Series) -> str:
    country_code = safe_text(row.get("country_code")).upper()
    if country_code in COUNTRY_CODE_TO_GROUP:
        return COUNTRY_CODE_TO_GROUP[country_code]

    name = safe_text(row.get("name"))
    provider = safe_text(row.get("provider"))
    rocket = safe_text(row.get("rocket"))
    mission_type = safe_text(row.get("mission_type"))
    mission_description = safe_text(row.get("mission_description"))
    location_name = safe_text(row.get("location_name"))
    pad_name = safe_text(row.get("pad_name"))

    text = " ".join(
        [name, provider, rocket, mission_type, mission_description, location_name, pad_name]
    ).lower()

    if "long march" in text or "yaogan" in text or "gaofen" in text or "beidou" in text:
        return "China"
    if "soyuz" in text or "kosmos" in text or "roscosmos" in text or "angara" in text:
        return "Russia"
    if "pslv" in text or "gslv" in text or "lvm3" in text or "isro" in text:
        return "India"
    if "jaxa" in text or "tanegashima" in text or "qzss" in text or "h3" in text:
        return "Japan"

    for country_group, hints in COUNTRY_NAME_HINTS.items():
        if any(text_contains_word(text, hint) for hint in hints):
            return country_group

    return "Other / Unclear"


def infer_likely_role(text: str) -> str:
    for role, patterns in GLOBAL_ROLE_PATTERNS.items():
        if any(pattern in text for pattern in patterns):
            return role
    return "Government or national security mission"


def looks_sensitive(row: pd.Series) -> bool:
    name = safe_text(row.get("name")).lower()
    mission_type = safe_text(row.get("mission_type")).lower()
    mission_description = safe_text(row.get("mission_description")).lower()
    provider = safe_text(row.get("provider")).lower()
    rocket = safe_text(row.get("rocket")).lower()
    location_name = safe_text(row.get("location_name")).lower()

    text = " ".join([name, mission_type, mission_description, provider, rocket, location_name])

    if any(keyword in text for keyword in SENSITIVE_KEYWORDS):
        return True

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
            "beidou",
            "yaogan",
            "gaofen",
            "qzss",
            "navic",
            "govsatcom",
            "secure communications",
            "kosmos",
            "soyuz",
        )
        return any(token in text for token in watched_pattern)

    return False


def attach_country_sources(country_group: str, source_keys: list):
    if country_group == "United States":
        add_source(source_keys, "us_space_capabilities")
    elif country_group == "China":
        add_source(source_keys, "cn_programme")
        add_source(source_keys, "cn_english_home")
    elif country_group == "India":
        add_source(source_keys, "in_launch_missions")
        add_source(source_keys, "in_spacecraft_missions")
    elif country_group == "Japan":
        add_source(source_keys, "jp_missions")
        add_source(source_keys, "jp_tanegashima")
    elif country_group == "Europe":
        add_source(source_keys, "eu_secure_comms")
    elif country_group == "United Kingdom":
        add_source(source_keys, "uk_space_command")
    elif country_group == "Russia":
        add_source(source_keys, "ru_roscosmos")


def assess_sensitive_launch(row: pd.Series) -> dict:
    name = safe_text(row.get("name"))
    mission_type = safe_text(row.get("mission_type"))
    mission_description = safe_text(row.get("mission_description"))
    provider = safe_text(row.get("provider"))
    rocket = safe_text(row.get("rocket"))
    location_name = safe_text(row.get("location_name"))
    country_group = infer_country_group(row)

    text = " ".join([name, mission_type, mission_description, provider, rocket, location_name]).lower()

    likely_role = infer_likely_role(text)
    source_keys = []
    attach_country_sources(country_group, source_keys)

    why_sensitive = (
        "Public mission labels suggest a government, military, security, navigation, surveillance, or secure-communications role rather than a purely commercial or civil profile."
    )
    vehicle_context = ""

    if country_group == "United States":
        if "nrol" in text or "nro" in text:
            likely_role = "Reconnaissance or intelligence support mission"
            why_sensitive = (
                "Public naming strongly suggests an NRO-linked mission. Official NRO mission material describes NROL launches as national security payloads supporting overhead reconnaissance and intelligence support."
            )
            add_source(source_keys, "us_nrol_101")
            add_source(source_keys, "us_nrol_82")
            add_source(source_keys, "us_nro_launch")
        elif any(token in text for token in ["gps", "positioning", "navigation", "timing"]):
            likely_role = "Positioning, navigation, and timing mission"
            why_sensitive = (
                "Public mission labels suggest a PNT payload. Official U.S. Space Force material says GPS provides military and civil positioning, navigation, and timing support."
            )
            add_source(source_keys, "us_gps")
            add_source(source_keys, "us_milcom_pnt")
        elif any(token in text for token in ["wgs", "satcom", "communications", "communication"]):
            likely_role = "Protected communications mission"
            why_sensitive = (
                "Public naming suggests a protected communications or military SATCOM role. Official SSC material says MILCOM and PNT delivers military SATCOM and protected command-and-control links."
            )
            add_source(source_keys, "us_milcom_pnt")
        elif any(token in text for token in ["gssap", "space surveillance", "space situational awareness"]):
            likely_role = "Space surveillance or space domain awareness mission"
            why_sensitive = (
                "Public mission labels suggest a space surveillance function. SSC mission material says GSSAP supports U.S. Space Command space surveillance operations."
            )
            add_source(source_keys, "us_ussf_87")
            add_source(source_keys, "us_vulcan_nssl")
        elif any(token in text for token in ["missile warning", "missile tracking", "tracking layer", "dsp"]):
            likely_role = "Missile warning or tracking mission"
            why_sensitive = (
                "Public mission labels suggest missile warning or missile tracking architecture. SSC launch material explicitly links this mission family to missile warning and missile tracking payloads."
            )
            add_source(source_keys, "us_missile_tracking")
        else:
            add_source(source_keys, "us_space_capabilities")
            add_source(source_keys, "us_vulcan_nssl")

        if "electron" in text or "rocket lab" in text:
            vehicle_context = "Rocket Lab's Electron has an official NRO mission pedigree for dedicated small-satellite launches."
            add_source(source_keys, "us_nrol_151")
        elif "falcon 9" in text or "spacex" in text:
            vehicle_context = "Falcon 9 has a clear national security launch pedigree in official U.S. mission material."
            add_source(source_keys, "us_nrol_87")
        elif "atlas v" in text:
            vehicle_context = "Atlas V appears in official NRO national security mission history."
            add_source(source_keys, "us_nrol_101")
        elif "delta iv" in text:
            vehicle_context = "Delta IV Heavy appears in official NRO heavy-lift mission history."
            add_source(source_keys, "us_nrol_82")
        elif "vulcan" in text or "united launch alliance" in text or "ula" in text:
            vehicle_context = "Vulcan is officially certified for National Security Space Launch missions."
            add_source(source_keys, "us_vulcan_nssl")

    elif country_group == "China":
        if any(token in text for token in ["yaogan", "gaofen", "remote sensing", "earth observation", "surveillance", "reconnaissance"]):
            likely_role = "Reconnaissance or remote sensing mission"
            why_sensitive = (
                "Public mission naming suggests a Chinese remote-sensing or surveillance-related payload. Official CNSA programme language says China's space programme serves scientific development, national rights and interests, and national security."
            )
        elif any(token in text for token in ["beidou", "positioning", "navigation", "timing"]):
            likely_role = "Positioning, navigation, and timing mission"
            why_sensitive = (
                "Public mission naming suggests a Chinese navigation or timing payload. That mission family has national infrastructure and strategic utility."
            )
        else:
            why_sensitive = (
                "Public launch naming or mission text suggests a state-linked Chinese payload rather than a purely commercial mission. Official CNSA programme language explicitly includes national rights, interests, and national security."
            )

        add_source(source_keys, "cn_programme")
        add_source(source_keys, "cn_english_home")

        if "long march" in text:
            vehicle_context = "The mission appears linked to China's Long March launch family, which is central to state launch activity and official CNSA reporting."

    elif country_group == "India":
        if any(token in text for token in ["navic", "irnss", "positioning", "navigation", "timing"]):
            likely_role = "Positioning, navigation, and timing mission"
            why_sensitive = (
                "Public naming suggests an Indian navigation or timing-related mission. PNT systems are strategically significant because they underpin national infrastructure and navigation resilience."
            )
        elif any(token in text for token in ["cartosat", "risat", "earth observation", "remote sensing", "eos"]):
            likely_role = "Reconnaissance or remote sensing mission"
            why_sensitive = (
                "Public naming suggests an Indian Earth observation or remote-sensing payload. These missions can have dual-use value for mapping, border monitoring, or strategic awareness."
            )
        elif any(token in text for token in ["gsat", "satcom", "communication"]):
            likely_role = "Protected communications mission"
            why_sensitive = (
                "Public naming suggests a communications payload. Communications spacecraft can overlap with state resilience and secure government usage."
            )
        else:
            why_sensitive = (
                "Public naming suggests a state-led Indian mission rather than a purely commercial launch. Official ISRO mission pages provide broader mission and launcher context."
            )

        add_source(source_keys, "in_launch_missions")
        add_source(source_keys, "in_spacecraft_missions")
        add_source(source_keys, "in_launchers")

        if any(token in text for token in ["pslv", "eos"]):
            add_source(source_keys, "in_pslv_c62")
            vehicle_context = "The mission appears associated with ISRO's PSLV family, which is regularly used for state and Earth-observation missions."
        elif any(token in text for token in ["gslv", "lvm3"]):
            vehicle_context = "The mission appears linked to India's heavier national launch vehicle families, which often support major state payloads."

    elif country_group == "Japan":
        if any(token in text for token in ["qzss", "quasi-zenith", "positioning", "navigation", "timing"]):
            likely_role = "Positioning, navigation, and timing mission"
            why_sensitive = (
                "Public naming suggests a Japanese regional navigation or timing payload. These systems are strategically relevant because they support navigation resilience and national infrastructure."
            )
        elif any(token in text for token in ["satellite", "earth observation", "observation", "remote sensing"]):
            likely_role = "Reconnaissance or remote sensing mission"
            why_sensitive = (
                "Public naming suggests an observation or remote-sensing mission. Even when described broadly, such payloads can have dual-use relevance."
            )
        else:
            why_sensitive = (
                "Public naming suggests a Japanese state or agency mission rather than a purely private launch. Official JAXA material gives programme and vehicle context."
            )

        add_source(source_keys, "jp_missions")
        add_source(source_keys, "jp_satellite_topics")

        if "h3" in text:
            add_source(source_keys, "jp_h3")
            vehicle_context = "The mission appears linked to Japan's H3 launch vehicle, now positioned by JAXA as a mainstay national launcher."
        elif any(token in text for token in ["tanegashima", "uchinoura"]):
            add_source(source_keys, "jp_tanegashima")
            vehicle_context = "The mission appears associated with a Japanese national launch site used for state launch activity."

    elif country_group == "Europe":
        if any(token in text for token in ["govsatcom", "secure communications", "spainsat", "satcom", "communication"]):
            likely_role = "Protected communications mission"
            why_sensitive = (
                "Public naming suggests a European secure communications or government satcom function. Official ESA material explicitly describes secure communications support for governments and public users in Europe."
            )
            add_source(source_keys, "eu_secure_comms")
            add_source(source_keys, "eu_govsatcom")
            add_source(source_keys, "eu_iris2_support")
            if "spainsat" in text:
                add_source(source_keys, "eu_spainsat_ng")
        else:
            why_sensitive = (
                "Public naming suggests a government-backed European mission or a payload with public-sector resilience relevance."
            )
            add_source(source_keys, "eu_secure_comms")

        if any(token in text for token in ["ariane", "vega", "arianespace"]):
            vehicle_context = "The mission appears linked to European institutional launch infrastructure or launch services."

    elif country_group == "United Kingdom":
        if any(token in text for token in ["skynet", "satcom", "communication"]):
            likely_role = "Protected communications mission"
        why_sensitive = (
            "Public naming suggests a UK government or defence-linked mission. UK official space command material is a better fit here than defaulting to U.S. mission references."
        )
        add_source(source_keys, "uk_space_command")

    elif country_group == "Russia":
        if any(token in text for token in ["kosmos", "reconnaissance", "surveillance"]):
            likely_role = "Reconnaissance or remote sensing mission"
            why_sensitive = (
                "Public naming suggests a Russian state-linked payload. Names like Kosmos are often associated with government-led or strategically relevant missions rather than ordinary commercial launches."
            )
        else:
            why_sensitive = (
                "Public naming suggests a Russian government or state-linked mission rather than a purely commercial profile."
            )
        add_source(source_keys, "ru_roscosmos")

        if any(token in text for token in ["soyuz", "fregat", "angara"]):
            vehicle_context = "The mission appears associated with a core Russian launch vehicle family used in state launch activity."

    else:
        likely_role = infer_likely_role(text)
        why_sensitive = (
            "Public naming suggests a government, surveillance, navigation, or secure-communications role, but the country-specific official context is less certain from the available metadata alone."
        )

    if not source_keys:
        if country_group == "China":
            add_source(source_keys, "cn_programme")
        elif country_group == "India":
            add_source(source_keys, "in_launch_missions")
        elif country_group == "Japan":
            add_source(source_keys, "jp_missions")
        elif country_group == "Europe":
            add_source(source_keys, "eu_secure_comms")
        elif country_group == "United Kingdom":
            add_source(source_keys, "uk_space_command")
        elif country_group == "Russia":
            add_source(source_keys, "ru_roscosmos")
        else:
            add_source(source_keys, "us_space_capabilities")

    official_basis = " | ".join(source["title"] for source in source_objects(source_keys)[:3])

    return {
        "country_group": country_group,
        "likely_role": likely_role,
        "why_sensitive": why_sensitive,
        "vehicle_context": vehicle_context,
        "official_basis": official_basis,
        "source_keys": source_keys,
    }


def save_launch_event(launch):
    mission_name = safe_text(launch.get("name")) or "unknown-launch"
    launch_time = launch.get("net")
    country = safe_text(launch.get("country_code")) or "Unknown"
    subcategory = safe_text(launch.get("mission_type")) or "orbital_launch"
    provider = safe_text(launch.get("provider")) or "Unknown"

    if pd.isna(launch_time):
        return

    if hasattr(launch_time, "isoformat"):
        launch_time = launch_time.isoformat()

    event_id = f"launch_{mission_name}_{launch_time}"

    log_event(
        {
            "event_id": event_id,
            "timestamp": launch_time,
            "country": country,
            "event_type": "launch",
            "subcategory": subcategory,
            "source": provider,
            "sensitive": looks_sensitive(launch),
        }
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_upcoming_launches():
    url = f"https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit={UPCOMING_LIMIT}&mode=detailed"
    raw = fetch_json_with_retry(url)["results"]
    df = build_launch_rows(raw)
    df = clean_time_col(df, "net")
    if not df.empty:
        df = df.sort_values("net")
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_recent_launches():
    url = f"https://ll.thespacedevs.com/2.2.0/launch/previous/?limit={RECENT_LIMIT}&mode=detailed"
    raw = fetch_json_with_retry(url)["results"]
    df = build_launch_rows(raw)
    df = clean_time_col(df, "net")
    if not df.empty:
        df = df.sort_values("net", ascending=False)
    return df


def render_metric_card(title, value, detail, accent):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="accent-bar" style="background:{accent};"></div>
            <div class="metric-label">{title}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-detail">{detail}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_signal_card(title, text):
    st.markdown(
        f"""
        <div class="signal-card">
            <div class="signal-card-label">{html.escape(title)}</div>
            <div class="signal-card-text">{html.escape(text)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def apply_filters(df: pd.DataFrame, search_query: str, providers):
    filtered = df.copy()
    if filtered.empty:
        return filtered

    if providers:
        filtered = filtered[filtered["provider"].isin(providers)]

    if search_query:
        search_text = search_query.lower()
        filtered = filtered[
            filtered["name"].fillna("").str.lower().str.contains(search_text, na=False)
            | filtered["provider"].fillna("").str.lower().str.contains(search_text, na=False)
            | filtered["rocket"].fillna("").str.lower().str.contains(search_text, na=False)
            | filtered["mission_type"].fillna("").str.lower().str.contains(search_text, na=False)
            | filtered["location_name"].fillna("").str.lower().str.contains(search_text, na=False)
        ]

    return filtered.reset_index(drop=True)


def build_launch_icon_html(color):
    return f"""
        <div style="position: relative; width: 32px; height: 32px; transform: translate(-16px, -16px);">
            <div style="width: 32px; height: 32px; border-radius: 999px; background: rgba(8, 18, 30, 0.82);
                        box-shadow: 0 0 0 1px rgba(255,255,255,0.18), 0 12px 22px {color}44;
                        display: flex; align-items: center; justify-content: center;">
                <svg viewBox="0 0 24 24" width="16" height="16">
                    <path d="M12 3c2.2 1.6 3.8 4.1 4.4 7.2l2.5 2-1.4 1.4-2.4-.6-1.2 1.6 1.2 4.1-1.5 1.4-2.6-3-2.6 3-1.5-1.4 1.2-4.1-1.2-1.6-2.4.6-1.4-1.4 2.5-2C8.2 7.1 9.8 4.6 12 3z"
                          fill="{color}" stroke="#ffffff" stroke-width="0.75"></path>
                </svg>
            </div>
        </div>
    """


def build_popup_html(row):
    return f"""
        <div style="min-width: 260px; font-family: Segoe UI, sans-serif;">
            <div style="font-size: 15px; font-weight: 700; color: #09111f; margin-bottom: 6px;">
                {html.escape(safe_text(row.get('name') or 'Unknown launch'))}
            </div>
            <table style="width:100%; border-collapse:collapse; font-size:12px;">
                <tr><td style="padding:4px 0; color:#5a6d85;">Provider</td><td style="padding:4px 0;">{html.escape(safe_text(row.get('provider') or 'Unknown'))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Rocket</td><td style="padding:4px 0;">{html.escape(safe_text(row.get('rocket') or 'Unknown'))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Mission</td><td style="padding:4px 0;">{html.escape(safe_text(row.get('mission_type') or 'Unknown'))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Time</td><td style="padding:4px 0;">{html.escape(format_time(row.get('net')))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Location</td><td style="padding:4px 0;">{html.escape(safe_text(row.get('location_name') or 'Unknown'))}</td></tr>
                <tr><td style="padding:4px 0; color:#5a6d85;">Map layer</td><td style="padding:4px 0;">{html.escape(safe_text(row.get('map_layer') or 'Launch'))}</td></tr>
            </table>
        </div>
    """


def build_map_dataframe(upcoming_df, failed_df, sensitive_df):
    frames = []

    if not upcoming_df.empty:
        upcoming_map = upcoming_df.copy()
        upcoming_map["map_layer"] = "Upcoming"
        upcoming_map["map_color"] = STATUS_COLORS["Upcoming"]
        frames.append(upcoming_map)

    if not failed_df.empty:
        failed_map = failed_df.copy()
        failed_map["map_layer"] = "Recent failure"
        failed_map["map_color"] = STATUS_COLORS["Recent failure"]
        frames.append(failed_map)

    if not sensitive_df.empty:
        sensitive_map = sensitive_df.copy()
        sensitive_map["map_layer"] = "Sensitive"
        sensitive_map["map_color"] = STATUS_COLORS["Sensitive"]
        frames.append(sensitive_map)

    if not frames:
        return pd.DataFrame()

    map_df = pd.concat(frames, ignore_index=True)
    map_df = map_df.dropna(subset=["lat", "lon"]).copy()
    map_df = map_df.drop_duplicates(subset=["name", "net", "map_layer"])
    return map_df.reset_index(drop=True)


def create_launch_map(map_df: pd.DataFrame, map_theme: str):
    if map_df.empty:
        return None

    center_lat = map_df["lat"].mean()
    center_lon = map_df["lon"].mean()

    launch_map = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=2,
        control_scale=True,
        prefer_canvas=True,
        tiles=None,
    )

    for theme_name, theme_config in MAP_THEMES.items():
        folium.TileLayer(
            tiles=theme_config["tiles"],
            attr=theme_config["attr"],
            name=theme_name,
            show=theme_name == map_theme,
        ).add_to(launch_map)

    Fullscreen(position="topright").add_to(launch_map)
    MousePosition(
        position="bottomright",
        separator=" | ",
        lng_first=False,
        num_digits=2,
        prefix="Lat / Lon",
    ).add_to(launch_map)

    for layer_name in ["Upcoming", "Recent failure", "Sensitive"]:
        layer_rows = map_df[map_df["map_layer"] == layer_name]
        if layer_rows.empty:
            continue

        layer_group = folium.FeatureGroup(name=layer_name, show=True)
        for _, row in layer_rows.iterrows():
            folium.Marker(
                location=[row["lat"], row["lon"]],
                tooltip=f"{safe_text(row.get('name'))} | {layer_name}",
                popup=folium.Popup(build_popup_html(row), max_width=360),
                icon=DivIcon(html=build_launch_icon_html(row["map_color"])),
            ).add_to(layer_group)
        layer_group.add_to(launch_map)

    folium.LayerControl(collapsed=True).add_to(launch_map)
    return launch_map


def display_launch_table(df: pd.DataFrame):
    if df.empty:
        return df

    display_df = df[
        ["name", "net", "status", "provider", "rocket", "mission_type", "location_name"]
    ].copy()
    display_df["net"] = display_df["net"].apply(format_time)
    display_df = display_df.rename(
        columns={
            "name": "Launch",
            "net": "Time (UTC)",
            "status": "Status",
            "provider": "Provider",
            "rocket": "Rocket",
            "mission_type": "Mission Type",
            "location_name": "Location",
        }
    )
    return display_df


def build_role_badge(role: str) -> str:
    role_lower = safe_text(role).lower()
    badge_class = "badge-blue"
    if any(token in role_lower for token in ["reconnaissance", "surveillance", "intelligence", "national security"]):
        badge_class = "badge-red"
    elif any(token in role_lower for token in ["missile", "tracking", "warning"]):
        badge_class = "badge-amber"
    elif any(token in role_lower for token in ["communications", "navigation", "timing", "pnt"]):
        badge_class = "badge-blue"
    return f'<span class="badge {badge_class}">{html.escape(role)}</span>'


def build_country_badge(country: str) -> str:
    return f'<span class="badge badge-blue">{html.escape(country)}</span>'


def build_signal_summary(filtered_sensitive_df: pd.DataFrame) -> dict:
    default_summary = {
        "headline": "No flagged dual-use or state-linked launch activity matches the current filters.",
        "signals": [
            "No filtered sensitive launch profiles available right now.",
            "Adjust provider or search filters to widen the operational picture.",
            "Upcoming launch coverage remains available even when strategic flags are limited.",
        ],
    }

    if filtered_sensitive_df.empty:
        return default_summary

    working_df = filtered_sensitive_df.copy()
    working_df["country_group"] = working_df.apply(infer_country_group, axis=1)
    country_counts = working_df["country_group"].value_counts()

    if country_counts.empty:
        return default_summary

    top_country = country_counts.index[0]
    top_count = int(country_counts.iloc[0])
    total_sensitive = len(working_df)
    top_share = round((top_count / total_sensitive) * 100)

    headline = f"{top_country} accounts for {top_share}% of flagged dual-use or state-linked launches in the current filtered view."

    signals = []

    signals.append(f"{top_country} leads the current sensitive-launch picture with {top_count} flagged missions.")

    recon_count = 0
    nav_count = 0
    comms_count = 0
    us_count = int(country_counts.get("United States", 0))
    china_count = int(country_counts.get("China", 0))

    for _, row in working_df.iterrows():
        text = " ".join(
            [
                safe_text(row.get("name")),
                safe_text(row.get("mission_type")),
                safe_text(row.get("mission_description")),
                safe_text(row.get("provider")),
                safe_text(row.get("rocket")),
            ]
        ).lower()
        role = infer_likely_role(text).lower()
        if "reconnaissance" in role or "remote sensing" in role or "surveillance" in role:
            recon_count += 1
        if "navigation" in role or "timing" in role or "pnt" in role:
            nav_count += 1
        if "communications" in role:
            comms_count += 1

    if us_count > 0 and china_count > 0:
        if us_count > china_count:
            signals.append(
                f"U.S. flagged launch activity currently exceeds China in the filtered operational picture ({us_count} vs {china_count})."
            )
        elif china_count > us_count:
            signals.append(
                f"China currently outpaces the U.S. in flagged launch activity within the filtered view ({china_count} vs {us_count})."
            )
        else:
            signals.append(
                f"The U.S. and China are currently level in flagged launch activity inside the filtered view ({us_count} each)."
            )
    elif nav_count > 0:
        signals.append(
            f"{nav_count} flagged missions align with navigation or timing architecture, reinforcing the role of orbital PNT resilience."
        )
    elif recon_count > 0:
        signals.append(
            f"{recon_count} flagged missions align with reconnaissance or remote-sensing profiles rather than purely civil launch activity."
        )
    elif comms_count > 0:
        signals.append(
            f"{comms_count} flagged missions point toward secure communications or protected-government connectivity roles."
        )
    else:
        signals.append(
            "Current flags are concentrated in state-linked or security-adjacent launch profiles rather than purely commercial missions."
        )

    if recon_count > 0:
        signals.append(
            f"{recon_count} missions fit reconnaissance or remote-sensing patterns, suggesting continued demand for surveillance-layer orbital access."
        )
    elif comms_count > 0:
        signals.append(
            f"{comms_count} missions fit secure-communications profiles, pointing to resilience and protected-connectivity priorities."
        )
    else:
        signals.append(
            "The current mix shows dual-use orbital demand spanning launch access, strategic payload support, and state mission continuity."
        )

    return {
        "headline": headline,
        "signals": signals[:3],
    }


inject_styles()

st.markdown(
    """
    <div class="hero-card">
        <div class="hero-kicker">LIVE ORBITAL OPERATIONS</div>
        <h1 class="hero-title">Orbital Launch Monitor</h1>
        <p class="hero-copy">
            Monitor upcoming launches, recent failures, and publicly signaled sensitive missions from live launch feeds,
            then automatically log real launch events into the Strategic Insights pipeline.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

launch_error = None
recent_launch_error = None

try:
    launches_df = get_upcoming_launches()
except Exception as error:
    launches_df = pd.DataFrame()
    launch_error = str(error)

try:
    recent_launches_df = get_recent_launches()
except Exception as error:
    recent_launches_df = pd.DataFrame()
    recent_launch_error = str(error)

if not launches_df.empty:
    for _, launch_row in launches_df.iterrows():
        save_launch_event(launch_row)

if not recent_launches_df.empty:
    for _, launch_row in recent_launches_df.iterrows():
        save_launch_event(launch_row)

failed_launches_df = pd.DataFrame()
sensitive_launches_df = pd.DataFrame()

if not recent_launches_df.empty:
    now_utc = pd.Timestamp.utcnow()
    failed_mask = (
        recent_launches_df["status"]
        .fillna("")
        .str.lower()
        .str.contains("failure", na=False)
    )
    failed_launches_df = recent_launches_df[failed_mask].copy()
    failed_launches_df = failed_launches_df[
        failed_launches_df["net"] >= now_utc - pd.Timedelta(days=30)
    ].copy()

    sensitive_mask = recent_launches_df.apply(looks_sensitive, axis=1)
    sensitive_launches_df = recent_launches_df[sensitive_mask].copy()
    sensitive_launches_df = sensitive_launches_df[
        sensitive_launches_df["net"] >= now_utc - pd.Timedelta(days=120)
    ].copy()

all_providers = sorted(
    {
        provider
        for provider in pd.concat(
            [
                launches_df.get("provider", pd.Series(dtype=str)),
                recent_launches_df.get("provider", pd.Series(dtype=str)),
            ],
            ignore_index=True,
        ).dropna()
        if safe_text(provider)
    }
)

with st.sidebar:
    st.markdown("### Launch Controls")
    refresh_clicked = st.button("Refresh launch feeds", use_container_width=True)
    if refresh_clicked:
        get_upcoming_launches.clear()
        get_recent_launches.clear()
        st.rerun()

    search_query = st.text_input(
        "Search launches",
        placeholder="Launch, provider, rocket, mission, or location",
    ).strip()

    provider_filter = st.multiselect(
        "Providers",
        options=all_providers,
        default=[],
    )

    st.markdown("### Map Layers")
    show_upcoming = st.toggle("Upcoming launches", value=True)
    show_failures = st.toggle("Recent failures", value=True)
    show_sensitive = st.toggle("Sensitive launches", value=True)
    map_theme = st.selectbox("Map theme", options=list(MAP_THEMES.keys()), index=1)

filtered_upcoming_df = apply_filters(launches_df, search_query, provider_filter)
filtered_failed_df = apply_filters(failed_launches_df, search_query, provider_filter)
filtered_sensitive_df = apply_filters(sensitive_launches_df, search_query, provider_filter)

map_df = build_map_dataframe(
    filtered_upcoming_df if show_upcoming else pd.DataFrame(),
    filtered_failed_df if show_failures else pd.DataFrame(),
    filtered_sensitive_df if show_sensitive else pd.DataFrame(),
)

signal_summary = build_signal_summary(filtered_sensitive_df)

st.markdown(
    f"""
    <div class="signal-banner">
        <div class="signal-banner-label">Today’s Strategic Signal</div>
        <div class="signal-banner-text">{html.escape(signal_summary["headline"])}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

signal_cols = st.columns(3)
for idx, col in enumerate(signal_cols):
    with col:
        render_signal_card(f"Signal {idx + 1}", signal_summary["signals"][idx])

metric_columns = st.columns(4)
with metric_columns[0]:
    render_metric_card(
        "Upcoming launches",
        f"{len(filtered_upcoming_df):,}",
        "Filtered view of the live upcoming schedule",
        STATUS_COLORS["Upcoming"],
    )
with metric_columns[1]:
    render_metric_card(
        "Recent failures",
        f"{len(filtered_failed_df):,}",
        "Previous 30 days with failure status in public data",
        STATUS_COLORS["Recent failure"],
    )
with metric_columns[2]:
    render_metric_card(
        "Flagged dual-use / state-linked",
        f"{len(filtered_sensitive_df):,}",
        "Public launch profiles linked to government, military, surveillance, navigation, or protected communications",
        STATUS_COLORS["Sensitive"],
    )
with metric_columns[3]:
    if launch_error and recent_launch_error:
        render_metric_card("Feed status", "Degraded", "Upcoming and recent feeds both had upstream issues", STATUS_COLORS["Sensitive"])
    elif launch_error or recent_launch_error:
        render_metric_card("Feed status", "Partial", "One of the two launch feeds is degraded", STATUS_COLORS["Recent failure"])
    else:
        render_metric_card("Feed status", "Online", "Upcoming and recent launch feeds loaded successfully", STATUS_COLORS["Healthy"])

st.markdown("")

map_col, side_col = st.columns([3.1, 1.15], gap="large")

with map_col:
    st.markdown(
        """
        <div class="panel-card">
            <div class="panel-title">Launch Site Map</div>
            <div class="panel-copy">
                Blue marks upcoming activity, amber marks recent failures, and red marks publicly signaled dual-use or state-linked launch profiles.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if launch_error and recent_launch_error:
        st.error("Launch map is unavailable because both upstream feeds are currently degraded.")
    elif map_df.empty:
        st.info("No launch sites match the current filters.")
    else:
        launch_map = create_launch_map(map_df, map_theme)
        if launch_map is None:
            st.info("No coordinates are available for the filtered launch records.")
        else:
            st_folium(launch_map, use_container_width=True, height=720)

with side_col:
    st.markdown("#### Next Strategic Event")
    if filtered_upcoming_df.empty:
        st.info("No upcoming launch matches the current filters.")
    else:
        next_launch = filtered_upcoming_df.sort_values("net").iloc[0]
        next_assessment = assess_sensitive_launch(next_launch) if looks_sensitive(next_launch) else None

        if next_assessment:
            why_it_matters = next_assessment["why_sensitive"]
            country_label = next_assessment["country_group"]
            role_label = next_assessment["likely_role"]
        else:
            country_label = infer_country_group(next_launch)
            role_label = "Scheduled launch activity"
            why_it_matters = "This is the next confirmed launch in the filtered queue and should be watched as the next operational change on the board."

        st.markdown(
            f"""
            <div class="event-card">
                <div class="event-kicker">Next launch to watch</div>
                <div class="event-title">{html.escape(safe_text(next_launch.get("name") or "Unknown launch"))}</div>
                <div style="margin-bottom:0.45rem;">
                    {build_country_badge(country_label)}
                    {build_role_badge(role_label)}
                </div>
                <div class="event-copy">
                    <strong>Time:</strong> {html.escape(format_time(next_launch.get("net")))}<br>
                    <strong>Provider:</strong> {html.escape(safe_text(next_launch.get("provider") or "Unknown provider"))}<br>
                    <strong>Vehicle:</strong> {html.escape(safe_text(next_launch.get("rocket") or "Unknown rocket"))}<br>
                    <strong>Location:</strong> {html.escape(safe_text(next_launch.get("location_name") or "Unknown location"))}<br><br>
                    <strong>Why it matters:</strong> {html.escape(why_it_matters)}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("#### Official context basis")
    st.markdown(
        """
        <div class="panel-card">
            <div class="panel-title">How sensitive mission logic is explained</div>
            <div class="panel-copy">
                This layer uses country-aware logic, mission-family cues, launch geography, public mission labels, and official English-language source pages where available.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Mission context below is best-effort and English-language only. It uses official source pages where possible based on mission family, launch geography, and country-level cues.")

tab_upcoming, tab_failed, tab_sensitive, tab_context = st.tabs(
    ["Upcoming Launches", "Recent Failures", "Sensitive Missions", "Official Mission Context"]
)

with tab_upcoming:
    st.markdown("### Upcoming Launches")
    if launch_error:
        st.warning("The upstream upcoming-launch feed is temporarily unavailable.")
    elif filtered_upcoming_df.empty:
        st.info("No upcoming launches match the current filters.")
    else:
        st.dataframe(display_launch_table(filtered_upcoming_df), use_container_width=True, hide_index=True)

with tab_failed:
    st.markdown("### Recent Failed Launches")
    if recent_launch_error:
        st.warning("The recent-launch feed is temporarily unavailable.")
    elif filtered_failed_df.empty:
        st.success("No failed launches were found in the last 30 days for the current filters.")
    else:
        st.dataframe(display_launch_table(filtered_failed_df), use_container_width=True, hide_index=True)

with tab_sensitive:
    st.markdown("### Publicly Signaled Sensitive Launches")
    st.caption("This table uses public naming, mission labels, launch metadata, and broader dual-use indicators to flag launches that look government, military, intelligence, navigation, surveillance, or protected-communications linked.")
    if recent_launch_error:
        st.warning("Sensitive launch detection depends on the recent-launch feed, which is temporarily unavailable.")
    elif filtered_sensitive_df.empty:
        st.info("No publicly signaled sensitive launches match the current filters.")
    else:
        st.dataframe(display_launch_table(filtered_sensitive_df), use_container_width=True, hide_index=True)

with tab_context:
    st.markdown("### Official Mission Context")
    st.caption("Possible reasons a launch may be sensitive, paired with official English-language mission and programme references from the most relevant country or agency when available.")
    if recent_launch_error:
        st.warning("Mission context is unavailable because the recent-launch feed could not be loaded.")
    elif filtered_sensitive_df.empty:
        st.info("No sensitive launch profiles are available for context right now.")
    else:
        context_rows = filtered_sensitive_df.head(min(10, len(filtered_sensitive_df))).copy()
        context_records = []
        for _, row in context_rows.iterrows():
            assessment = assess_sensitive_launch(row)
            context_records.append(
                {
                    "Launch": safe_text(row.get("name")),
                    "Time (UTC)": format_time(row.get("net")),
                    "Country / Context": assessment["country_group"],
                    "Provider": safe_text(row.get("provider")),
                    "Rocket": safe_text(row.get("rocket")),
                    "Likely Role": assessment["likely_role"],
                    "Why It Could Be Sensitive": assessment["why_sensitive"],
                    "Vehicle Context": assessment["vehicle_context"],
                    "Official Basis": assessment["official_basis"],
                    "sources": assessment["source_keys"],
                }
            )

        context_df = pd.DataFrame(context_records)
        summary_df = context_df[
            ["Launch", "Time (UTC)", "Country / Context", "Provider", "Rocket", "Likely Role", "Official Basis"]
        ].copy()
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        for _, row in context_df.iterrows():
            st.markdown(
                f"""
                <div class="panel-card">
                    <div class="panel-title">{html.escape(row['Launch'])}</div>
                    <div style="margin: 0.35rem 0 0.4rem 0;">
                        {build_country_badge(row['Country / Context'])}
                        {build_role_badge(row['Likely Role'])}
                    </div>
                    <div class="panel-copy">
                        <strong>Why it could be sensitive:</strong> {html.escape(row['Why It Could Be Sensitive'])}<br>
                        <strong>Launch vehicle context:</strong> {html.escape(row['Vehicle Context'] or 'No extra vehicle-specific note applied for this launch.')}<br>
                        <strong>Official basis:</strong> {html.escape(row['Official Basis'])}
                    </div>
                    {source_links_html(row['sources'])}
                </div>
                """,
                unsafe_allow_html=True,
            )

st.markdown("---")
st.caption(
    f"Loaded {len(filtered_upcoming_df):,} upcoming launches, {len(filtered_failed_df):,} recent failures, and "
    f"{len(filtered_sensitive_df):,} flagged dual-use or state-linked launch profiles under the current filters."
)
