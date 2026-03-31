import streamlit as st

# ----------------------------
# PAGE CONFIG
# ----------------------------

st.set_page_config(
    page_title="Signal Console",
    page_icon="◉",
    layout="wide",
)

# ----------------------------
# HIDE DEFAULT SIDEBAR NAV (backup CSS if TOML fails)
# ----------------------------

st.markdown("""
<style>

/* Hide Streamlit default sidebar nav completely */
[data-testid="stSidebarNav"] {
    display: none;
}

/* Clean sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(9,19,32,.98), rgba(9,19,32,.92));
    border-right: 1px solid rgba(130,161,191,.18);
}

</style>
""", unsafe_allow_html=True)

# ----------------------------
# CUSTOM SIDEBAR
# ----------------------------

with st.sidebar:
    st.markdown("### SIGNAL CONSOLE")

    st.page_link("pages/0_Home.py", label="Home")
    st.page_link("pages/1_Orbital_Launch_Monitor.py", label="Launch Intelligence")
    st.page_link("pages/3_Satellite_Activity.py", label="Satellite Watch")
    st.page_link("pages/Strategic_Insights.py", label="Strategic Insights")

# ----------------------------
# AUTO REDIRECT TO HOME
# ----------------------------

st.switch_page("pages/0_Home.py")
