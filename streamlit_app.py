# streamlit_app.py
import streamlit as st
import pandas as pd
import subprocess, sys, json

st.set_page_config(page_title="📶 WE Usage Dashboard", layout="wide")
st.title("📶 Telecom Egypt Usage")

def load_data():
    cmd = [sys.executable, "we_scraper.py"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        st.error("🚨 Scraper failed:\n" + proc.stderr)
        return []
    try:
        return json.loads(proc.stdout)
    except Exception as e:
        st.error("Failed to parse JSON: " + str(e))
        return []

if st.button("🔄 Refresh"):
    st.experimental_rerun()

with st.spinner("⏳ Fetching latest usage…"):
    data = load_data()

if not data:
    st.stop()

df = pd.DataFrame(data)

# Formatting
df["Balance"]      = df["Balance"].map(lambda x: f"{int(x):,} EGP")
df["Renewal Cost"] = df["Renewal Cost"].map(lambda x: f"{int(x):,} EGP")

# Center text in table
centered = df.style.set_properties(**{"text-align":"center"}).set_table_styles(
    [{"selector":"th","props":[("text-align","center")]}]
)

# Conditional color for Remaining
def col_rem(val):
    if val < 20: return "background-color: red; color: white;"
    if val < 80: return "background-color: yellow;"
    return ""

centered = centered.applymap(col_rem, subset=["Remaining"])

st.subheader("Usage Overview")
st.write(centered, unsafe_allow_html=True)

st.subheader("Remaining vs Used (GB)")
chart1 = df.set_index("Store")[["Remaining","Used"]]
st.bar_chart(chart1)

st.subheader("Balance & Renewal Cost (EGP)")
money = (
    df.set_index("Store")[["Balance","Renewal Cost"]]
      .replace({r",| EGP":""}, regex=True)
      .astype(int)
)
st.bar_chart(money)
