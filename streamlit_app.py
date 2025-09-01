
import streamlit as st, pandas as pd, altair as alt
from etl.clean import load_cleaned

st.set_page_config(page_title="International Student Visa Dashboard", page_icon="🧳", layout="wide")

DATA_FILE = "data/H1B_Visa_Sponsors_2025.csv"

st.title("International Student Visa Dashboard")

@st.cache_data
def load_data():
    try:
        return load_cleaned(DATA_FILE)
    except Exception:
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No data found. Please add H1B_Visa_Sponsors_2025.csv to data/.")
else:
    with st.sidebar:
        st.header("Filters")
        years = sorted(df["decision_year"].unique())
        year_sel = st.multiselect("Year", years, default=years)
        states = sorted(df["state"].unique())
        state_sel = st.multiselect("State", states, default=[])
        employer_sel = st.multiselect("Employer", sorted(df["employer"].unique()), default=[])

    f = df[df["decision_year"].isin(year_sel)]
    if state_sel: f = f[f["state"].isin(state_sel)]
    if employer_sel: f = f[f["employer"].isin(employer_sel)]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Cases", f.shape[0])
    if len(f) > 0:
        c2.metric("Approval Rate", f"{(f['case_status']=='CERTIFIED').mean()*100:.1f}%")
        c3.metric("Median Wage (Annualized)", f"${f['wage_annual'].median():,.0f}")
    else:
        c2.metric("Approval Rate", "—")
        c3.metric("Median Wage (Annualized)", "—")

    st.subheader("Yearly Filing Trend")
    yearly = f.groupby("decision_year").size().reset_index(name="count")
    st.altair_chart(alt.Chart(yearly).mark_line(point=True).encode(x="decision_year:O", y="count:Q"), use_container_width=True)

    st.subheader("Top Employers")
    top_emp = f.groupby("employer").size().reset_index(name="count").sort_values("count", ascending=False).head(15)
    st.dataframe(top_emp, use_container_width=True)

    st.subheader("Records")
    st.dataframe(f.head(200), use_container_width=True)
