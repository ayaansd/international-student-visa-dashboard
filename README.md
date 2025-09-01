International Student Visa Dashboard

🧳 International Student Visa Dashboard is an end-to-end pipeline and dashboard that helps students and professionals explore H-1B filings, sponsors, wages, and approval rates. It integrates scrapers, ETL, Prefect orchestration, and a Streamlit dashboard into one project.

Built to run on free-tier environments (≤8 GB RAM), this project showcases how to combine data engineering + analysis + visualization into a working, portfolio-ready system.

🚩 What It Solves

❌ Problem: Visa-related data (H-1B, sponsors, wages, approvals) is scattered across multiple sources (USCIS, Trackitt, OFLC disclosure).

✅ Solution: Centralize collection, clean it, and expose insights through an interactive dashboard:

Aggregates data from multiple sources (official + community).

Normalizes wages (hourly/weekly → annual).

Provides filters + trends + top employer views.

Helps international students quickly explore the job/visa landscape.
📊 Dashboard Features

Filters: by year, state, employer, case status

KPIs: total cases, approval rate, median annualized wage

Charts: yearly filing trends, wage distribution, top employers

Table: filtered results (up to 200 rows)

Extendable: add predictors or OPT/CPT analysis

📊 Data Sources

USCIS → visa case updates & processing times (scraped).

Trackitt → community self-reports of timelines and outcomes.

H-1B Disclosure Data (OFLC) → employer filings, wages, SOC codes.

⚠️ Respect robots.txt and ToS when scraping. For production, prefer official datasets.

🛠️ Architecture & Flow

<img width="451" height="627" alt="image" src="https://github.com/user-attachments/assets/04c370c6-a557-40e1-83c3-9956fde801d9" />

CI & Proof

tests/test_clean.py validates wage normalization and cleaning.

.github/workflows/ci.yml runs tests automatically on push/PR.

🗺️ Roadmap

Add more visualizations (geo maps, employer segmentation).

Extend to OPT/CPT analysis for international students.

Build a simple wage predictor (linear regression or tree model).

Deploy dashboard on Streamlit Cloud or Render.

Integrate Prefect scheduling for auto-refresh.

📜 License

MIT — free to use and adapt.
