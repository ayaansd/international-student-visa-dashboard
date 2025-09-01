
import pandas as pd
ANNUAL_HOURS = 2080.0

def normalize_wage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['wage_unit'] = df['wage_unit'].str.upper().str.strip()
    def to_annual(r):
        u = r['wage_unit']; w = float(r['wage_offered'])
        if u == 'YEAR': return w
        if u == 'HOUR': return w * ANNUAL_HOURS
        if u == 'WEEK': return w * 52.0
        return w
    df['wage_annual'] = df.apply(to_annual, axis=1)
    return df

def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ['employer','job_title','city','state','case_status']:
        df[c] = df[c].astype(str).str.strip()
    df['employer'] = df['employer'].str.title()
    df['job_title'] = df['job_title'].str.title()
    df['city'] = df['city'].str.title()
    df['state'] = df['state'].str.upper()
    return df

def load_cleaned(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = basic_clean(df)
    df = normalize_wage(df)
    return df
