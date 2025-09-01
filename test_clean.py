
import pandas as pd
from etl.clean import normalize_wage, basic_clean

def test_norm():
    df = pd.DataFrame({
        "wage_offered":[100000,50,2000],
        "wage_unit":["YEAR","HOUR","WEEK"],
        "employer":["e"]*3,"job_title":["a"]*3,"city":["x"]*3,"state":["ny"]*3,
        "case_status":["CERTIFIED"]*3,"decision_year":[2024]*3,"soc_code":["15-0000"]*3
    })
    out = normalize_wage(df)
    assert abs(out.loc[0,'wage_annual']-100000)<1e-6
    assert abs(out.loc[1,'wage_annual']-50*2080)<1e-6
    assert abs(out.loc[2,'wage_annual']-2000*52)<1e-6

def test_clean():
    df = pd.DataFrame({
        "employer":["  acme  "],"job_title":["data scientist  "],"city":["  new york "],
        "state":["ny"],"case_status":[" certified "],
        "wage_offered":[1],"wage_unit":["YEAR"],"soc_code":["15-2051"],"decision_year":[2024]
    })
    out = basic_clean(df)
    assert out.loc[0,"employer"]=="Acme"
    assert out.loc[0,"job_title"]=="Data Scientist"
    assert out.loc[0,"city"]=="New York"
    assert out.loc[0,"state"]=="NY"
