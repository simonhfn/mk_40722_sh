from typing import Iterable, Dict, Any

import pandas as pd


def extract_company_from_email(email: str):
    if email != "" and email is not None:
        return email.split("@")[-1].split(".")[0]
    else:
        return "Unknown company"


def preprocess(df: pd.DataFrame):
    df["company"] = df['email'].apply(lambda x: extract_company_from_email(x))
    return df


def pretty_printing(df: pd.DataFrame, colOI: str, nb_res: int = 5):
    print(f"---------- TOP events for {colOI}-------------")
    min_values = min(len(df), nb_res)
    for i, row in df.sort_values("count", ascending=False).head(min_values).iterrows():
        print(f"{row[colOI]} -> {row['count']}")
    print("------------------------------------------------")


def perform_analysis_on_events(df: pd.DataFrame, columnsOI: Iterable[str]) -> Dict[str, Any]:
    res = {}
    for col in columnsOI:
        subdf = df.groupby([col]).size().reset_index(name='count')
        res[col] = subdf
    return res


