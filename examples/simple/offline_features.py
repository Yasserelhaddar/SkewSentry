import pandas as pd


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    amt = (df["price"] * df["qty"]).rolling(7, min_periods=1).sum()
    df = df.copy()
    df["spend_7d"] = amt.round(2)
    return df[["user_id", "ts", "spend_7d", "country"]]

