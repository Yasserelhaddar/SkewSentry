import math
import pandas as pd


def get_features(df: pd.DataFrame) -> pd.DataFrame:
    amt = (df["price"] * df["qty"]).rolling(7, closed="left").sum()
    amt = amt.fillna(0.0)
    df = df.copy()
    df["spend_7d"] = ( (amt * 100).apply(math.floor) / 100 )
    return df[["user_id", "ts", "spend_7d", "country"]]

