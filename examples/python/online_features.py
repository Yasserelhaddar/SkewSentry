"""Example online feature computation (serving pipeline)."""

import math
import pandas as pd


def get_features(df: pd.DataFrame) -> pd.DataFrame:
    """Get features for serving pipeline.
    
    This represents the online feature engineering that happens during serving.
    Note: This has a subtle difference from offline - uses floor instead of round.
    """
    df = df.copy()
    
    # Calculate 7-day rolling spend with floor-based rounding (different from offline!)
    amt = (df["price"] * df["qty"]).rolling(7, closed="left").sum()
    df["spend_7d"] = (amt * 100).apply(lambda x: math.floor(x) if pd.notna(x) else x) / 100
    
    # Return features only
    return df[["user_id", "ts", "spend_7d", "country"]]