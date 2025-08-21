"""Example offline feature computation (training pipeline)."""

import pandas as pd


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build features for training pipeline.
    
    This represents the offline feature engineering that happens during training.
    """
    df = df.copy()
    
    # Calculate 7-day rolling spend with proper rounding
    df["spend_7d"] = (df["price"] * df["qty"]).rolling(7, min_periods=1).sum().round(2)
    
    # Return features only
    return df[["user_id", "ts", "spend_7d", "country"]]