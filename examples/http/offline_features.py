"""
Offline Feature Pipeline (Training/Batch Processing)

This represents feature engineering as done in training environments:
- Uses pandas with specific configurations
- Rolling windows with `min_periods=1` (standard for historical data)
- Standard rounding with round()
- Business logic assumptions from training data
"""

import pandas as pd
import numpy as np
from typing import Optional


def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract ML features for user behavior prediction model.
    
    This is the OFFLINE (training) feature pipeline that runs in batch mode
    on historical data using pandas/Spark.
    
    Features:
    - User spending patterns (7d, 30d windows)
    - Transaction frequency and recency
    - Product affinity scores
    - Risk indicators
    """
    df = df.copy()
    
    # Ensure proper sorting for time-based features
    df = df.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
    
    # Calculate order value (price * quantity, handling returns)
    df['order_value'] = df['price'] * df['quantity']
    df['gross_order_value'] = df['price'] * abs(df['quantity'])  # Ignore return sign
    
    # === TIME-BASED AGGREGATIONS ===
    
    # 7-day rolling spending (with min_periods=1 for training data)
    df['spend_7d'] = (
        df.groupby('user_id')['order_value']
        .rolling(7, min_periods=1)  # Standard for historical analysis
        .sum()
        .round(2)  # Standard Python rounding
        .reset_index(0, drop=True)
    )
    
    # 30-day rolling spending
    df['spend_30d'] = (
        df.groupby('user_id')['order_value']
        .rolling(30, min_periods=1)
        .sum()
        .round(2)
        .reset_index(0, drop=True)
    )
    
    # Transaction count in last 7 days
    df['txn_count_7d'] = (
        df.groupby('user_id')['transaction_id']
        .rolling(7, min_periods=1)
        .count()
        .reset_index(0, drop=True)
    )
    
    # === CATEGORY AFFINITY ===
    
    # Calculate user's category spend percentage (last 30 transactions)
    def calculate_category_affinity(group):
        """Calculate electronics affinity score for user"""
        last_30 = group.tail(30)  # Last 30 transactions per user
        electronics_spend = last_30[last_30['category'] == 'electronics']['gross_order_value'].sum()
        total_spend = last_30['gross_order_value'].sum()
        
        # Avoid division by zero
        if total_spend == 0:
            return pd.Series([0.0] * len(group), index=group.index)
            
        affinity = electronics_spend / total_spend
        return pd.Series([affinity] * len(group), index=group.index)
    
    df['electronics_affinity'] = (
        df.groupby('user_id')
        .apply(calculate_category_affinity)
        .round(3)  # Round to 3 decimal places
        .reset_index(0, drop=True)
    )
    
    # === BEHAVIORAL FEATURES ===
    
    # Average days between transactions
    def calculate_avg_days_between_txn(group):
        """Calculate average days between transactions for user"""
        if len(group) < 2:
            return pd.Series([np.nan] * len(group), index=group.index)
        
        # Calculate days between consecutive transactions
        time_diffs = group['timestamp'].diff().dt.total_seconds() / 86400  # Convert to days
        avg_days = time_diffs.mean()
        return pd.Series([avg_days] * len(group), index=group.index)
    
    df['avg_days_between_txns'] = (
        df.groupby('user_id')
        .apply(calculate_avg_days_between_txn)
        .round(1)
        .reset_index(0, drop=True)
    )
    
    # Return rate (percentage of transactions that are returns)
    def calculate_return_rate(group):
        """Calculate return rate for user"""
        return_count = (group['is_return'] == True).sum()
        total_count = len(group)
        return_rate = return_count / total_count if total_count > 0 else 0.0
        return pd.Series([return_rate] * len(group), index=group.index)
    
    df['return_rate'] = (
        df.groupby('user_id')
        .apply(calculate_return_rate)
        .round(3)
        .reset_index(0, drop=True)
    )
    
    # === RISK FEATURES ===
    
    # High-value transaction flag (>$500)
    df['is_high_value'] = df['gross_order_value'] > 500
    
    # Weekend transaction frequency
    def calculate_weekend_frequency(group):
        """Calculate percentage of transactions on weekends"""
        weekend_count = group['is_weekend'].sum()
        total_count = len(group)
        weekend_freq = weekend_count / total_count if total_count > 0 else 0.0
        return pd.Series([weekend_freq] * len(group), index=group.index)
    
    df['weekend_frequency'] = (
        df.groupby('user_id')
        .apply(calculate_weekend_frequency)
        .round(3)
        .reset_index(0, drop=True)
    )
    
    # Time since last transaction (in days)
    def calculate_days_since_last_txn(group):
        """Calculate days since user's last transaction"""
        # For training data, we calculate from a reference point (last date in data)
        max_date = df['timestamp'].max()
        last_txn_date = group['timestamp'].iloc[-1]  # Last transaction for this user
        days_since = (max_date - last_txn_date).total_seconds() / 86400
        return pd.Series([days_since] * len(group), index=group.index)
    
    df['days_since_last_txn'] = (
        df.groupby('user_id')
        .apply(calculate_days_since_last_txn)
        .round(1)
        .reset_index(0, drop=True)
    )
    
    # === USER PROFILE FEATURES ===
    
    # Country (categorical)
    df['country'] = df['country'].fillna('UNKNOWN')  # Handle nulls
    
    # User type (categorical)
    df['user_type'] = df['user_type']
    
    # Payment method (categorical)
    df['primary_payment_method'] = df['payment_method'].fillna('UNKNOWN')
    
    # Select features for model
    feature_columns = [
        # Keys for alignment
        'user_id', 'timestamp',
        
        # Spending features
        'spend_7d', 'spend_30d', 'txn_count_7d',
        
        # Behavioral features
        'electronics_affinity', 'avg_days_between_txns', 'return_rate',
        'weekend_frequency', 'days_since_last_txn',
        
        # Risk features  
        'is_high_value',
        
        # Categorical features
        'country', 'user_type', 'primary_payment_method'
    ]
    
    return df[feature_columns]


def validate_features(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean features before model training."""
    df = df.copy()
    
    # Handle infinite values that might arise from calculations
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].replace([np.inf, -np.inf], np.nan)
    
    # Ensure proper data types
    df['is_high_value'] = df['is_high_value'].astype(bool)
    
    return df
    

# Main function for testing
if __name__ == "__main__":
    # Test with sample data
    sample_data = pd.DataFrame({
        'user_id': [1, 1, 1, 2, 2],
        'timestamp': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-01', '2024-01-05']),
        'transaction_id': ['t1', 't2', 't3', 't4', 't5'],
        'price': [100.0, 50.0, 200.0, 75.0, 150.0],
        'quantity': [1, 2, 1, 1, -1],  # Last is return
        'category': ['electronics', 'clothing', 'electronics', 'books', 'electronics'],
        'is_return': [False, False, False, False, True],
        'is_weekend': [False, False, True, False, True],
        'country': ['US', 'US', 'US', 'UK', 'UK'],
        'user_type': ['regular', 'regular', 'regular', 'casual', 'casual'],
        'payment_method': ['credit_card', 'credit_card', 'paypal', 'debit_card', 'debit_card'],
    })
    
    print("Testing offline feature pipeline...")
    features = extract_features(sample_data)
    print(features.head())
    print(f"\nFeature columns: {list(features.columns)}")