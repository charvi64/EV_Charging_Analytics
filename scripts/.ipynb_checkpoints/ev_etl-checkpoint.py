import pandas as pd
import numpy as np
import os

def load_data(file_path):
    print("📥 Loading data...")
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows.")
    return df

def clean_columns(df):
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
        .str.replace("/", "_", regex=False)
    )
    return df

def transform_data(df):
    print("🧹 Cleaning data...")
    df = df.drop_duplicates()
    
    # Check if the column exists before trying to access it
    if "Cost_USD_kWh" in df.columns:  # Note: '/' was replaced with '_' in clean_columns
        df["Cost_USD_kWh"] = df["Cost_USD_kWh"].fillna(df["Cost_USD_kWh"].mean())
    elif "Cost_USD/kWh" in df.columns:  # Try original column name if clean_columns wasn't applied
        df["Cost_USD/kWh"] = df["Cost_USD/kWh"].fillna(df["Cost_USD/kWh"].mean())
    
    if "Reviews_Rating" in df.columns:
        df["Reviews_Rating"] = df["Reviews_Rating"].fillna(df["Reviews_Rating"].median())
    
    if "Renewable_Energy_Source" in df.columns:
        df["Renewable_Energy_Source"] = df["Renewable_Energy_Source"].fillna("Unknown")
    
    return df

def save_clean_data(df, output_path):
    print("💾 Saving cleaned data...")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved: {output_path}")

def run_pipeline():
    input_path = "/Users/charvisaig/ev_charging_analysis/data/detailed_ev_charging_stations.csv"
    output_path = "/Users/charvisaig/ev_charging_analysis/notebooks/cleaned_ev_stations.csv"
    
    df = load_data(input_path)
    df = clean_columns(df)
    
    # Print column names to debug
    print("Available columns:", df.columns.tolist())
    
    df = transform_data(df)
    save_clean_data(df, output_path)

if __name__ == "__main__":
    run_pipeline()