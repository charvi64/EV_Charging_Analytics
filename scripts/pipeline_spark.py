from extract_spark import extract_data
from transform_spark import transform_data
from load_spark import load_data
from analytics_spark import run_analytics
import os

def run_pipeline():
    print("\n==========================================")
    print("⚡ STARTING EV CHARGING SPARK ETL + ANALYTICS")
    print("==========================================")

    # 👇 Path to your input CSV file
    input_file = "/Users/charvisaig/ev_charging_analysis/data/detailed_ev_charging_stations.csv"
    # 👇 Path to your cleaned data output folder
    output_path = "/Users/charvisaig/ev_charging_analysis/output/cleaned_ev_data_spark"
    # 👇 Path to ETL log file
    log_file_path = "/Users/charvisaig/ev_charging_analysis/output/etl_log.txt"

    # Run ETL
    df = extract_data(input_file)
    cleaned_df = transform_data(df)
    load_data(cleaned_df, output_path)

    # Create/update ETL log
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    num_rows = cleaned_df.count()        # Spark method for row count
    num_cols = len(cleaned_df.columns)  # list of columns, len() works
    with open(log_file_path, "w") as f:
        f.write("✅ ETL Run completed successfully!\n")
        f.write(f"Rows: {num_rows}, Columns: {num_cols}\n")

    # Run Analytics
    run_analytics()

    print("\n==========================================")
    print("✅ FULL PIPELINE (ETL + ANALYTICS) COMPLETED SUCCESSFULLY!")
    print("==========================================")

if __name__ == "__main__":
    run_pipeline()
