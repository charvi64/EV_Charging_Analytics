from extract_spark import extract_data
from transform_spark import transform_data
from load_spark import load_data
from analytics_spark import run_analytics

def run_pipeline():
    print("\n==========================================")
    print("⚡ STARTING EV CHARGING SPARK ETL + ANALYTICS")
    print("==========================================")

    # 👇 Path to your input CSV file
    input_file = "../data/detailed_ev_charging_stations.csv"
    # 👇 Path to your cleaned data output folder
    output_path = "../output/cleaned_ev_data_spark"

    # Run ETL
    df = extract_data(input_file)
    cleaned_df = transform_data(df)
    load_data(cleaned_df, output_path)

    # Run Analytics
    run_analytics()

    print("\n==========================================")
    print("✅ FULL PIPELINE (ETL + ANALYTICS) COMPLETED SUCCESSFULLY!")
    print("==========================================")

if __name__ == "__main__":
    run_pipeline()
