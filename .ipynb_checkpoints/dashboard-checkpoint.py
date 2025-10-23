import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_folium import st_folium
import folium
import os

# ------------------------------------
# Page Config
# ------------------------------------
st.set_page_config(page_title="EV Charging Dashboard", layout="wide")

# ------------------------------------
# Tabs for Dashboard & ETL Status
# ------------------------------------
tab1, tab2 = st.tabs(["⚡ Dashboard", "🔄 ETL Status"])

# ------------------------------------
# Dashboard Tab
# ------------------------------------
with tab1:
    st.title("⚡ EV Charging Stations Dashboard")
    st.markdown("An interactive dashboard to explore EV charging station performance and insights.")

    # Load Data
    @st.cache_data
    def load_data():
        cleaned_path = os.path.join("output", "cleaned_ev_data_spark")
        if os.path.exists(cleaned_path) and os.listdir(cleaned_path):
            # Load the entire folder, Pandas handles all part files
            return pd.read_parquet(cleaned_path)
        else:
            st.error("Cleaned data not found. Run ETL pipeline first!")
            return pd.DataFrame()

    df = load_data()
    if not df.empty:
        # Sidebar Filters
        st.sidebar.title("🔌 Filter Options")
        charger_types = sorted(df["Charger Type"].dropna().unique())
        selected_types = st.sidebar.multiselect("Select Charger Type(s):", charger_types, default=charger_types)

        availability = sorted(df["Availability"].dropna().unique())
        selected_availability = st.sidebar.multiselect("Select Availability:", availability, default=availability)

        filtered_df = df[
            (df["Charger Type"].isin(selected_types)) &
            (df["Availability"].isin(selected_availability))
        ]

        # Key Metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Stations", len(filtered_df))
        col2.metric("Avg Cost (USD/kWh)", round(filtered_df["Cost (USD/kWh)"].mean(), 2))
        col3.metric("Avg Usage (users/day)", round(filtered_df["Usage Stats (avg users/day)"].mean(), 1))
        col4.metric("Avg Rating", round(filtered_df["Reviews (Rating)"].mean(), 2))

        # Custom Colors
        custom_colors = {
            "AC Level 1": "#1f77b4",
            "AC Level 2": "#2ca02c",
            "DC Fast Charger": "#d62728",
            "Tesla Supercharger": "#9467bd"
        }

        # Stations by Charger Type
        st.subheader("🔋 Stations by Charger Type")
        charger_chart = filtered_df["Charger Type"].value_counts().reset_index()
        charger_chart.columns = ["Charger Type", "Count"]
        fig1 = px.bar(
            charger_chart,
            x="Charger Type",
            y="Count",
            color="Charger Type",
            color_discrete_map=custom_colors,
            title="Count of Stations by Charger Type"
        )
        st.plotly_chart(fig1, use_container_width=True)

        # Average Cost by Charger Type
        st.subheader("💰 Average Cost by Charger Type")
        cost_chart = filtered_df.groupby("Charger Type")["Cost (USD/kWh)"].mean().reset_index()
        fig2 = px.bar(
            cost_chart,
            x="Charger Type",
            y="Cost (USD/kWh)",
            color="Charger Type",
            color_discrete_map=custom_colors,
            title="Average Cost per kWh by Charger Type"
        )
        st.plotly_chart(fig2, use_container_width=True)

        # Ratings vs Usage
        st.subheader("⭐ Ratings vs Usage")
        fig3 = px.scatter(
            filtered_df,
            x="Usage Stats (avg users/day)",
            y="Reviews (Rating)",
            color="Charger Type",
            size="Charging Capacity (kW)",
            hover_name="Station Operator",
            color_discrete_map=custom_colors,
            title="Station Ratings vs Daily Usage"
        )
        st.plotly_chart(fig3, use_container_width=True)

        # Map of EV Charging Stations
        st.subheader("🗺️ Map of EV Charging Stations")
        if filtered_df["Latitude"].notna().any() and filtered_df["Longitude"].notna().any():
            avg_lat = filtered_df["Latitude"].mean()
            avg_lon = filtered_df["Longitude"].mean()
            m = folium.Map(location=[avg_lat, avg_lon], zoom_start=5, tiles="CartoDB positron")

            # Color map for charger types
            charger_types = filtered_df["Charger Type"].dropna().unique()
            colors = px.colors.qualitative.Dark24
            type_color_map = {ctype: colors[i % len(colors)] for i, ctype in enumerate(charger_types)}

            for _, row in filtered_df.iterrows():
                ctype = row.get("Charger Type", "Unknown")
                folium.CircleMarker(
                    location=[row["Latitude"], row["Longitude"]],
                    radius=6,
                    color=type_color_map.get(ctype, "blue"),
                    fill=True,
                    fill_color=type_color_map.get(ctype, "blue"),
                    fill_opacity=0.7,
                    popup=(
                        f"<b>Station:</b> {row.get('Station Operator','Unknown')}<br>"
                        f"<b>Charger Type:</b> {ctype}<br>"
                        f"<b>Cost:</b> {row.get('Cost (USD/kWh)','N/A')} USD/kWh<br>"
                        f"<b>Usage:</b> {row.get('Usage Stats (avg users/day)','N/A')} users/day"
                    )
                ).add_to(m)

            st_folium(m, width=700, height=500)
        else:
            st.warning("⚠️ No geolocation data available for the selected filters.")

# ------------------------------------
# ETL Status Tab
# ------------------------------------
with tab2:
    st.title("🔄 ETL Pipeline Status")

    # Show ETL Logs
    log_file = os.path.join("output", "etl_log.txt")
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            logs = f.read()
        st.text_area("ETL Logs", logs, height=400)
    else:
        st.warning("No ETL logs found. Run ETL pipeline first!")

    # Show last cleaned dataset stats
    cleaned_path = os.path.join("output", "cleaned_ev_data_spark")
    if os.path.exists(cleaned_path) and os.listdir(cleaned_path):
        df_cleaned = pd.read_parquet(cleaned_path)
        st.success("Cleaned data loaded successfully!")
        st.write(f"Rows: {len(df_cleaned)}, Columns: {len(df_cleaned.columns)}")
        st.dataframe(df_cleaned.head())
    else:
        st.info("Cleaned data not found yet.")
