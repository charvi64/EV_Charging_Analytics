import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_folium import st_folium
import folium

# ------------------------------------
# Page Config
# ------------------------------------
st.set_page_config(page_title="EV Charging Dashboard", layout="wide")

# ------------------------------------
# Load Data
# ------------------------------------
@st.cache_data
def load_data():
    return pd.read_parquet("output/cleaned_ev_data_spark")  # folder path

df = load_data()

# ------------------------------------
# Sidebar Filters
# ------------------------------------
st.sidebar.title("🔌 Filter Options")

charger_types = sorted(df["Charger Type"].dropna().unique().tolist())
selected_types = st.sidebar.multiselect("Select Charger Type(s):", charger_types, default=charger_types)

availability = sorted(df["Availability"].dropna().unique().tolist())
selected_availability = st.sidebar.multiselect("Select Availability:", availability, default=availability)

filtered_df = df[
    (df["Charger Type"].isin(selected_types)) &
    (df["Availability"].isin(selected_availability))
]

# ------------------------------------
# Dashboard Title
# ------------------------------------
st.title("⚡ EV Charging Stations Dashboard")
st.markdown("An interactive dashboard to explore EV charging station performance and insights.")

# ------------------------------------
# Key Metrics
# ------------------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Stations", len(filtered_df))
col2.metric("Avg Cost (USD/kWh)", round(filtered_df["Cost (USD/kWh)"].mean(), 2))
col3.metric("Avg Usage (users/day)", round(filtered_df["Usage Stats (avg users/day)"].mean(), 1))
col4.metric("Avg Rating", round(filtered_df["Reviews (Rating)"].mean(), 2))

# ------------------------------------
# Define Custom Colors
# ------------------------------------
custom_colors = {
    "AC Level 1": "#1f77b4",       # blue
    "AC Level 2": "#2ca02c",       # green
    "DC Fast Charger": "#d62728",  # red
    "Tesla Supercharger": "#9467bd" # purple (optional)
}

# ------------------------------------
# Plotly Charts (with consistent custom colors)
# ------------------------------------
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

# ------------------------------------
# Folium Map with Theme-Aware Legend
# ------------------------------------
st.subheader("🗺️ Map of EV Charging Stations")

if not filtered_df.empty and filtered_df["Latitude"].notna().any() and filtered_df["Longitude"].notna().any():
    avg_lat = filtered_df["Latitude"].mean()
    avg_lon = filtered_df["Longitude"].mean()

    # Clean map base
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=5, tiles="CartoDB positron")

    # Define colors for charger types
    charger_types = filtered_df["Charger Type"].dropna().unique()
    colors = px.colors.qualitative.Dark24  # distinct colors from Plotly
    type_color_map = {ctype: colors[i % len(colors)] for i, ctype in enumerate(charger_types)}

    # Add markers for each station
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

    # Detect Streamlit theme (dark or light)
    theme = st.get_option("theme.base")
    if theme == "dark":
        bg_color = "rgba(25, 25, 25, 0.9)"
        text_color = "#f2f2f2"
        border_color = "#999"
        dot_border = "#fff"
    else:
        bg_color = "rgba(255, 255, 255, 0.95)"
        text_color = "#000000"
        border_color = "#444"
        dot_border = "#000"

    # Add theme-aware legend
    legend_html = f"""
    <div style="
        position: fixed;
        bottom: 50px; left: 50px;
        width: 200px;
        background-color: {bg_color};
        border: 1px solid {border_color};
        border-radius: 8px;
        z-index: 9999;
        font-size: 14px;
        color: {text_color};
        padding: 10px 14px;
        box-shadow: 2px 2px 8px rgba(0,0,0,0.5);
    ">
    <b style="font-size:15px;">Charger Type Legend</b><br><br>
    """
    for ctype, color in type_color_map.items():
        legend_html += f"""
            <div style="margin-bottom:6px;">
                <i style='background:{color};
                          width:14px;
                          height:14px;
                          float:left;
                          margin-right:8px;
                          border-radius:50%;
                          border:1px solid {dot_border};'></i>
                <span style="color:{text_color};">{ctype}</span>
            </div>
        """
    legend_html += "</div>"

    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, width=700, height=500)

else:
    st.warning("⚠️ No data available for the selected filters. Please adjust your selections.")
