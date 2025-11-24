
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc
import warnings
warnings.filterwarnings("ignore")

# Load clean data from Airflow ETL
df = pd.read_csv(
    "../data/processed/air_quality_clean.csv",
    dtype={"id": str, "sensor_id": str, "sensor_nombre": str, "municipio": str, "estado": str},
    parse_dates=["fecha"]
)

# KPIs
total_sensors = df["id"].nunique()
total_records = len(df)
avg_pm25 = df["pm25"].mean()
monterrey_pm25 = df[df["city"] == "Monterrey"]["pm25"].mean()
saltillo_pm25 = df[df["city"] == "Saltillo"]["pm25"].mean()
worst_hour = int(df.groupby("hour")["pm25"].mean().idxmax())

# Dash App
app = Dash(__name__)
app.title = "Air Quality Dashboard - Monterrey & Saltillo"

app.layout = html.Div(style={'backgroundColor': '#f4f6f9', 'padding': '30px', 'fontFamily': 'Arial'}, children=[
    html.H1("Air Quality Dashboard - Community Sensors (2023-2025)",
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 40}),

    # 3 KPIs
    html.Div([
        html.Div([
            html.H4("Average PM2.5", style={'margin': '0', 'color': '#34495e'}),
            html.H2(f"{avg_pm25:.1f} µg/m³",
                    style={'margin': '10px 0', 'color': '#e74c3c' if avg_pm25 > 35 else '#e67e22' if avg_pm25 > 15 else '#27ae60'})
        ], style={'background': 'white', 'padding': '25px', 'borderRadius': '12px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)', 'textAlign': 'center', 'flex': '1', 'margin': '10px'}),

        html.Div([
            html.H4("Active Sensors", style={'margin': '0', 'color': '#34495e'}),
            html.H2(f"{total_sensors:,}", style={'margin': '10px 0', 'color': '#3498db'})
        ], style={'background': 'white', 'padding': '25px', 'borderRadius': '12px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)', 'textAlign': 'center', 'flex': '1', 'margin': '10px'}),

        html.Div([
            html.H4("Worst Hour of Day", style={'margin': '0', 'color': '#34495e'}),
            html.H2(f"{worst_hour:02d}:00", style={'margin': '10px 0', 'color': '#9b59b6'})
        ], style={'background': 'white', 'padding': '25px', 'borderRadius': '12px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)', 'textAlign': 'center', 'flex': '1', 'margin': '10px'}),
    ], style={'display': 'flex', 'justifyContent': 'center', 'marginBottom': '50px'}),

    # 4 Charts
    html.Div([
        dcc.Graph(figure=px.line(
            df.groupby(["hour", "city"])["pm25"].mean().reset_index(),
            x="hour", y="pm25", color="city",
            title="Daily PM2.5 Pattern by City (Rush Hour Peaks)",
            labels={"pm25": "PM2.5 (µg/m³)", "hour": "Hour"},
            color_discrete_map={"Monterrey": "#e74c3c", "Saltillo": "#3498db"}
        ).update_layout(legend_title="City")),

        dcc.Graph(figure=px.bar(
            df.groupby("city")["pm25"].mean().reset_index(),
            x="city", y="pm25", color="city",
            title="Average PM2.5: Monterrey vs Saltillo",
            color_discrete_map={"Monterrey": "#e74c3c", "Saltillo": "#3498db"}
        )),
    ], style={'display': 'flex', 'justifyContent': 'space-around', 'flexWrap': 'wrap'}),

    html.Div([
        dcc.Graph(figure=px.box(
            df, x="city", y="pm25", color="city",
            title="PM2.5 Distribution by City"
        )),

        dcc.Graph(figure=px.scatter_mapbox(
            df.sample(2000), lat="sensor_lat", lon="sensor_lon",
            color="pm25", size="pm25", hover_name="sensor_nombre",
            color_continuous_scale="Reds", zoom=7, height=550,
            title="Heatmap of High-Pollution Sensors"
        ).update_layout(mapbox_style="carto-positron", margin={"r":0,"t":50,"l":0,"b":0}))
    ], style={'display': 'flex', 'justifyContent': 'space-around', 'flexWrap': 'wrap', 'marginTop': '40px'}),

    html.Footer("ETL by Apache Airflow | Data: Community PurpleAir Sensors | November 2025",
                style={'textAlign': 'center', 'marginTop': '60px', 'color': '#7f8c8d', 'fontSize': '14px'})
])

if __name__ == '__main__':
    print("Dashboard running → Open your browser: http://127.0.0.1:8050")
    app.run(debug=False, port=8050)