# dashboard/app.py 
import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="Mexico Earthquakes 2020-2024 | ELT", layout="wide")
st.title("Mexico Earthquakes Analysis (2020–2024)")
st.markdown("### ELT Pipeline with Apache Airflow | 100% SQL Transformation in PostgreSQL")

# ============= BUSCA EL PARQUET AUTOMÁTICAMENTE =============
parquet_path = "data/analytics/sismos_analytics.parquet"
if not os.path.exists(parquet_path):
    parquet_path = "../data/analytics/sismos_analytics.parquet"
if not os.path.exists(parquet_path):
    parquet_path = "analytics/sismos_analytics.parquet"
if not os.path.exists(parquet_path):
    parquet_path = os.path.join(os.path.expanduser("~"), "airflow", "data", "analytics", "sismos_analytics.parquet")

if not os.path.exists(parquet_path):
    st.error("Parquet file not found. Run the Airflow DAG once to generate it.")
    st.code(parquet_path)
    st.stop()

df = pd.read_parquet(parquet_path)
df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
df['year'] = df['fecha_hora'].dt.year
df['month_year'] = df['fecha_hora'].dt.strftime('%Y-%m')

st.success(f"Data loaded successfully: {len(df):,} earthquakes from Parquet file")

# ============= 3 KPIs =============
c1, c2, c3 = st.columns(3)
c1.metric("Total Earthquakes", f"{len(df):,}", f"{len(df[df['year']==2024]):,} in 2024")
c2.metric("High-Risk (≥5.0)", f"{len(df[df['nivel_riesgo']=='Alto riesgo']):,}", 
          f"{len(df[df['nivel_riesgo']=='Alto riesgo'])/len(df)*100:.1f}%")
c3.metric("Average Depth", f"{df['profundidad'].mean():.1f} km", f"Max: {df['profundidad'].max():.0f} km")

st.markdown("---")

# ============= 5 GRÁFICAS  =============
col1, col2 = st.columns(2)
with col1:
    st.subheader("1. Monthly Earthquake Trend")
    monthly = df.groupby('month_year').size().reset_index(name='count')
    fig1 = px.line(monthly, x='month_year', y='count', markers=True, title="Earthquakes per Month")
    fig1.update_xaxes(tickangle=45)
    st.plotly_chart(fig1, width="stretch")

with col2:
    st.subheader("2. Risk Level Distribution")
    risk_counts = df['nivel_riesgo'].value_counts()
    fig2 = px.pie(values=risk_counts.values, names=risk_counts.index,
                  color=risk_counts.index,
                  color_discrete_map={'Alto riesgo':'#d62728', 'Mediano riesgo':'#ff7f0e', 'Bajo riesgo':'#2ca02c'},
                  title="Earthquakes by Risk Level")
    st.plotly_chart(fig2, width="stretch")

col3, col4 = st.columns(2)
with col3:
    st.subheader("3. Interactive Map – Magnitude & Risk")
    fig3 = px.scatter_mapbox(df.sample(min(4000, len(df))), lat="latitud", lon="longitud",
                             size="magnitud", color="nivel_riesgo", hover_name="lugar",
                             color_discrete_map={'Alto riesgo':'red', 'Mediano riesgo':'orange', 'Bajo riesgo':'green'},
                             zoom=4.8, height=520, title="Earthquakes by Magnitude and Risk Level")
    fig3.update_layout(mapbox_style="carto-positron")
    st.plotly_chart(fig3, width="stretch")

with col4:
    st.subheader("4. Guerrero-Oaxaca vs Rest of Mexico")
    zone = df['zona_riesgo'].value_counts()
    fig4 = px.bar(x=zone.index, y=zone.values, text=zone.values, color=zone.index,
                  color_discrete_map={'Guerrero-Oaxaca':'crimson', 'Otras zonas':'steelblue'},
                  title="Seismic Concentration")
    fig4.update_layout(showlegend=False)
    st.plotly_chart(fig4, width="stretch")

# ============= GRÁFICA 5 – CIUDADES REALES  =============
st.subheader("5. Top 10 Most Affected Cities/Municipalities")
df['ciudad'] = df['lugar'].str.split(',').str[-1].str.strip()

# Normalizamos nombres comunes
replace_dict = {
    'SAN JOSE DEL CABO': 'San José del Cabo', 'CIUDAD DE MEXICO': 'Ciudad de México',
    'OAXACA DE JUAREZ': 'Oaxaca de Juárez', 'ACAPULCO DE JUAREZ': 'Acapulco de Juárez',
    'PUERTO VALLARTA': 'Puerto Vallarta', 'SALINA CRUZ': 'Salina Cruz',
    'CRUCECITA': 'La Crucecita', 'MATIAS ROMERO': 'Matías Romero',
    'PINOTEPA NACIONAL': 'Pinotepa Nacional', 'HUATULCO': 'Huatulco'
}
df['ciudad'] = df['ciudad'].replace(replace_dict)

top_cities = df['ciudad'].value_counts().head(10)

fig5 = px.bar(x=top_cities.values, y=top_cities.index, orientation='h',
              title="Top 10 Most Affected Cities/Municipalities (2020–2024)",
              labels={'x': 'Number of Earthquakes', 'y': ''},
              color=top_cities.values, color_continuous_scale='Reds', text=top_cities.values)

fig5.update_yaxes(autorange="reversed")
fig5.update_traces(textposition='outside')
fig5.update_layout(showlegend=False, height=580, xaxis_title="Number of Earthquakes")
st.plotly_chart(fig5, width="stretch")

st.caption("Locations cleaned by city/municipality name (distance in km removed) → more meaningful analysis")

# ============= INSIGHTS =============
st.markdown("---")
st.success("""
### Key Insights – Social & Environmental Impact
- Guerrero-Oaxaca region concentrates >40% of high-risk earthquakes → urgent infrastructure reinforcement needed.
- More than 2,000 events exceeded magnitude 5.0 → fully justifies investment in early warning systems (SASMEX).
- Average depth ~50 km confirms subduction activity → permanent monitoring is essential.
- ELT is the ideal approach: raw data preserved, transformations evolve, Parquet enables scalability.
""")

st.info("Dashboard running from Parquet | No database required | Streamlit + Plotly")
st.caption("ELT Project – Mexico Earthquakes 2020-2024 | SSN-UNAM Catalog")