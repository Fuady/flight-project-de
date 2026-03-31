"""
dashboard/app.py
────────────────────────────────────────────────────────────────────────────
Streamlit dashboard for the Flights Delay DE project.
Queries BigQuery mart tables directly and renders two tiles:

  Tile 1 – Average arrival delay by airline (bar chart, categorical)
  Tile 2 – Monthly delay trend with 3-month rolling average (line chart)

Run:
  streamlit run dashboard/app.py
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# ─── Config ──────────────────────────────────────────────────────────────────

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
MART_DATASET = "flights_mart"

st.set_page_config(
    page_title="US Flight Delays Dashboard",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── BQ Client ───────────────────────────────────────────────────────────────

@st.cache_resource
def get_bq_client():
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        credentials = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=3600)
def load_carrier_data() -> pd.DataFrame:
    client = get_bq_client()
    query = f"""
        SELECT
            carrier_code,
            carrier_name,
            total_flights,
            delayed_flights,
            delay_pct,
            avg_arr_delay_min,
            avg_carrier_delay,
            avg_weather_delay,
            avg_nas_delay,
            avg_late_aircraft_delay
        FROM `{PROJECT_ID}.{MART_DATASET}.rpt_delay_by_carrier`
        ORDER BY avg_arr_delay_min DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_trend_data() -> pd.DataFrame:
    client = get_bq_client()
    query = f"""
        SELECT
            year_month,
            month_start_date,
            total_flights,
            delayed_flights,
            delay_pct,
            avg_arr_delay_min,
            avg_carrier_delay,
            avg_weather_delay,
            avg_nas_delay,
            avg_late_aircraft_delay,
            avg_delay_3mo_rolling
        FROM `{PROJECT_ID}.{MART_DATASET}.rpt_delay_trend`
        ORDER BY month_start_date
    """
    return client.query(query).to_dataframe()


# ─── UI ───────────────────────────────────────────────────────────────────────

def render_header():
    st.markdown(
        """
        <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
                    padding: 2rem; border-radius: 12px; margin-bottom: 1.5rem;">
            <h1 style="color: white; margin: 0; font-size: 2rem;">✈️ US Flight Delays</h1>
            <p style="color: #a0aec0; margin: 0.5rem 0 0;">
                BTS On-Time Performance · Powered by GCS + Spark + BigQuery + dbt
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpis(carrier_df: pd.DataFrame, trend_df: pd.DataFrame):
    total_flights = carrier_df["total_flights"].sum()
    total_delayed = carrier_df["delayed_flights"].sum()
    overall_delay_pct = (total_delayed / total_flights * 100) if total_flights else 0
    avg_delay = carrier_df["avg_arr_delay_min"].mean()
    worst_carrier = carrier_df.iloc[0]["carrier_name"]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Flights",    f"{total_flights:,.0f}")
    col2.metric("Delayed Flights",  f"{total_delayed:,.0f}")
    col3.metric("Delay Rate",       f"{overall_delay_pct:.1f}%")
    col4.metric("Avg Arrival Delay",f"{avg_delay:.1f} min")


def render_tile1(df: pd.DataFrame):
    st.subheader("Tile 1 — Average Arrival Delay by Airline")
    st.caption("Categorical distribution: which carriers are worst offenders?")

    # Sidebar filters
    top_n = st.sidebar.slider("Top N airlines", min_value=5, max_value=len(df), value=10)
    plot_df = df.head(top_n).copy()

    # Color by delay severity
    plot_df["color"] = plot_df["avg_arr_delay_min"].apply(
        lambda x: "#e53e3e" if x > 20 else ("#ed8936" if x > 10 else "#48bb78")
    )

    fig = px.bar(
        plot_df,
        x="carrier_name",
        y="avg_arr_delay_min",
        text="avg_arr_delay_min",
        color="avg_arr_delay_min",
        color_continuous_scale=["#48bb78", "#ed8936", "#e53e3e"],
        labels={
            "carrier_name":      "Airline",
            "avg_arr_delay_min": "Avg Arrival Delay (min)",
        },
        title=f"Top {top_n} Airlines by Average Arrival Delay",
        hover_data={
            "delay_pct":          ":.1f",
            "total_flights":      ":,.0f",
            "avg_arr_delay_min":  ":.1f",
        },
    )
    fig.update_traces(texttemplate="%{text:.1f} min", textposition="outside")
    fig.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        xaxis_tickangle=-30,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=450,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Delay cause breakdown stacked bar
    st.markdown("**Delay cause breakdown (minutes)**")
    cause_cols = ["avg_carrier_delay", "avg_weather_delay",
                  "avg_nas_delay", "avg_late_aircraft_delay"]
    cause_labels = ["Carrier", "Weather", "NAS", "Late Aircraft"]

    melted = plot_df[["carrier_name"] + cause_cols].melt(
        id_vars="carrier_name",
        value_vars=cause_cols,
        var_name="cause",
        value_name="minutes",
    )
    melted["cause"] = melted["cause"].map(dict(zip(cause_cols, cause_labels)))

    fig2 = px.bar(
        melted,
        x="carrier_name",
        y="minutes",
        color="cause",
        barmode="stack",
        color_discrete_sequence=px.colors.qualitative.Set2,
        labels={"carrier_name": "Airline", "minutes": "Avg Delay (min)", "cause": "Cause"},
        title="Delay Cause Breakdown by Airline",
    )
    fig2.update_layout(
        xaxis_tickangle=-30,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=400,
    )
    st.plotly_chart(fig2, use_container_width=True)


def render_tile2(df: pd.DataFrame):
    st.subheader("Tile 2 — Monthly Delay Trend Over Time")
    st.caption("Temporal distribution: seasonal patterns and year-over-year changes")

    df["month_start_date"] = pd.to_datetime(df["month_start_date"])

    # Year filter
    years = sorted(df["month_start_date"].dt.year.unique().tolist())
    selected_years = st.sidebar.multiselect(
        "Filter years", options=years, default=years
    )
    filtered = df[df["month_start_date"].dt.year.isin(selected_years)]

    fig = go.Figure()

    # Bar: total flights (secondary axis)
    fig.add_trace(go.Bar(
        x=filtered["month_start_date"],
        y=filtered["total_flights"],
        name="Total Flights",
        marker_color="rgba(99,179,237,0.3)",
        yaxis="y2",
    ))

    # Line: avg delay
    fig.add_trace(go.Scatter(
        x=filtered["month_start_date"],
        y=filtered["avg_arr_delay_min"],
        name="Avg Arrival Delay",
        mode="lines+markers",
        line=dict(color="#e53e3e", width=2),
        marker=dict(size=5),
    ))

    # Line: 3-month rolling average
    fig.add_trace(go.Scatter(
        x=filtered["month_start_date"],
        y=filtered["avg_delay_3mo_rolling"],
        name="3-Month Rolling Avg",
        mode="lines",
        line=dict(color="#ed8936", width=2, dash="dash"),
    ))

    fig.update_layout(
        title="Monthly Average Arrival Delay (with 3-month rolling average)",
        xaxis=dict(title="Month"),
        yaxis=dict(title="Avg Delay (minutes)", side="left"),
        yaxis2=dict(
            title="Total Flights",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=480,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Cause breakdown over time
    st.markdown("**Monthly delay cause breakdown (stacked area)**")
    cause_cols = {
        "avg_carrier_delay":       "Carrier",
        "avg_weather_delay":       "Weather",
        "avg_nas_delay":           "NAS",
        "avg_late_aircraft_delay": "Late Aircraft",
    }
    fig3 = go.Figure()
    colors = ["#e53e3e", "#3182ce", "#38a169", "#d69e2e"]
    for (col, label), color in zip(cause_cols.items(), colors):
        fig3.add_trace(go.Scatter(
            x=filtered["month_start_date"],
            y=filtered[col],
            name=label,
            stackgroup="one",
            mode="none",
            fillcolor=color.replace("#", "rgba(").rstrip(")") + ",0.6)",
            line=dict(color=color),
        ))
    fig3.update_layout(
        xaxis=dict(title="Month"),
        yaxis=dict(title="Avg Delay (min)"),
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=380,
    )
    st.plotly_chart(fig3, use_container_width=True)


def render_data_tables(carrier_df, trend_df):
    with st.expander("View raw data tables"):
        tab1, tab2 = st.tabs(["By Carrier", "Monthly Trend"])
        with tab1:
            st.dataframe(carrier_df, use_container_width=True)
        with tab2:
            st.dataframe(trend_df, use_container_width=True)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    render_header()

    st.sidebar.title("Filters")
    st.sidebar.markdown("---")

    with st.spinner("Loading data from BigQuery…"):
        carrier_df = load_carrier_data()
        trend_df   = load_trend_data()

    if carrier_df.empty:
        st.error("No data found. Make sure the pipeline has run successfully.")
        return

    render_kpis(carrier_df, trend_df)
    st.markdown("---")

    col_l, col_r = st.columns([1, 1])
    with col_l:
        render_tile1(carrier_df)
    with col_r:
        render_tile2(trend_df)

    st.markdown("---")
    render_data_tables(carrier_df, trend_df)

    st.caption(
        "Data source: BTS On-Time Performance · "
        "Pipeline: Airflow → GCS → Spark → BigQuery → dbt"
    )


if __name__ == "__main__":
    main()
