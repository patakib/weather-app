import duckdb
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

# Connect to DuckDB
DUCKDB_PATH = "data/warehouse/weather_dwh.duckdb"
con = duckdb.connect(DUCKDB_PATH, read_only=True)

# Load available cities
cities = (
    con.execute("""
    SELECT DISTINCT city
    FROM daily_data
    ORDER BY city
""")
    .fetchdf()["city"]
    .tolist()
)

# Create Dash app
app = Dash(__name__)

app.layout = html.Div(
    [
        html.H3("Daily Weather"),
        dcc.Dropdown(
            id="city-dropdown",
            options=[{"label": c, "value": c} for c in cities],
            value="Sopron",
            clearable=False,
        ),
        dcc.Graph(id="weather-graph-daily"),
        dcc.Graph(id="weather-table-daily"),
    ]
)


@app.callback(
    [Output("weather-graph-daily", "figure"), Output("weather-table-daily", "figure")],
    Input("city-dropdown", "value"),
)
def update_daily_data(city):
    df = con.execute(
        """
        SELECT
            time,
            temperature_2m_max,
            temperature_2m_min,
            precipitation_sum,
            precipitation_hours,
            precipitation_probability_max,
            snowfall_sum,
            sunrise,
            sunset,
            daylight_duration,
            sunshine_duration,
            uv_index_max,
            wind_speed_10m_max,
            wind_direction_10m_dominant,
            wind_gusts_10m_max
        FROM daily_data
        WHERE city = ?
        ORDER BY time
    """,
        [city],
    ).fetchdf()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["time"],
            y=df["temperature_2m_max"],
            name="Max Temp",
            mode="lines",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["time"],
            y=df["temperature_2m_min"],
            name="Min Temp",
            mode="lines",
        )
    )

    fig.add_trace(
        go.Bar(
            x=df["time"],
            y=df["precipitation_sum"],
            name="Precipitation",
            yaxis="y2",
            opacity=0.3,
        )
    )

    nrows = len(df)
    fill_colors = []
    for col in df.columns:
        # Conditional coloring only for specific columns
        if col == "temperature_2m_max":
            colors = [
                "red" if v > 30 else "orange" if v > 25 else "white" for v in df[col]
            ]

        elif col == "temperature_2m_min":
            colors = ["lightblue" if v < 0 else "white" for v in df[col]]

        elif col == "precipitation_sum":
            colors = [
                "blue" if v > 10 else "lightblue" if v > 0 else "white" for v in df[col]
            ]

        # All other columns → no coloring
        else:
            colors = ["white"] * nrows

        fill_colors.append(colors)

    table = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[
                        "Date",
                        "Max Temp (°C)",
                        "Min Temp (°C)",
                        "Precipitation (mm)",
                        "Precipitation Hours",
                        "Precipitation Probability Max (%)",
                        "Snowfall (mm)",
                        "Sunrise",
                        "Sunset",
                        "Daylight Duration (h)",
                        "Sunshine Duration (h)",
                        "UV Index Max",
                        "Wind Speed Max (km/h)",
                        "Wind Direction Dominant (°)",
                        "Wind Gusts Max (km/h)",
                    ],
                    fill_color="paleturquoise",
                    align="left",
                ),
                cells=dict(
                    values=[
                        df["time"],
                        df["temperature_2m_max"],
                        df["temperature_2m_min"],
                        df["precipitation_sum"],
                        df["precipitation_hours"],
                        df["precipitation_probability_max"],
                        df["snowfall_sum"],
                        df["sunrise"].dt.strftime("%H:%M"),
                        df["sunset"].dt.strftime("%H:%M"),
                        df["daylight_duration"] / 3600,  # Convert seconds to hours
                        df["sunshine_duration"] / 3600,  # Convert seconds to hours
                        df["uv_index_max"],
                        df["wind_speed_10m_max"],
                        df["wind_direction_10m_dominant"],
                        df["wind_gusts_10m_max"],
                    ],
                    fill_color=fill_colors,
                    align="left",
                ),
            )
        ]
    )

    fig.update_layout(
        title=f"Weather in {city}",
        xaxis_title="Date",
        yaxis_title="Temperature (°C)",
        yaxis2=dict(title="Precipitation (mm)", overlaying="y", side="right"),
        height=500,
    )
    table.update_layout(title=f"Daily Weather Data for {city}", height=400)
    return table, fig

    return fig


if __name__ == "__main__":
    app.run(debug=True)
