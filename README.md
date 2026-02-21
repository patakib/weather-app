# Weather Data App

Credits to https://open-meteo.com for making high quality weather data available! Please checkout their page for further information. It is truly an amazing project.

## Purpose
I always felt I needed a "playground" to try out new ideas or technical solutions but I like well-rounded projects which has actually depth and meaning.
Especially in case of platform solutions (like k8s, Argo or similar) it is quite difficult to set up an environment with all the necessary data and services, in which one can REALLY try out and develop new things.
Although there are more and more solutions which make cloud native or I should say end-to-end, close-to-real-world development more accessible, there are still gaps in this area.
Therefore I decided to make this project to provide an *entry point* for fellow developers who are interested in *end-to-end data engineering projects and cloud native technologies*.

## Structure
This repository serves as a base of a multi-platform data project - this is the core of the platform, *focusing only on local development*, holding the actual logic:
- getting data from API
- storing raw data
- running data quality checks and storing data like in a data lake for further processing
- visualizing data
- data warehousing - data transformation and storage (local dwh)

## Design and Technical Decisions

### Architecture - Data Layers
There are several layers of this data pipeline, each of them are serving a special purpose.
- getting data from API and storing the raw data in JSON - always have the raw data as-is
- validate the data types, adding some columns and storing that in Parquet format - separate this from raw data for later debugging / recovery
- loading data into a data warehouse (in this case a local DuckDB instance) - we are adding the geographical information at this step
- make heavier transformations, dimension tables and create metrics in DuckDB - data warehousing step
- Dashboard for daily weather variables to have an visual, easy-to-digest overview of the weather forecast for the next days

### Tests
For testing I use Pytest. This is quite standard tool with a lot of capabilities.

### Data Validation
Even if the Weather API is a high quality project, data validation remains important.  
First I tried to involve spatial libraries sooner to maintain geographic information. 
I wanted to use pyarrow, geoarrow and geoparquet for this particular use case but I found the documentation not so straightforward and that is always a liability in a project.  
Also reading and validating JSON data in pyarrow is much more complicated than in Polars - therefore I decided to use the latter, store the data as Parquet without dedicated geographical metadata, 
because when loading into DuckDB, we can add geographical information - which I have done.

## Install
There are multiple ways to install the service.

1. Ansible:
- clone this repo: ```https://github.com/patakib/weather-platform``` and run the necessary steps defined to bootstrap this local project.

2. uv:
- clone this repo
- ```cd weather-app```
- ```uv sync```
- then you can run any scripts with ```uv run src/...```

## Tests
```uv run pytest {SELECTED_TEST_FILES}```

## Further Plans
This structure is planned to be used with slightly different services for cloud native deployment scenarios.
