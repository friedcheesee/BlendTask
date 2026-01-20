# Urban Mobility Analytics & Chatbot

Urban mobility analytics pipeline and chatbot over NYC yellow taxi data. Includes PySpark ETL jobs, SQL/CSV reports, a Streamlit data-assistant, and supporting notebooks.

## Links

Video links:

Streamlit UI (Chatbot) demo (Step 5):https://www.youtube.com/watch?v=2aoTewHA1Yg

All of the processes executed in order from data ingestion to running PySpark (Steps 1-4): https://youtu.be/1yryqaeI5WQ


## Repository Contents
- Notebooks: [1+2Piyush_Ingestion+KPI.ipynb](1+2Piyush_Ingestion+KPI.ipynb) (ingestion/KPIs), [3.0Piyush_SQL_Layer.ipynb](3.0Piyush_SQL_Layer.ipynb) (SQL layer exploration).
- [3.1_Execution_plan.txt](3.1_Execution_plan.txt): Spark logical/physical plan for the monthly revenue aggregation.
- [parquet/high_value_trips](parquet/high_value_trips), [parquet/monthly_revenue](parquet/monthly_revenue), [parquet/peak_congestion](parquet/peak_congestion): ETL parquet outputs (overwrite on each run).
- [4Piyush_PySpark.py](4Piyush_PySpark.py): PySpark ETL building monthly revenue, peak congestion, and high-value trip parquet outputs.
- [5_Piyush_Chatbot_streamlit.py](5_Piyush_Chatbot_streamlit.py): Streamlit chatbot that answers questions on the cleaned taxi dataset via SambaNova-hosted Llama 3.3 and LangChain.

- [SQLreports](SQLreports): SQL outputs in text format, exported from the notebook cells of the SQL layer notebook.
- [cleaned_dataset_link.txt](cleaned_dataset_link.txt): Gdrive link to cleaned dataset.
- [requirements.txt](requirements.txt): Python dependencies.

## Prerequisites
- Python 3.10+ (for Streamlit app and notebooks).
- Java 8+ and Spark installed and on PATH for running the PySpark job (`spark-submit`).
- SambaNova API key (for the chatbot) set as environment variable `SAMBANOVA_API_KEY`.

## Installation
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Data
- Place the cleaned dataset at `cleaned_yellow_tripdata.csv` in the repo root (or update paths below).
- Current scripts use absolute paths; for portability, change to repo-relative paths as noted in each section.

## Run the PySpark ETL
1) Open [4Piyush_PySpark.py](4Piyush_PySpark.py) and update the CSV read path and parquet output paths to this repo, e.g.:
   - Input: `cleaned_yellow_tripdata.csv`
   - Outputs: `parquet/monthly_revenue`, `parquet/peak_congestion`, `parquet/high_value_trips`
2) Submit the job:
```bash
spark-submit 4Piyush_PySpark.py
```
3) Results are written to the parquet folders above; the job prints a Spark execution plan to stdout.

## Run the Streamlit Chatbot
1) Ensure `SAMBANOVA_API_KEY` is set.
2) Update `CSV_FILE_PATH` at the top of [5_Piyush_Chatbot_streamlit.py](5_Piyush_Chatbot_streamlit.py) to a local path (e.g., `cleaned_yellow_tripdata.csv`).
3) Launch:
```bash
streamlit run 5_Piyush_Chatbot_streamlit.py
```
4) The app previews the data, then answers questions using a pandas-aware agent backed by SambaNova Llama 3.3.


