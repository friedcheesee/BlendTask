import streamlit as st
import pandas as pd
import os
from langchain_sambanova import ChatSambaNova
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_classic.agents import AgentType
import dotenv
dotenv.load_dotenv()


API_KEY =  os.getenv("SAMBANOVA_API_KEY")  
CSV_FILE_PATH = "D:\\Blend assignments\\Task\\cleaned_yellow_tripdata.csv"  

st.set_page_config(page_title="Urban Mobility Insights AI", layout="wide")
st.title("🚕 Urban Mobility Insights Assistant")

SAMBANOVA_MODEL = "Meta-Llama-3.3-70B-Instruct"

with st.sidebar:
    st.header("⚙️ Configuration")
    st.info(f"Using dataset: {CSV_FILE_PATH}")

    st.markdown("---")
    st.markdown("### 📝 Suggested Questions")
    st.code("What were the busiest pickup hours last month?")
    st.code("Which pickup locations generate the most revenue?")
    st.code("Why did revenue drop in February?")
    st.code("When is surge demand highest?")

SCHEMA_CONTEXT = """
DATA SCHEMA:
- vendor iD (int): Taxi provider identifier
- tpep_pickup_datetime (datetime): Trip pickup timestamp
- tpep_dropoff_datetime (datetime): Trip dropoff timestamp
- passenger_count (int): Number of passengers
- trip_distance (float): Trip distance in miles
- pickup_longitude (float): Pickup longitude
- pickup_latitude (float): Pickup latitude
- dropoff_longitude (float): Dropoff longitude
- dropoff_latitude (float): Dropoff latitude
- fare_amount (float): Metered fare
- tip_amount (float): Tip amount
- total_amount (float): Total charged amount
- pickup_hour (int): Hour of day (0–23)
- pickup_day_of_week (int): Day of week (0=Mon)
- pickup_month (int): Month number (1–12)
"""

ONE_SHOT_EXAMPLE = """
### ONE-SHOT EXAMPLE ###
User Question: "What was the total revenue in January?"

Thought:
1. Revenue comes from the 'total_amount' column.
2. January corresponds to pickup_month == 1.
3. Sum total_amount for those rows.

Python Code:
```python
january_df = df[df['pickup_month'] == 1]
total_revenue = january_df['total_amount'].sum()
print(total_revenue)
Final Answer:
Total revenue in January was $12,450,320.75.
"""

SYSTEM_PROMPT = f"""
You are a Senior Urban Mobility Data Analyst.
You have access to a pandas dataframe called 'df'.

{SCHEMA_CONTEXT}

{ONE_SHOT_EXAMPLE}

RULES:

Use only the dataframe provided.

Use pandas-based reasoning.

All revenue comes from 'total_amount'.

Ignore nulls in aggregations.

Do not hallucinate external data.

No plots or charts – text or tables only.

Provide concise, executive-level insights.
""" 

def main():
    try:
        #load data 
        df = pd.read_csv(CSV_FILE_PATH)

        #convert datetime columns
        df['tpep_pickup_datetime'] = pd.to_datetime(
            df['tpep_pickup_datetime'],
            errors='coerce'
        )
        df['tpep_dropoff_datetime'] = pd.to_datetime(
            df['tpep_dropoff_datetime'],
            errors='coerce'
        )

        st.subheader("📊 Data Preview")
        st.dataframe(df.head(5))

        
        llm = ChatSambaNova(
            model=SAMBANOVA_MODEL,
            sambanova_api_key=API_KEY,
            temperature=0
        )

        #create pandas dataframe agent
        agent = create_pandas_dataframe_agent(
            llm,
            df,
            verbose=True,
            allow_dangerous_code=True,
            agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
            prefix=SYSTEM_PROMPT,
            handle_parsing_errors=True
        )

        if "messages" not in st.session_state:
            st.session_state.messages = []

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if prompt := st.chat_input("Ask an urban mobility question..."):
            st.session_state.messages.append(
                {"role": "user", "content": prompt}
            )
            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                with st.spinner("Analyzing mobility data..."):
                    try:
                        response = agent.invoke(prompt)
                        output = response["output"]
                        st.markdown(output)
                        st.session_state.messages.append(
                            {"role": "assistant", "content": output}
                        )
                    except Exception as e:
                        st.error(f"Analysis failed: {str(e)}")

    except FileNotFoundError:
        st.error(f"❌ File not found: {CSV_FILE_PATH}")
        st.info("Please update the CSV_FILE_PATH variable at the top of the script.")
    except Exception as e:
        st.error(f"File processing error: {str(e)}")

if __name__ == "__main__":
    main()