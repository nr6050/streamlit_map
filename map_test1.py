# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
import pandas as pd
import numpy as np
import json


st.set_page_config(
     page_title="Intage Store Master",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
     }
)

# Create Session object
def create_session():
    connection_parameters = {
         "account"   : st.secrets.account,
         "user"      : st.secrets.user,
         "password"  : st.secrets.password,
         "role"      : st.secrets.role,
         "warehouse" : st.secrets.warehouse,
         "database"  : st.secrets.database,
         "schema"    : st.secrets.schema
    }
    session = Session.builder.configs(connection_parameters).create()
    return session

# Add header and a subheader
st.header("Intage Store Master")
st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")

    
# Create Snowpark DataFrames that loads data from Intage Store Master
def load_data(session):
    snow_df_co1 = session.table("STORE").select(col("都道府県名"),col("都道府県コード")).distinct()
    snow_df_co1 = snow_df_co1.sort(col("都道府県コード"))
    snow_df_co1 = snow_df_co1.select(col("都道府県名"))
    pd_df_co1  = snow_df_co1.to_pandas()

    option = st.selectbox(
    '都道府県',
    pd_df_co1)
    

    snow_df_co2 = session.table("STORE").select(col("世界測地経度"), col("世界測地緯度")).filter((col("都道府県名") == option))
    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    pd_df_co2  = snow_df_co2.to_pandas()
    
    pd_df_co2  = pd_df_co2.rename(columns={'世界測地緯度': 'lat','世界測地経度': 'lon'})
    snow_df_co2 = snow_df_co2.with_column_renamed(col("世界測地緯度"), "lat")
    snow_df_co2 = snow_df_co2.with_column_renamed(col("世界測地経度"), "lon")

   
    df2 = pd.DataFrame(
    pd_df_co2,
    columns=['lat', 'lon'])
    st.map(df2)


    # Use columns to display the three dataframes side-by-side along with their headers
    #col1, col2, col3 = st.columns(3)
    #with st.container():
        #with col1:
            #st.subheader('CO2 Emissions by Country')
            #st.dataframe(pd_df_co2)


            
if __name__ == "__main__":
    session = create_session()
    load_data(session)    
