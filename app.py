import streamlit as st
import pandas as pd
import pandas_gbq
import pandas 
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
st.set_page_config(page_title="SunPower Overview Dash",page_icon="🧑‍🚀",layout="wide")

def password_protection():
  if 'authenticated' not in st.session_state:
      st.session_state.authenticated = False
      
  if not st.session_state.authenticated:
      password = st.text_input("Enter Password:", type="password")
      correct_hashed_password = "Sunpower1234"
      
      if st.button("Login"):
          if password == correct_hashed_password:
              st.session_state.authenticated = True
              main_dashboard()
          else:
              st.error("Incorrect Password. Please try again or contact the administrator.")
  else:
      main_dashboard()

def main_dashboard():
  st.markdown("<h1 style='text-align: center;'>SunPower Creative Ad Testing</h1>", unsafe_allow_html=True)
  # Calculate the date one year ago from today
  one_year_ago = (datetime.now() - timedelta(days=365)).date()
  
  if 'full_data' not in st.session_state:
      credentials = service_account.Credentials.from_service_account_info(
          st.secrets["gcp_service_account"]
      )
      client = bigquery.Client(credentials=credentials)
      # Modify the query
      query = f"""
      SELECT * FROM `sunpower-375201.sunpower_segments.sunpower_platform_ad_level` 
      WHERE Date BETWEEN '{one_year_ago}' AND CURRENT_DATE() """
      st.session_state.full_data = pandas.read_gbq(query, credentials=credentials)

  data = st.session_state.full_data
  data = data[data['Ad_Set_Name__Facebook_Ads'] == 'T1-T3_Adults-25+1DC_Batch-26-Shelbi-Repurposed-Test-102423']

  selected_columns = ['Ad_Set_Name__Facebook_Ads', 'Ad_Name__Facebook_Ads', 'Impressions__Facebook_Ads', 'Link_Clicks__Facebook_Ads','Amount_Spent__Facebook_Ads', 'Lead_Submit_SunPower__Facebook_Ads']
  filtered_data = data[selected_columns]

  # Grouping the data by 'Ad_Name__Facebook_Ads'
  grouped_data = filtered_data.groupby(['Ad_Set_Name__Facebook_Ads', 'Ad_Name__Facebook_Ads'])
  
  # Summing up the numeric columns for each group
  aggregated_data = grouped_data.sum()
  
  # Display the aggregated data
  st.write(aggregated_data)

  #st.write(filtered_data)

if __name__ == '__main__':
    password_protection()

    
