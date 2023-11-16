import streamlit as st
import pandas as pd
import pandas_gbq
import pandas 
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
from scipy.stats import chi2_contingency

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
  # Renaming columns in a DataFrame
  data = data.rename(columns={
      'Campaign_Name__Facebook_Ads': 'Campaign',
      'Ad_Set_Name__Facebook_Ads': 'Ad_Set',
      'Ad_Name__Facebook_Ads' : 'Ad_Name',
      'Impressions__Facebook_Ads' : 'Impressions',
      'Link_Clicks__Facebook_Ads' : 'Clicks',
      'Amount_Spent__Facebook_Ads' : 'Cost',
      'Lead_Submit_SunPower__Facebook_Ads' : 'Leads',
      'Ad_Effective_Status__Facebook_Ads' : 'Ad_Status',
      'Ad_Preview_Shareable_Link__Facebook_Ads' : 'Ad_Link'
  })
  
  data = data[data['Ad_Set'] == 'T1-T3_Adults-25+1DC_Batch-26-Shelbi-Repurposed-Test-102423']

  selected_columns = ['Ad_Set', 'Ad_Name', 'Impressions', 'Clicks','Cost', 'Leads']
  filtered_data = data[selected_columns]

  # Grouping the data by 'Ad_Set'
  grouped_data = filtered_data.groupby(['Ad_Set', 'Ad_Name'])
  
  # Summing up the numeric columns for each group
  aggregated_data = grouped_data.sum()

  # Reset the index
  aggregated_data.reset_index(inplace=True)

  total = aggregated_data.sum(numeric_only=True)
  total['CPC'] = total['Cost']/total['Clicks']
  total['CPM'] = (total['Cost']/total['Impressions'])*1000
  total['CTR'] = total['Clicks']/total['Impressions']
  total['CVR'] = total['Leads']/total['Clicks']
  total['Ad_Name'] = ""
  total['Ad_Set'] = 'Total'
  
  #Calculate cols
  aggregated_data['CPC'] = aggregated_data['Cost']/aggregated_data['Clicks']
  aggregated_data['CPM'] = (aggregated_data['Cost']/aggregated_data['Impressions'])*1000
  aggregated_data['CTR'] = aggregated_data['Clicks']/aggregated_data['Impressions']
  aggregated_data['CVR'] = aggregated_data['Leads']/aggregated_data['Clicks']

  #Sort leads so highest performer is at the top
  aggregated_data.sort_values(by='Leads', ascending=False, inplace=True)
  
  total_df = pd.DataFrame([total])
  # Reorder columns in total_df to match aggregated_data
  total_df = total_df[['Ad_Set', 'Ad_Name', 'Impressions', 'Clicks', 'Cost', 'Leads', 'CPC', 'CPM', 'CTR', 'CVR']]

  # Concatenate aggregated_data with total_df
  final_df = pd.concat([aggregated_data, total_df])

  column_order = ['Ad_Set', 'Ad_Name', 'Cost', 'Clicks', 'CPC', 'CPM', 'CTR', 'Leads', 'CVR']
  final_df = final_df[column_order]

  final_df.reset_index(drop=True, inplace=True)
  
  # Display the aggregated data
  st.dataframe(final_df, width=2000)

  col1, col2, col3, col4 = st.columns(4)
  
  with col1:
    st.image('https://github.com/reporting-MR/CreativeTestingDash/blob/main/ShelbyWarrantyVariant.png?raw=true', caption = "Shelbi Warranty Variant")

  with col2:
    st.image('https://github.com/reporting-MR/CreativeTestingDash/blob/main/ShelbyTaxCredit.png?raw=true', caption = "Shelbi Tax Credit")

  with col3:
    st.image('https://github.com/reporting-MR/CreativeTestingDash/blob/main/ShelbyTaxCreditVariant.png?raw=true', caption = "Shelbi Tax Credit Variant")

  with col4:
    st.image('https://github.com/reporting-MR/CreativeTestingDash/blob/main/ShelbyWarranty.png?raw=true', caption = "Shelbi Warranty")

if __name__ == '__main__':
    password_protection()

    
