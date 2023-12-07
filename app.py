import streamlit as st
import pandas as pd
import pandas_gbq
import pandas 
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
from scipy.stats import chi2_contingency
from PIL import Image
from git import Repo

Git_token = "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"

st.set_page_config(page_title="SunPower Creative Ad Testing Dash",page_icon="🧑‍🚀",layout="wide")

credentials = service_account.Credentials.from_service_account_info(
          st.secrets["gcp_service_account"]
      )
client = bigquery.Client(credentials=credentials)

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

def update_ad_set_table(new_ad_set_name):
    # Query to find the current Ad-Set
    query = """
    SELECT Ad_Set FROM `sunpower-375201.sunpower_streamlit.CreativeTestingStorage` WHERE Type = 'Current'
    """
    current_ad_set = pandas.read_gbq(query, credentials=credentials)

    # If current Ad-Set exists, update it to 'Past'
    if not current_ad_set.empty:
        update_query = """
        UPDATE `sunpower-375201.sunpower_streamlit.CreativeTestingStorage`
        SET Type = 'Past'
        WHERE Ad_Set = @current_ad_set
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("current_ad_set", "STRING", current_ad_set.iloc[0]['Ad_Set'])
            ]
        )
        client.query(update_query, job_config=job_config).result()

    # Insert the new Ad-Set with Type 'Current'
    insert_query = """
    INSERT INTO `sunpower-375201.sunpower_streamlit.CreativeTestingStorage` (Ad_Set, Type) VALUES (@new_ad_set, 'Current')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_ad_set", "STRING", new_ad_set_name)
        ]
    )
    client.query(insert_query, job_config=job_config).result()
    st.experimental_rerun()

def update_ad_set_if_exists(new_ad_set_name, full_data):
    if new_ad_set_name in full_data['Ad_Set_Name__Facebook_Ads'].values:
        update_ad_set_table(new_ad_set_name)  # Assuming this is the function you use to update BigQuery
    else:
        st.error("Ad_Set does not exist.")

def delete_ad_set(ad_set_value_to_delete, full_data):
        # SQL statement for deletion
        if ad_set_value_to_delete in full_data['Ad_Set_Name__Facebook_Ads'].values:
                  delete_query = """
                  DELETE FROM `sunpower-375201.sunpower_streamlit.CreativeTestingStorage`
                  WHERE Ad_Set = @ad_set_value
                  AND Type = 'Past'
                  """
                  # Configure query parameters
                  job_config = bigquery.QueryJobConfig(
                      query_parameters=[
                          bigquery.ScalarQueryParameter("ad_set_value", "STRING", ad_set_value_to_delete)
                      ]
                  )
                  # Execute the query
                  client.query(delete_query, job_config=job_config).result()
                  st.experimental_rerun()
        else:
                  st.error("Ad_Set does not exist")
          
### Code for past tests function ###
def process_ad_set_data(data, ad_set):
    # Filter data for the specific ad set

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
  
    ad_set_data = data[data['Ad_Set'] == ad_set]

    # Your data processing steps
    selected_columns = ['Ad_Set', 'Ad_Name', 'Impressions', 'Clicks', 'Cost', 'Leads']
    filtered_data = ad_set_data[selected_columns]
    grouped_data = filtered_data.groupby(['Ad_Set', 'Ad_Name']).sum()
    aggregated_data = grouped_data.reset_index()

    
    total = aggregated_data.sum(numeric_only=True)
    total['CPC'] = total['Cost']/total['Clicks']
    total['CPM'] = (total['Cost']/total['Impressions'])*1000
    total['CTR'] = total['Clicks']/total['Impressions']
    total['CVR'] = total['Leads']/total['Clicks']
    total['CPL'] = total['Cost']/total['Leads']
    total['Ad_Name'] = ""
    total['Ad_Set'] = 'Total'
  
    #Calculate cols
    aggregated_data['CPC'] = aggregated_data['Cost']/aggregated_data['Clicks']
    aggregated_data['CPM'] = (aggregated_data['Cost']/aggregated_data['Impressions'])*1000
    aggregated_data['CTR'] = aggregated_data['Clicks']/aggregated_data['Impressions']
    aggregated_data['CVR'] = aggregated_data['Leads']/aggregated_data['Clicks']
    aggregated_data['CPL'] = aggregated_data['Cost']/aggregated_data['Leads']

    #Sort leads so highest performer is at the top
    aggregated_data.sort_values(by='Leads', ascending=False, inplace=True)
  
    total_df = pd.DataFrame([total])
    # Reorder columns in total_df to match aggregated_data
    total_df = total_df[['Ad_Set', 'Ad_Name', 'Impressions', 'Clicks', 'Cost', 'Leads', 'CPL', 'CPC', 'CPM', 'CTR', 'CVR']]

    # Concatenate aggregated_data with total_df
    final_df = pd.concat([aggregated_data, total_df])

    # Initialize an empty list to store significance results
    significance_results = []
  
    # Top row data for comparison
    top_ad_leads = final_df.iloc[0]['Leads']
    top_ad_impressions = final_df.iloc[0]['Impressions']
  
    # Iterate through each row except the first and last
    for index, row in final_df.iloc[1:-1].iterrows():
        variant_leads = row['Leads']
        variant_impressions = row['Impressions']
  
        # Chi-square test
        chi2, p_value, _, _ = chi2_contingency([
            [top_ad_leads, top_ad_impressions - top_ad_leads],
            [variant_leads, variant_impressions - variant_leads]
        ])
  
        # Check if the result is significant and store the result
        significance_label = f"{p_value:.3f} - {'Significant' if p_value < 0.05 else 'Not significant'}"
        significance_results.append(significance_label)

    # Add a placeholder for the top row and append for the total row
    significance_results = [''] + significance_results + ['']
  
    # Add the significance results to the DataFrame
    final_df['Significance'] = significance_results

    column_order = ['Ad_Set', 'Ad_Name', 'Cost', 'Clicks', 'CPL', 'CPC', 'CPM', 'CTR', 'Leads', 'CVR', 'Significance']
    final_df = final_df[column_order]
  
    final_df.reset_index(drop=True, inplace=True)

    return final_df

# Function to create columns and display images with captions
def display_images(images, captions):
    num_images = len(images)
    cols = st.columns(num_images + 2)  # Extra columns for white space

    # Display images in the center columns
    for idx, image in enumerate(images):
        with cols[idx + 1]:  # +1 for offset due to initial white space
            st.image(image, caption=captions[idx])


def main_dashboard():
  st.markdown("<h1 style='text-align: center;'>SunPower Creative Ad Testing</h1>", unsafe_allow_html=True)
  st.markdown("<h2 style='text-align: center;'>Current Test</h2>", unsafe_allow_html=True)
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
  
  if 'current_test_data' not in st.session_state:
      credentials = service_account.Credentials.from_service_account_info(
          st.secrets["gcp_service_account"]
      )
      client = bigquery.Client(credentials=credentials)
      # Modify the query
      query = f"""
      SELECT * FROM `sunpower-375201.sunpower_streamlit.CreativeTestingStorage` 
      WHERE Type = 'Current'"""
      st.session_state.current_test_data = pandas.read_gbq(query, credentials=credentials)

  current_test_data = st.session_state.current_test_data

  if 'past_test_data' not in st.session_state:
      credentials = service_account.Credentials.from_service_account_info(
          st.secrets["gcp_service_account"]
      )
      client = bigquery.Client(credentials=credentials)
      # Modify the query
      query = f"""
      SELECT * FROM `sunpower-375201.sunpower_streamlit.CreativeTestingStorage` 
      WHERE Type = 'Past'"""
      st.session_state.past_test_data = pandas.read_gbq(query, credentials=credentials)

  past_test_data = st.session_state.past_test_data
  
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

  # Use this in your Streamlit input handling
  with st.expander('Update Current Test'):
            new_ad_set_name = st.text_input("Update Current Ad Test")
            st.write("*Note: Adding a new Ad Set will move the current test to past tests")
            st.write('Need to refresh the app to see updates')
            if st.button("Update Ad Set"):
                update_ad_set_if_exists(new_ad_set_name, st.session_state.full_data)

  current_Ad_Set = current_test_data['Ad_Set'].iloc[0]
  data = data[data['Ad_Set'] == current_Ad_Set]

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
  total['CPL'] = total['Cost']/total['Leads']
  total['Ad_Name'] = ""
  total['Ad_Set'] = 'Total'
  
  #Calculate cols
  aggregated_data['CPC'] = aggregated_data['Cost']/aggregated_data['Clicks']
  aggregated_data['CPM'] = (aggregated_data['Cost']/aggregated_data['Impressions'])*1000
  aggregated_data['CTR'] = aggregated_data['Clicks']/aggregated_data['Impressions']
  aggregated_data['CVR'] = aggregated_data['Leads']/aggregated_data['Clicks']
  aggregated_data['CPL'] = aggregated_data['Cost']/aggregated_data['Leads']

  #Sort leads so highest performer is at the top
  aggregated_data.sort_values(by='Leads', ascending=False, inplace=True)
  
  total_df = pd.DataFrame([total])
  # Reorder columns in total_df to match aggregated_data
  total_df = total_df[['Ad_Set', 'Ad_Name', 'Impressions', 'Clicks', 'Cost', 'Leads', 'CPL', 'CPC', 'CPM', 'CTR', 'CVR']]

  # Concatenate aggregated_data with total_df
  final_df = pd.concat([aggregated_data, total_df])

  # Initialize an empty list to store significance results
  significance_results = []
  
  # Top row data for comparison
  top_ad_leads = final_df.iloc[0]['Leads']
  top_ad_impressions = final_df.iloc[0]['Impressions']
  
  # Iterate through each row except the first and last
  for index, row in final_df.iloc[1:-1].iterrows():
      variant_leads = row['Leads']
      variant_impressions = row['Impressions']
  
      # Chi-square test
      chi2, p_value, _, _ = chi2_contingency([
          [top_ad_leads, top_ad_impressions - top_ad_leads],
          [variant_leads, variant_impressions - variant_leads]
      ])
  
      # Check if the result is significant and store the result
      significance_label = f"{p_value:.3f} - {'Significant' if p_value < 0.05 else 'Not significant'}"
      significance_results.append(significance_label)

  # Add a placeholder for the top row and append for the total row
  significance_results = [''] + significance_results + ['']
  
  # Add the significance results to the DataFrame
  final_df['Significance'] = significance_results

  column_order = ['Ad_Set', 'Ad_Name', 'Cost', 'Clicks', 'CPL', 'CPC', 'CPM', 'CTR', 'Leads', 'CVR', 'Significance']
  final_df = final_df[column_order]

  final_df.reset_index(drop=True, inplace=True)

  uploaded_images = []
  image_captions = []
          
  with st.expander("Upload Images"):        
            # Allow users to upload multiple images
            st.write("*Note... images will not be saved, refreshing or exiting the page will reset the display")
            uploaded_file = st.file_uploader("Select all images of tests at the same time (Shift+Select)", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)

            # Process each uploaded file
            for file in uploaded_file:
                if file is not None:
                    # Convert to PIL Image
                    image = Image.open(file)
                    uploaded_images.append(image)

                    # Get caption for each image
                    caption = st.text_input(f"Enter caption for image {len(uploaded_images)}", key=f"caption_{len(uploaded_images)}")
                    image_captions.append(caption)
          
  # Display the aggregated data
  st.dataframe(final_df, width=2000)

  # Display images with captions if both are provided
  if uploaded_images and all(image_captions):
            display_images(uploaded_images, image_captions)

  st.markdown("<h2 style='text-align: center;'>Past Tests</h2>", unsafe_allow_html=True)
  
  past_tests = past_test_data['Ad_Set']

  # Dictionary to store DataFrames for each ad set
  ad_set_dfs = {}

  #remove_ad_set = st.text_input("Enter Past Ad Set Name to remove")
  #if st.button("Remove Ad Set"):
  #        delete_ad_set(remove_ad_set, st.session_state.full_data)
      
  for ad_set in past_tests:
      ad_set_dfs[ad_set] = process_ad_set_data(st.session_state.full_data, ad_set)

  # Creating a dropdown for each ad set in past_tests
  for ad_set in past_tests:
      with st.expander(f"Show Data for {ad_set}"):
          st.dataframe(ad_set_dfs[ad_set], width=2000)

if __name__ == '__main__':
    password_protection()

    
