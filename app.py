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
import base64
import requests
import json
from google.cloud import storage

st.set_page_config(page_title="SunPower Creative Ad Testing Dash",page_icon="🧑‍🚀",layout="wide")

credentials = service_account.Credentials.from_service_account_info(
          st.secrets["gcp_service_account"]
      )
client = bigquery.Client(credentials=credentials)

def get_image(bucket_name, object_name):

    bucket_name = "creativetesting_images"
    image_data = get_image(bucket_name, object_name)  
    # Download the image data
    image_data = blob.download_as_bytes()
    return image_data

def initialize_storage_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    storage_client = storage.Client(credentials=credentials)
    return storage_client

# Use this client for GCS operations
storage_client = initialize_storage_client()


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


# Function to check ad set existence and update BigQuery
def update_ad_set_if_exists(new_ad_set_name, uploaded_images, full_data, bucket_name):
    ad_names = get_ad_names(new_ad_set_name, full_data)
    
    if len(uploaded_images) != len(ad_names):
        st.error(f"Please upload exactly {len(ad_names)} images for the ad names in this set.")
        return
    
    # Upload each file to GCS and update the ad set table
    for ad_name, uploaded_file in uploaded_images.items():
        destination_blob_name = f"{new_ad_set_name}/{ad_name}.jpg"  # Customize as needed
        upload_to_gcs(bucket_name, uploaded_file, destination_blob_name)
    
    update_ad_set_table(new_ad_set_name)  # Update the ad set table after successful uploads



def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    # Initialize the GCS client
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)

    # Create a new blob and upload the file's content.
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(source_file, content_type='image/jpeg')  # Set content_type as per your file type

    st.success(f"Uploaded file to {destination_blob_name} in bucket {bucket_name}")



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



def update_current_tests(new_ad_set_name, uploaded_files, full_data, bucket_name):
    ad_names = get_ad_names(new_ad_set_name, full_data)
    
    if len(uploaded_files) != len(ad_names):
        st.error(f"Please upload exactly {len(ad_names)} images for the ad names in this set.")
        return
    
    # Upload each file to GCS and update the ad set table
    for i, file in enumerate(uploaded_files):
        destination_blob_name = f"{new_ad_set_name}/{ad_names[i]}.jpg"  # Customize as needed
        upload_to_gcs(bucket_name, file, destination_blob_name)
    
    update_ad_set_table(new_ad_set_name)  # Update the ad set table after successful uploads



def get_ad_names(ad_set_name, ad_data):
    # Retrieve all ad names from the given ad set
    ad_names = ad_data[ad_data['Ad_Set_Name__Facebook_Ads'] == ad_set_name]['Ad_Name__Facebook_Ads'].tolist()
    ad_names = list(set(ad_names))

    # List of image paths
    image_paths = []

     # Iterate through each ad name and find corresponding images
    for ad_name in ad_names:
          image_name = f'{ad_name}.jpg'
          image_paths.append(image_name)
    return image_paths
    

# Function to create columns and display images with captions
def display_images(images, captions):
    num_images = len(images)
    cols = st.columns(num_images + 2)  # Extra columns for white space

    # Display images in the center columns
    for idx, image in enumerate(images):
        
        with cols[idx + 1]:  # +1 for offset due to initial white space
            st.image(image, caption=captions[idx], use_column_width=True)


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
#  with st.expander('Update Current Test'):
#            new_ad_set_name = st.text_input("Update Current Ad Test")
#            st.write("*Note: Adding a new Ad Set will move the current test to past tests")
#            st.write('Need to refresh the app to see updates')
#            if st.button("Update Ad Set"):
#                update_ad_set_if_exists(new_ad_set_name, st.session_state.full_data)



  # Streamlit interface for selecting new ad set
  with st.expander('Update Current Test and Upload Images'):
      new_ad_set_name = st.text_input("Enter New Ad Set Name")
      uploaded_images = {}

      if new_ad_set_name:
          # Retrieve ad names for the new ad set
          ad_names = get_ad_names(new_ad_set_name, st.session_state.full_data)

          # Display file uploaders for each ad name
          all_images_uploaded = True
          for ad_name in ad_names:
              uploaded_file = st.file_uploader(f"Upload image for {ad_name}", key=ad_name, type=['png', 'jpg', 'jpeg'])
              uploaded_images[ad_name] = uploaded_file

              # Check if image has been uploaded for each ad name
              if uploaded_file is None:
                  all_images_uploaded = False

          # If all images are uploaded, process the update
          if all_images_uploaded and st.button("Update Ad Set and Upload Images"):
              update_ad_set_if_exists(new_ad_set_name, uploaded_images, st.session_state.full_data, "your-bucket-name")
              st.success("Ad set updated and images uploaded successfully.")
          elif not all_images_uploaded:
              st.error("Please upload an image for each ad name.")


  current_Ad_Set = current_test_data['Ad_Set'].iloc[0]
  current_Ad_Set = current_Ad_Set.strip("'")
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


#  with st.expander("Upload Images"):        
#            # Allow users to upload multiple images
#            st.write("*Note... images will not be saved, refreshing or exiting the page will reset the display")
#            uploaded_file = st.file_uploader("Select all images of tests at the same time (Shift+Select)", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)
#
#            # Process each uploaded file
#            for file in uploaded_file:
#                if file is not None:
#                    # Convert to PIL Image
#                    image = Image.open(file)
#                    uploaded_images.append(image)
#
#                    # Get caption for each image
#                    caption = st.text_input(f"Enter caption for image {len(uploaded_images)}", key=f"caption_{len(uploaded_images)}")
#                    image_captions.append(caption)
#

          
  # Display the aggregated data
  st.dataframe(final_df, width=2000)
  #ct_images = final_df["Ad_Name"]
  #filtered_list = [item for item in ct_images if item is not None]
  final_adset = get_ad_names(final_df["Ad_Set"].iloc[0], st.session_state.full_data)
  display_images(final_adset, final_adset)        
          
  st.markdown("<h2 style='text-align: center;'>Past Tests</h2>", unsafe_allow_html=True)
  
  past_tests = past_test_data['Ad_Set']

  # Dictionary to store DataFrames for each ad set
  ad_set_dfs = {}

  #remove_ad_set = st.text_input("Enter Past Ad Set Name to remove")
  #if st.button("Remove Ad Set"):
  #        delete_ad_set(remove_ad_set, st.session_state.full_data)
      
  for ad_set in past_tests:
      ad_set_dfs[ad_set] = process_ad_set_data(st.session_state.full_data, ad_set)

  ad_set_dfs = ad_set_dfs[::-1]

  # Creating a dropdown for each ad set in past_tests
  for ad_set in past_tests:
      with st.expander(f"Show Data for {ad_set}"):
          st.dataframe(ad_set_dfs[ad_set], width=2000)
          ad_names = get_ad_names(ad_set, st.session_state.full_data)
          display_images(ad_names, ad_names)


if __name__ == '__main__':
    password_protection()

    
