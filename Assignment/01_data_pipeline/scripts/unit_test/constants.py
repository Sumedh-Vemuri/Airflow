# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = "/home/Airflow/Assignment/01_data_pipeline/scripts"
DB_FILE_NAME = "utils_output.db"
UNIT_TEST_DB_FILE_NAME = ''
DATA_DIRECTORY = "/home/Airflow/Assignment/01_data_pipeline/scripts/data"
INTERACTION_MAPPING = '/home/Airflow/Assignment/01_data_pipeline/scripts/interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag']
INDEX_COLUMNS_INFERENCE = [ 'city_tier', 'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'referred_lead','interaction_value' ]
NOT_FEATURES = ['created_date']




