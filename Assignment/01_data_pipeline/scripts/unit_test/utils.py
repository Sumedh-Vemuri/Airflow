##############################################################################
# Import necessary modules and files
# #############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error


###############################################################################
# Define the function to build database
# ##############################################################################

def build_dbs(DB_FILE_NAME,DB_PATH):
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    db_file = os.path.join(DB_PATH, DB_FILE_NAME)
    if os.path.exists(db_file):
        print('DB Already Exists')
        return 'DB Exists'

    # If the db file is not present, create it
    print('Creating Database')
    conn = sqlite3.connect(db_file)
    conn.close()

    print('New DB Created')
    return 'DB created'

###############################################################################
# Define function to load the csv file to the database
# ##############################################################################

def load_data_into_db(DB_FILE_NAME,DB_PATH, DATA_DIRECTORY):
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    # Read the CSV file into a pandas DataFrame
    csv_file = DATA_DIRECTORY + '/leadscoring.csv'
    df = pd.read_csv(csv_file)

    # Replace null values in 'total_leads_dropped' and 'referred_lead' columns with 0
    df['total_leads_droppped'].fillna(0, inplace=True)
    df['referred_lead'].fillna(0, inplace=True)

    # Create a connection to the SQLite database
    db_file = DB_PATH + '/' + DB_FILE_NAME
    conn = sqlite3.connect(db_file)

    # Save the DataFrame as a table in the database
    df.to_sql('loaded_data', conn, if_exists='replace', index=False)

    # Close the database connection
    conn.close()

###############################################################################
# Define function to map cities to their respective tiers
# ##############################################################################

    
def map_city_tier(DB_FILE_NAME, DB_PATH):
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.
    
    
    SAMPLE USAGE
        map_city_tier()

    '''
    # Create a connection to the SQLite database
    db_file = DB_PATH + '/' + DB_FILE_NAME
    conn = sqlite3.connect(db_file)
    
    from city_tier_mapping import city_tier_mapping as city
    # Read the existing data from the city_tier_mapping.py file
    city_tier_data = city

    # Read the existing data from the database into a pandas DataFrame
    query = "SELECT * FROM loaded_data"  # Assuming loaded_data table exists
    df = pd.read_sql(query, conn)

    # Map the cities to their respective tier
    df['city_tier'] = df['city_mapped'].map(city_tier_data)
    df['city_tier'].fillna(3.0, inplace=True)  # Fill missing tier with 3.0 (tier-3)

    # Save the mapped DataFrame as a table in the database
    df.to_sql('city_tier_mapped', conn, if_exists='replace', index=False)

    # Close the database connection
    conn.close()



###############################################################################
# Define function to map insignificant categorial variables to "others"
# ##############################################################################


def map_categorical_vars(DB_FILE_NAME, DB_PATH):
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
     # Create a connection to the SQLite database
    db_file = DB_PATH + '/' + DB_FILE_NAME
    conn = sqlite3.connect(db_file)
    
    from significant_categorical_level import list_platform, list_medium, list_source

    # Read the existing data from the database into a pandas DataFrame
    query = "SELECT * FROM city_tier_mapped"
    df = pd.read_sql(query, conn)

    # Map insignificant variables in 'first_platform_c'
    df.loc[~df['first_platform_c'].isin(list_platform), 'first_platform_c'] = 'Other'

    # Map insignificant variables in 'first_utm_medium_c'
    df.loc[~df['first_utm_medium_c'].isin(list_medium), 'first_utm_medium_c'] = 'Other'

    # Map insignificant variables in 'first_utm_source_c'
    df.loc[~df['first_utm_source_c'].isin(list_source), 'first_utm_source_c'] = 'Other'

    # Save the mapped DataFrame as a table in the database
    df.to_sql('categorical_variables_mapped', conn, if_exists='replace', index=False)

    # Close the database connection
    conn.close()


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
# #############################################################################
def interactions_mapping(DB_FILE_NAME, DB_PATH, INTERACTION_MAPPING, INDEX_COLUMNS_TRAINING, INDEX_COLUMNS_INFERENCE, NOT_FEATURES):
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    
   # Create a connection to the SQLite database
    db_file = DB_PATH + '/' + DB_FILE_NAME
    conn = sqlite3.connect(db_file)

    # Read the existing data from the database into a pandas DataFrame
    query = "SELECT * FROM categorical_variables_mapped"
    df = pd.read_sql(query, conn)
    
    # Read the interaction mappings from the CSV file
    interaction_df = pd.read_csv(INTERACTION_MAPPING)
    
#     # Perform interaction mapping
#     for i, row in interaction_df.iterrows():
#         interaction_cols = row['interaction_type'].split(' x ')
#         new_interaction_col = row['interaction_mapping']
        
#         # Create a new interaction column with mapped values
#         df[new_interaction_col] = df[interaction_cols].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
    
    df_unpivot = pd.melt(df, id_vars=['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag'], var_name='interaction_type', value_name='interaction_value')
    
    # handle the nulls in the interaction value column
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    
    # map interaction type column with the mapping file to get interaction mapping
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    
        
    # Drop the original interaction columns
    df.drop(interaction_df['interaction_type'], axis=1, inplace=True)

    df_pivot = df.pivot_table(
        values='interaction_value', index=['created_date', 'city_tier', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
       'referred_lead', 'app_complete_flag'], columns='interaction_mapping', aggfunc='sum')
    df = df_pivot.reset_index()
    
    # Drop the features that are not required
    df.drop(NOT_FEATURES, axis=1, inplace=True)

    # Save the mapped DataFrame as a table in the database
    df.to_sql('interactions_mapped', conn, if_exists='replace', index=False)

    # Save the model input DataFrame as a table in the database
    model_input = df.set_index(INDEX_COLUMNS_TRAINING if 'app_complete_flag' in df.columns else INDEX_COLUMNS_INFERENCE)
    model_input.to_sql('model_input', conn, if_exists='replace')
    
    # Close the database connection
    conn.close()
    
