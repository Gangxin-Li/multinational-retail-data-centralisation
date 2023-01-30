from data_extraction import DataExtractor
import pandas as pd
from database_utils import DatabaseConnector
class DataCleaning():
    def __init__(self) -> None:
        pass
    def clean_user_data(self):
        table = DataExtractor().read_rds_table('legacy_users')
        print(table.tail(10))
        # Nan values
        nan_values = table[table.isnull().any(axis=1)]
        nan_num = len(nan_values)
        print(f"There are Nan {nan_num} rows")
        if nan_num != 0: 
            print("Here are the Nan rows")
            print(nan_values)


        # NULL values
        null_values = table[table['first_name'].str.contains("NULL")]
        null_num = len(null_values)
        print(f"There are NULL {null_num} rows")
        if null_num != 0: 
            print("Here are the NULL rows:")
            print(null_values)
        table = table.drop(null_values.index)
        
        #Date 
        # pd.to_datetime(table['join_date'], format='%Y-%M-%d', errors='raise')
        table['date_of_birth'] = pd.to_datetime(table['date_of_birth'], errors='coerce')
        table = table.dropna(subset=['date_of_birth']) 
        table['join_date'] = pd.to_datetime(table['join_date'], errors='coerce')
        table = table.dropna(subset=['join_date'])
        # Check datetime formate
        pd.to_datetime(table['join_date'], format='%Y-%M-%d', errors='raise')

        # Country
        print(table['country'].unique())

        print(table)
        # Upload
        upload = DatabaseConnector()
        upload.upload_to_db(table,'dim_users')
        return table

    def clean_card_data(self):

        file = DataExtractor().retreve_pdf_data()
        table = pd.DataFrame()
        for item in file:
            table = pd.concat([table, item],ignore_index=True)
        # Upload data
        print(table)
        upload = DatabaseConnector()
        upload.upload_to_db(table,'dim_card_details')
        return table



   
if __name__ == "__main__":
    isinstance = DataCleaning()
    # isinstance.clean_user_data()
    isinstance.clean_card_data()