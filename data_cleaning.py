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

    def called_clean_store_data(self):
        store_data_instance = DataExtractor()
        table = store_data_instance.retrieve_stores_data()
        print(table)
        upload = DatabaseConnector()
        upload.upload_to_db(table,'dim_store_details')
    def convert_product_weights(self):
        def convert_unit(x):
            x = str(x)
            if 'kg' == x[-2:]:
                x = float(x[:-2]) * 1000
                return x
            elif 'g' == x[-1] and 'x' not in x:
                x = float(x[:-1])
                return x
            elif 'g' == x[-1] and 'x' in x:
                x = x[:-1].split('x')
                return float(x[0]) * float(x[1])
            else:
                return 0

        file = DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')
        # print(file)
        # file['weight(g)'] = file['weight'].apply(lambda x:float(x[:-2])*1000 if x[-2:]=='kg' else float(x[:-1]))
        # print(file['weight'].unique())
        file['weight(g)'] = file['weight'].apply(convert_unit)
        file['ml'] = file['weight'].apply(convert_unit)
        # print(file['weight(g)'].unique())
        return file
    def clean_products_data(self,table):
        # Clean the data
        upload = DatabaseConnector()
        upload.upload_to_db(table,'dim_products')
        return table

    def clean_orders_data(self):
        store_data_instance = DataExtractor()
        table =store_data_instance.read_rds_table('orders_table')
        # print(table.info())
        # print(table.columns())
        table = table.drop(columns=['first_name','last_name','1','level_0'])
        print(table.info())
        upload = DatabaseConnector()
        upload.upload_to_db(table,'orders_table')
    def clean_date_time(self):
        store_data_instance = DataExtractor()
        table =store_data_instance.extract_from_s3_json()
        # Clean the data 
        upload = DatabaseConnector()
        upload.upload_to_db(table,'dim_date_times')

if __name__ == "__main__":
    isinstance = DataCleaning()
    # isinstance.clean_user_data()
    # isinstance.clean_card_data()
    # isinstance.called_clean_store_data()
    # table = isinstance.convert_product_weights()
    # isinstance.clean_products_data(table)
    isinstance.clean_orders_data()
    # isinstance.clean_date_time()