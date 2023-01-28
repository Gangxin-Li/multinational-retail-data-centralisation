from data_extraction import DataExtractor
import pandas as pd
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





        # table = table.drop(index = null_values['index'])
        # null_values = table.loc[table['first_name'] == 'NULL']
        # print(null_values)




        # print(table.head(10))
        # print(type(table))
        # List null row
        # null_row = len(table[table.isnull().any(axis=1)])
        # print(f"There are NULL {null_row} rows")
        # if null_row != 0:
        #     print("Here are the NULL rows")
        #     print(table[table.isnull().any(axis=1)])
        # # Check date errors
        # table.dropna(subset=['date_of_birth'], inplace = True)

        # print(len(table))
        # table.dropna(subset=['date_of_birth'], inplace = True)
        # print(len(table))
        # table = table.drop([752,864])
        # print(table[860:870])
        # table['date_of_birth'] = pd.to_datetime(table['date_of_birth'])
        
        # pd.to_datetime(table['date_of_birth'], format='%Y-%M-%d', errors='raise')
#Test
isinstance = DataCleaning()
isinstance.clean_user_data()