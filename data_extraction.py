from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import inspect
import requests
import pandas as pd
import tabula
import json
#Step 4
class DataExtractor:
    def list_db_tables(slef):
        database =DatabaseConnector().init_db_engine()
        # for table in database:
        #     print(table)
        return database
# Step 5
    def read_rds_table(self,table_name):
        engine_information = DatabaseConnector().read_db_creds()

        # RDS_HOST: data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com
        # RDS_PASSWORD: AiCore2022
        # RDS_USER: aicore_admin
        # RDS_DATABASE: postgres
        # RDS_PORT: 5432

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = engine_information['RDS_HOST']
        USER = engine_information['RDS_USER']
        PASSWORD = engine_information['RDS_PASSWORD']
        DATABASE = engine_information['RDS_DATABASE']
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine = engine.connect()
        inspector = inspect(engine)
        # names = inspector.get_table_names()
        # print(names)
        # engine.execute('''SELECT * FROM legacy_users''').fetchall()
        table = pd.read_sql_table(table_name,engine)
        # print(table.head(10))
        return table

    """
    import tabula

    # Read pdf into list of DataFrame
    dfs = tabula.read_pdf("test.pdf", pages='all')

    # Read remote pdf into list of DataFrame
    dfs2 = tabula.read_pdf("https://github.com/tabulapdf/tabula-java/raw/master/src/test/resources/technology/tabula/arabic.pdf")

    # convert PDF into CSV file
    tabula.convert_into("test.pdf", "output.csv", output_format="csv", pages='all')

    # convert all PDFs in a directory
    tabula.convert_into_by_batch("input_directory", output_format='csv', pages='all')
    """
    def retreve_pdf_data(self,address='card_details.pdf'):
        dfs = tabula.read_pdf(address,pages='all')
        # print(dfs)
        return dfs
    def list_number_of_stores(self,):
        dictionary ={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',headers=dictionary)
        # print(stores)
        # print(stores.text)
        number_stores = json.loads(stores.text)['number_stores']
        return number_stores
        # return json.load(stores.text)
    def retrieve_stores_data(self,endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'):
        table_dict = []
        store_number = self.list_number_of_stores()
        dictionary ={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        for num in range(store_number):
            if num%10 == 0:
                print(num,"/",store_number)
            table = requests.get(f'{endpoint}/{num}',headers=dictionary)
            content = json.loads(table.text)
            table_dict.append(content)
     
        table = pd.DataFrame.from_dict(table_dict)
        # print(table)
        return table
        # table = pd.DataFrame.from_dict(table.text)
        # print(table)

# Test
# instance = DataExtractor()
# instance.list_db_tables()
if __name__ == "__main__":
    isinstance = DataExtractor()
    # isinstance.read_rds_table('legacy_users')
    # isinstance.retreve_pdf_data()
    # isinstance.list_number_of_stores()
    isinstance.retrieve_stores_data()