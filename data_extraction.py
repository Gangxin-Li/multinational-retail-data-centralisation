from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
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


# Test
# instance = DataExtractor()
# instance.list_db_tables()

isinstance = DataExtractor()
isinstance.read_rds_table('legacy_users')