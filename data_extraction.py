from database_utils import DatabaseConnector
#Step 4
class DataExtractor:
    def list_db_tables(slef):
        database =DatabaseConnector().init_db_engine()
        for table in database:
            print(table)
    # def read_rds_table(self,):

instance = DataExtractor()
instance.list_db_tables()