from database_utils import DatabaseConnector
class DataExtractor:
    def list_db_tables():
        database =DatabaseConnector.init_db_engine()
