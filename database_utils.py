import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
#Step 4
class DatabaseConnector:
    def __init__(self):
        pass
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            data = yaml.safe_load(f)
        return data
    def init_db_engine(self):
        credentials = self.read_db_creds()
        with psycopg2.connect(host=credentials['RDS_HOST'], user=credentials['RDS_USER'], password=credentials['RDS_PASSWORD'], dbname=credentials['RDS_DATABASE'], port=credentials['RDS_PORT']) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'""")
                records = cur.fetchall()
                # for table in records:
                #     print(table)

        return records
    def upload_to_db(self,DataFrame):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'gangxinli'
        PASSWORD = 'gangxinli'
        DATABASE = 'postgres'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine = engine.connect()
        DataFrame.to_sql('dim_users',engine,if_exists='replace')
        

if __name__ == "__main__":
    ins = DatabaseConnector()
    ins.init_db_engine()