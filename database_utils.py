import yaml
import psycopg2

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
                records = cur.fetchall()
        print(records)

        return records



ins = DatabaseConnector()
ins.init_db_engine()