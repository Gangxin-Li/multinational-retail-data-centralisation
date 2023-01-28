import yaml
import psycopg2

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



ins = DatabaseConnector()
ins.init_db_engine()