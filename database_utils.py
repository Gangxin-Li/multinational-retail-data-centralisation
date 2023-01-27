import yaml
class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            data = yaml.safe_load(f)
        return data
    def init_db_engine(self):
        credentials = self.read_db_creds()
        return credentials