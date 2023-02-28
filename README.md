# Multinational Retail Data Centralisation

> AWS (S3, RDS), PostgreSQL

## Milestone 1

### Build utils to AWS and PostgreSQL

- Connect with AWS engine
  
```python
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
```

## Milestone 2

- Get original data sourece
- Read RDS table


```python
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
        print(table.head(10))
        return table

```

- Extract From S3

```python
def extract_from_s3(self,address):
        s3 = boto3.client('s3')
        bucket = 'data-handling-public'
        object = 'products.csv'
        file = 'products.csv'
        s3.download_file(bucket,object,file)
        table = pd.read_csv('./products.csv')
        return table
```

> Insert screenshot of what you have built working.

## Milestone 3

- Data Cleaning (Part)

```python
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
```

- Upload to PostgreSQL

```python

    def upload_to_db(self,DataFrame,dataname):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'gangxinli'
        DATABASE = 'postgres'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        # engine = engine.connect() 
        
        DataFrame.to_sql(dataname,engine,if_exists='append')
        
```

## Conclusions

- Build connections with AWS and PostgreSQL

- Download data from AWS (RDS, S3) return as pandas table

- EDA, data cleaning, detect outliers, treate missing values.

- upload the processed data to PostgreSQL

- Using SQL query to search specific results.