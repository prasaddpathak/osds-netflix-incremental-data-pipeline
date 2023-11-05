import pandas as pd
from sqlalchemy import create_engine


if __name__ == "__main__":

    TRINO_USER = 'admin'
    TRINO_PWD = ''
    TRINO_PORT = 8085
    CATALOG_NAME = 'hdfs'
    TABLE_NAME = 'clickstream_source'
    SCHEMA_NAME = 'osds'

    # Read data from CSV file
    data = pd.read_csv('./../data/vodclickstream_uk_movies.csv')
    data['event_date'] = pd.to_datetime(data['datetime']).dt.date

    print(f"Loading {len(data):,} records")

    conn = create_engine(f'trino://{TRINO_USER}:{TRINO_PWD}@localhost:{TRINO_PORT}/{CATALOG_NAME}')

    data.to_sql(name=TABLE_NAME,
                schema=SCHEMA_NAME,
                con=conn,
                index=False,
                chunksize=5_000,
                if_exists='replace',
                method='multi')

    print("Data loading complete")
