import pandas as pd
from utils import get_trino_conn


if __name__ == "__main__":

    TABLE_NAME = 'clickstream_source'
    SCHEMA_NAME = 'osds'

    # Read data from CSV file
    data = pd.read_csv('./../data/vodclickstream_uk_movies.csv')
    data['event_date'] = pd.to_datetime(data['datetime']).dt.date

    print(f"Loading {len(data):,} records")

    conn = get_trino_conn()

    data.to_sql(name=TABLE_NAME,
                schema=SCHEMA_NAME,
                con=conn,
                index=False,
                chunksize=5_000,
                if_exists='replace',
                method='multi')

    print("Data loading complete")
