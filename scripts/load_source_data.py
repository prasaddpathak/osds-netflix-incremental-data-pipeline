import pandas as pd
from sqlalchemy import create_engine


if __name__ == "__main__":

    # Read data from CSV file
    # data = pd.read_csv('./../data/vodclickstream_uk_movies_mini.csv')
    data = pd.read_csv('./../data/vodclickstream_uk_movies.csv')
    print(data.dtypes)
    print(data)

    conn = create_engine('trino://admin:@localhost:8085/hdfs')

    data.to_sql(name='clickstream_source',
                schema='osds',
                con=conn,
                index=False,
                chunksize=7_000,
                if_exists='replace',
                method='multi')

    print("Loading complete")