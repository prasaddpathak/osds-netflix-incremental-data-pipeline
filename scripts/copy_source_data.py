from utils import get_trino_conn
import pandas as pd


def copy_source_data(event_date: str) -> None:
    """Copy date from source for the given event date"""

    TABLE_NAME = 'clickstream_clicks'
    SCHEMA_NAME = 'osds'

    conn = get_trino_conn()

    QUERY = f"SELECT * FROM hdfs.osds.clickstream_source WHERE event_date=DATE '{event_date}'"

    df = pd.read_sql(sql=QUERY, con=conn)

    print(len(df))
    print(df)

    # df.to_sql(name=TABLE_NAME,
    #             schema=SCHEMA_NAME,
    #             con=conn,
    #             index=False,
    #             if_exists='replace',
    #             method='multi',
    #             partition={'event_date': event_date})

    df.to_parquet(path='./../data/clickstream_source',
                  index=False,
                  partition_cols=['event_date'])

    print('Partition write complete')


if __name__ == "__main__":
    copy_source_data('2017-01-01')
