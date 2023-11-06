
def copy_source_data(*args):
    """Copy date from source for the given event date"""

    import pandas as pd

    from sqlalchemy import create_engine

    SOURCE_TABLE = 'source'
    TARGET_TABLE = 'clicks'
    SCHEMA_NAME = 'osds'

    event_date = args[0]
    print(f"Running for {event_date}")

    conn = create_engine('trino://admin:@host.docker.internal:8085/hdfs')

    print("Dropping existing partition")

    DELETE_QUERY = f"DELETE FROM hdfs.{SCHEMA_NAME}.{TARGET_TABLE} WHERE event_date= DATE '{event_date}'"

    with conn.connect() as con:
        con.execute(DELETE_QUERY)


    print("Creating new partition")

    INSERT_QUERY = f"SELECT * FROM hdfs.{SCHEMA_NAME}.{SOURCE_TABLE} WHERE event_date=DATE '{event_date}'"

    df = pd.read_sql(sql=INSERT_QUERY, con=conn)

    print(f"Copying {len(df):} records")

    df.to_sql(name=TARGET_TABLE,
              schema=SCHEMA_NAME,
              con=conn,
              index=False,
              if_exists='append',
              method='multi')

    print('Partition write complete')

