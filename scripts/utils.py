import sqlalchemy.engine
from sqlalchemy import create_engine


def get_trino_conn() -> sqlalchemy.engine.Engine:
    """Create and return Trino connection"""

    TRINO_USER = 'admin'
    TRINO_PWD = ''
    TRINO_PORT = 8085
    CATALOG_NAME = 'hdfs'

    return create_engine(f'trino://{TRINO_USER}:{TRINO_PWD}@localhost:{TRINO_PORT}/{CATALOG_NAME}')