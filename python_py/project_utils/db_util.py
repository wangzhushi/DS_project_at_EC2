import psycopg2

from python_py.project_utils.config import DB_CONFIG
from sqlalchemy import create_engine
import os


def connect_to_db():
    """
    initiate an db instance
    :return: db instance object
    """
    db_conn = psycopg2.connect(
        database=DB_CONFIG["DATABASE"],
        user=DB_CONFIG["USER"],
        password=DB_CONFIG["PASSWORD"],
        host=DB_CONFIG["HOST"],
        port=DB_CONFIG["PORT"]
    )
    return db_conn


def create_db_engine():
    """
    initiate an pandas to_sql db engine
    :return: db engine object
    """
    database = ''.join(DB_CONFIG["DATABASE"])
    user = DB_CONFIG["USER"]
    password = DB_CONFIG["PASSWORD"]
    host = DB_CONFIG["HOST"]
    port = DB_CONFIG["PORT"]

#    url_eg = f'postgresql://user:{password}@{host}:{port}/{database}'
    url_eg = "postgresql://" + user + ":" + password + "@" + host + ":" + port + "/" + database
    # print(url_eg)
#    db_engine = create_engine('postgresql://scott:tiger@localhost/mydatabase')
    db_engine = create_engine(url_eg, pool_size=10, max_overflow=20)
#        driver='postgresql://',
#        username=DB_CONFIG["USER"],
#        password=DB_CONFIG["PASSWORD"],
#        host="@"+DB_CONFIG["HOST"],
#        port=DB_CONFIG["PORT"],
#        database=DB_CONFIG["DATABASE"]
#    )

    return db_engine


# test db connection
# db = connect_to_db()
# engine = create_engine()
# cursor = db.cursor
# internal = "daily"
# sql = f"select * from us_equity_{internal}_finnhub"
# sql = "select * from sales"
# df = pdsqlio.read_sql_query(sql, db)
# print(df.head())


def datetime_to_int(dt):
    return int(dt.strftime("%Y%m%d%H%M%S"))


def datetime_to_int_todate(dt):
    return int(dt.strftime("%Y%m%d"))
