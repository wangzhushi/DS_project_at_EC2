import psycopg2
import pandas as pd
import pandas.io.sql as pdsqlio
from python_py.project_utils.config import DB_CONFIG
from python_py.project_utils.config import API_KEY_FINN
import finnhub
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

    # database = ''.join(DB_CONFIG["DATABASE"])
    # user = os.getenv("USER")
    # password = os.getenv("PASSWORD")
    # host = os.getenv("HOST")
    # port = os.getenv("PORT")
#    url_eg = f'postgresql://user:{password}@{host}:{port}/{database}'
    url_eg = "postgresql://" + user + ":" + password + "@" + host + ":" + port + "/" + database
    print(url_eg)
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


#    engine = create_engine('postgresql://', echo=False)


# test db connection
# db = connect_to_db()
# engine = create_engine()
# cursor = db.cursor
# internal = "daily"
# sql = f"select * from us_equity_{internal}_finnhub"
# sql = "select * from sales"
# df = pdsqlio.read_sql_query(sql, db)
# print(df.head())


def download_data_from_finnhub(security_symbol, interval, start_time, end_time):
    api_key_finn = API_KEY_FINN["API_KEY_FINN"]
    #    finnhub_client = finnhub.Client(api_key="bvrpbjf48v6ol3okodkg")
    finnhub_client = finnhub.Client(api_key=api_key_finn)
    try:
        raw_data = finnhub_client.stock_candles(security_symbol, interval, start_time, end_time)
        raw_data_df = pd.DataFrame(data=raw_data)
        return raw_data_df
    except Exception as e:
        print(e, security_symbol, "api fetching failed")
        return None


# test download_data_from_finnhub
# df = download_data_from_finnhub('AAPL', 'D', 1590988249, 1591852249)
# print(df.head())

def datetime_to_int(dt):
    return int(dt.strftime("%Y%m%d%H%M%S"))


def datetime_to_int_todate(dt):
    return int(dt.strftime("%Y%m%d"))
