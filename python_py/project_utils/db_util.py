import psycopg2
import pandas as pd
import pandas.io.sql as pdsqlio
from python_py.project_utils.config import DB_CONFIG
from python_py.project_utils.config import API_KEY_FINN
import finnhub


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

# test db connection
# db = connect_to_db()
# cursor = db.cursor
# sql = "select * from us_equity_daily_finnhub"
# sql = "select * from sales"
# df = pdsqlio.read_sql_query(sql, db)
# print(df.head())


def download_data_from_finnhub(security_symbol, interval, start_time, end_time):
    api_key_finn = API_KEY_FINN["API_KEY_FINN"]
#    finnhub_client = finnhub.Client(api_key="bvrpbjf48v6ol3okodkg")
    finnhub_client = finnhub.Client(api_key=api_key_finn)
    try:
        return finnhub_client.stock_candles(security_symbol, interval, start_time, end_time)
    except Exception as e:
        print(e, security_symbol, "api fetching failed")
        return None


raw_data = download_data_from_finnhub('AAPL', 'D', 1590988249, 1591852249)
df = pd.DataFrame(data=raw_data)
print(df.head())


