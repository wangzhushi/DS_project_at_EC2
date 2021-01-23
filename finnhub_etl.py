import pytz
import datetime
from project_utils import db_util
import csv as csv
import pandas
from project_utils.config import API_KEY_FINN
import finnhub


def read_stock_list(file_name):
    stk_list = list()
    with open(file_name, 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        for row in csvreader:
            stk_list.append(row[0])
    return stk_list


def download_data_from_finnhub(security_symbol, interval, start_time, end_time):
    api_key_finn = API_KEY_FINN["API_KEY_FINN"]
    finnhub_client = finnhub.Client(api_key=api_key_finn)
    try:
        raw_data = finnhub_client.stock_candles(security_symbol, interval, start_time, end_time)
        raw_data_df = pandas.DataFrame(data=raw_data)
        return raw_data_df
    except Exception as error:
        print(error, security_symbol, "api fetching failed")
        return None


# test download_data_from_finnhub
# df = download_data_from_finnhub('AAPL', 'D', 1590988249, 1591852249)
# print(df.head())

def daily_etl(interval, start_time, end_time):
    # stock_list = read_stock_list("../../postgresql/data/harry_sec_list_1000.csv")
    stock_list = read_stock_list("harry_sec_list_1000.csv")
    # stock_list = ['AAPL', 'TSLA']
    # engine = create_engine('sqlite://', echo=False)
    engine = db_util.sqlalchemy_create_db_engine()
    table = f"us_equity_{interval}_finnhub"
    res = 'D'
    if interval == 'daily':
        res = 'D'
    elif interval == '1m':
        res = '1'

    for each_stock in stock_list:
        start = eastern_tz.localize(datetime.datetime.today())
        es = each_stock
        # es = 'AAPL'
        raw_df = download_data_from_finnhub(es, res, start_time, end_time)
        raw_df['symbol'] = es
        raw_df['trade_date_int'] = [db_util.datetime_to_int_todate(datetime.datetime.fromtimestamp(x).date())
                                    for x in raw_df['t']]
        raw_df['t'] = pandas.to_datetime(raw_df['t'], unit='s')

        raw_df = raw_df.rename({'c': 'close_price', 'h': 'high_price', 'l': 'low_price', 'o': 'open_price',
                                's': 'return_status', 't': 'trade_time', 'v': 'volume'}, axis='columns')
#        raw_df = raw_df.reset_index()
#         print(raw_df.head())
#        raw_df.drop(columns='index')

#        raw_df.to_sql(table, engine, index=False, chunksize=None, if_exists='append', method="multi")
        raw_df.to_sql(table, engine, index=False, chunksize=None, if_exists='append', method="multi")

        now = eastern_tz.localize(datetime.datetime.today())
        print('finish ' + each_stock + ' time: ' + str(now - start) + 'total time: '+ str(now - process_start_time) )

    return True


if __name__ == '__main__':
    # scope = input("please input running scope: ALL for everyday, One for one day, SP for specified day").upper()
    # resolution = input("please input running interval: D for daily, M for 1 minute").upper()
    scope = 'ALL'
    resolution = 'D'
    eastern_tz = pytz.timezone("US/Eastern")
    process_start_time = eastern_tz.localize(datetime.datetime.today())
    today = eastern_tz.localize(datetime.datetime.today()).date()
    tomorrow = today + datetime.timedelta(days=1)
    yesterday = today + datetime.timedelta(days=-1)
    interval = None
    base_time = datetime.datetime(1970, 1, 1).date()
    init_mark_day = datetime.datetime(2000, 1, 1).date()
    # cut_off_day = datetime.datetime(2001, 1, 1).date()
    cut_off_day = today
    start_time = int((yesterday - base_time).total_seconds())
    end_time = int((today - base_time).total_seconds())

    if scope == 'ALL':
        psycopg2_connect = db_util.psycopg2_connect_to_db()
        sql_truncate_daily = "truncate table us_equity_daily_finnhub"
        db_util.psycopg2_db_dml(psycopg2_conn=psycopg2_connect, sql_dml=sql_truncate_daily)

        start_time = int((init_mark_day - base_time).total_seconds())
        end_time = int((cut_off_day - base_time).total_seconds())
        if resolution == "D":
            interval = 'daily'
        elif resolution == "M":
            interval = '1m'
        else:
            print("don't support this resolution function yet")

    elif scope == 'ONE':
        start_time = int((yesterday - base_time).total_seconds())
        end_time = int((today - base_time).total_seconds())
        if resolution == "D":
            interval = 'daily'
        elif resolution == "M":
            interval = '1m'
        else:
            print("don't support this resolution function yet")

    elif scope == 'SP':
        print("don't support this scope function yet")
        # start_time =
        # end_time = today
        # etl = daily_etl(interval, start_time, end_time)
    else:
        print("wrong scope")

    if interval is not None:
        etl = daily_etl(interval, start_time, end_time)

    db_engine = db_util.sqlalchemy_create_db_engine()
    sql_select = "select * from us_equity_daily_finnhub where symbol= 'AAPL' and trade_date_int = 20000102"
    check_pd = db_util.sqlalchemy_db_query(sql_select=sql_select, db_engine=db_engine)
    print(check_pd.head())


