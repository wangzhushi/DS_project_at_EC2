import pytz
import datetime
from project_utils import db_util
import csv as csv
import pandas
from project_utils.config import API_KEY_FINN
import finnhub
import time


def read_stock_list(file_name):
    stk_list = list()
    try:
        with open(file_name, 'r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            for row in csvreader:
                stk_list.append(row[0])
        return stk_list
    except Exception as error:
        print(error, "Can't open the stock list file")
        return None


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

def daily_etl(db_engine_i, interval_i, start_time_i, end_time_i):
    # stock_list = read_stock_list("../../postgresql/data/harry_sec_list_1000.csv")
    stock_list = read_stock_list("/home/ubuntu/DS_project/harry_sec_list_1000.csv")
    # stock_list = read_stock_list("harry_sec_list_1000.csv")
    # stock_list = ['AAPL']
    # engine = create_engine('sqlite://', echo=False)
    # engine = db_util.sqlalchemy_create_db_engine()
    engine = db_engine_i
    table = f"us_equity_{interval_i}_finnhub"
    res = 'D'
    if interval_i == 'daily':
        res = 'D'
    elif interval_i == '1m':
        res = '1'

    num_of_stock = 0

    for each_stock in stock_list:
        num_of_stock = num_of_stock + 1
        start = eastern_tz.localize(datetime.datetime.today())
        es = each_stock
        # es = 'AAPL'
        try:
            raw_df = download_data_from_finnhub(es, res, start_time_i, end_time_i)
            if raw_df is None:
                continue
        except Exception as error:
            print(error, es, "download_data_from_finnhub error")
            continue

        raw_df['symbol'] = es

        if interval == 'daily':
            raw_df['trade_date_int'] = [db_util.datetime_to_int_todate(datetime.datetime.fromtimestamp(x).date())
                                        for x in raw_df['t']]
        elif interval == '1m':
            raw_df['trade_date_int'] = [db_util.datetime_to_int_todate(datetime.datetime.fromtimestamp(x).date())
                                        for x in raw_df['t']]

        raw_df['t'] = pandas.to_datetime(raw_df['t'], unit='s')

        raw_df = raw_df.rename({'c': 'close_price', 'h': 'high_price', 'l': 'low_price', 'o': 'open_price',
                                's': 'return_status', 't': 'trade_time', 'v': 'volume'}, axis='columns')
#        raw_df = raw_df.reset_index()
#         print(raw_df.head())
#        raw_df.drop(columns='index')

#        raw_df.to_sql(table, engine, index=False, chunksize=None, if_exists='append', method="multi")
        try:
            raw_df.to_sql(table, engine, index=False, chunksize=None, if_exists='append', method="multi")
        except Exception as error:
            print(error, es, "couldn't write dataframe to Database")
            continue
        now = eastern_tz.localize(datetime.datetime.today())
        print('finish ' + each_stock + ' time: ' + str(now - start) + 'total time: ' + str(now - process_start_time)
              + 'total numbers: ' + str(num_of_stock) + ' Period: ' + str(step_period))
    return True


if __name__ == '__main__':
    # scope = input("please input running scope: ALL for everyday, One for one day, SP for specified day").upper()
    # resolution = input("please input running interval: D for daily, M for 1 minute").upper()
    scope = 'ALL'
    resolution = 'M'
    eastern_tz = pytz.timezone("US/Eastern")
    process_start_time = eastern_tz.localize(datetime.datetime.today())
    today = eastern_tz.localize(datetime.datetime.today()).date()
    tomorrow = today + datetime.timedelta(days=1)
    yesterday = today + datetime.timedelta(days=-1)
    interval = None
    base_time = datetime.datetime(1970, 1, 1, 00, 00, 00).date()
    init_mark_day = datetime.datetime(2000, 1, 1).date()
    # cut_off_day = datetime.datetime(2001, 1, 1).date()
    cut_off_day = today
    start_time = int((yesterday - base_time).total_seconds())
    end_time = int((today - base_time).total_seconds())

    if scope == 'ALL':
        start_time = int((init_mark_day - base_time).total_seconds())
        end_time = int((cut_off_day - base_time).total_seconds())
        if resolution == "D":
            interval = 'daily'
            psycopg2_connect = db_util.psycopg2_connect_to_db()
            sql_truncate_daily = "truncate table us_equity_daily_finnhub"
            db_util.psycopg2_db_dml(psycopg2_conn=psycopg2_connect, sql_dml=sql_truncate_daily)
        elif resolution == "M":
            interval = '1m'
            psycopg2_connect = db_util.psycopg2_connect_to_db()
            sql_truncate_daily = "truncate table us_equity_1m_finnhub"
            db_util.psycopg2_db_dml(psycopg2_conn=psycopg2_connect, sql_dml=sql_truncate_daily)
        else:
            print("don't support this resolution function yet")

        db_engine = db_util.sqlalchemy_create_db_engine()

        if interval == 'daily':
            etl = daily_etl(db_engine, interval, start_time, end_time)
        elif interval == '1m':
            init_mark_day = today + datetime.timedelta(days=-365)
            very_first_start_time_utc = db_util.dt_utc_start_end(init_mark_day)[2]
            very_final_end_time_utc = db_util.dt_utc_start_end(today)[3]
            start_time = very_first_start_time_utc
            next_stop_date = init_mark_day
            break_flag = 'N'
            step_period = 0
            while break_flag == 'N':
                step_period += 1
                next_stop_date = next_stop_date + datetime.timedelta(days=30)
                next_end_time_utc = db_util.dt_utc_start_end(next_stop_date)[3]
                if next_end_time_utc >= very_final_end_time_utc:
                    break_flag = 'Y'
                    next_end_time_utc = very_final_end_time_utc
                    end_time = next_end_time_utc
                else:
                    end_time = next_end_time_utc

                etl = daily_etl(db_engine, interval, start_time, end_time)

                if break_flag == 'Y':
                    break

                start_time = end_time + 1

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

    db_engine = db_util.sqlalchemy_create_db_engine()
    sql_select = "select * from us_equity_daily_finnhub where symbol= 'AAPL' and trade_date_int = 20000102"
    check_pd = db_util.sqlalchemy_db_query(sql_select=sql_select, db_engine=db_engine)
    print(check_pd.head())


