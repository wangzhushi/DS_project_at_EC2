import psycopg2
import pandas
from project_utils.config import DB_CONFIG
from sqlalchemy import create_engine


def psycopg2_connect_to_db():
    """
    initiate an db instance
    :return: db instance object
    """
    try:
        db_conn = psycopg2.connect(
            database=DB_CONFIG["DATABASE"],
            user=DB_CONFIG["USER"],
            password=DB_CONFIG["PASSWORD"],
            host=DB_CONFIG["HOST"],
            port=DB_CONFIG["PORT"]
        )
        return db_conn
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in psycopg2_connect_to_db: %s" % error)
        return 1


def sqlalchemy_create_db_engine():
    """
    initiate an pandas to_sql db engine
    :return: db engine object
    """
    database = DB_CONFIG["DATABASE"]
    user = DB_CONFIG["USER"]
    password = DB_CONFIG["PASSWORD"]
    host = DB_CONFIG["HOST"]
    port = DB_CONFIG["PORT"]

#    url_eg = f'postgresql://user:{password}@{host}:{port}/{database}'
#     url_eg = "postgresql://" + user + ":" + password + "@" + host + ":" + port + "/" + database
#     #    db_engine = create_engine('postgresql://scott:tiger@localhost/mydatabase')
#     db_engine = create_engine(url_eg, pool_size=10, max_overflow=20)
    try:
        db_engine = create_engine("postgresql://{user}:{pw}@{host}:{port}/{db}".format(
                                user=user, pw=password,
                                host=host, port=port, db=database))        
        return db_engine
    except Exception as e:
        print(e, "Error while creating sqlalchemy_create_db_engine")
        return 1

# test db connection
# db = psycopg2_connect_to_db()
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


def psycopg2_db_dml(psycopg2_conn=None, sql_dml=''):
    """
    to execute database manipulation SQL -- INSERT, UPDATE TRUNCATE and DELETE
    :return
    """
    db_cursor = psycopg2_conn.cursor()
    try:
        db_cursor.execute(sql_dml)
        psycopg2_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        # psycopg2_conn.rollback()
    finally:
        db_cursor.close()


def sqlalchemy_db_query(sql_select='', db_engine=None):
    """
    to execute database select query, use this because it can return a dataframe
    :return DataFrame of query results
    """
    try:
        return pandas.read_sql(sql_select, con=db_engine)
    except Exception as e:
        print(e, "Error while selecting in sqlalchemy_db_query")
        return 1

