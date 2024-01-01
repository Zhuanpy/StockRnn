import pymysql
from root_ import file_root
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


def sql_password():
    path_ = file_root()
    path_ = f'{path_}/pp/sql.txt'
    f = open(path_, 'r')
    w = f.read()
    return w


def sql_cursor(database: str):
    w = sql_password()
    cur = pymysql.connect(host='localhost', user='root', password=w,
                          database=database, charset='utf8', autocommit=True)
    cursor = cur.cursor()
    return cursor


def execute_sql(database: str, sql: str):
    cursor = sql_cursor(database)
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data


def sql_data(database: str, sql: str):
    cursor = sql_cursor(database)
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data


def create_session(database: str):
    w = sql_password()
    en = f'mysql+pymysql://root:{w}@localhost:3306/{database}?charset=utf8'
    engine = create_engine(en)
    DbSession = sessionmaker(bind=engine)
    session = DbSession()
    return session


class MysqlAlchemy:

    @classmethod
    def pd_read(cls, database: str, table: str):
        w = sql_password()
        conn = pymysql.connect(host='localhost', user='root', passwd=w, db=database, port=3306, charset='utf8')
        sql = f'SELECT * FROM {database}.{table};'
        data = pd.read_sql(sql=sql, con=conn)  # 读取SQL数据库中数据;
        return data

    @classmethod
    def pd_append(cls, data, database: str, table: str):
        w = sql_password()
        conn = f'mysql+pymysql://root:{w}@localhost:3306/{database}?charset=utf8'
        data.to_sql(table, con=conn, if_exists='append', index=False, chunksize=None, dtype=None)

    @classmethod
    def pd_replace(cls, data, database: str, table: str):
        w = sql_password()
        conn = f'mysql+pymysql://root:{w}@localhost:3306/{database}?charset=utf8'
        data.to_sql(table, con=conn, if_exists='replace', index=False, chunksize=None, dtype=None)


if __name__ == '__main__':
    path = sql_password()
    print(path)
