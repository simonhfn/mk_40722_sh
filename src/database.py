from typing import Iterable, Dict, List, Any
import pandas as pd
import psycopg2
import project
import psycopg2.extras


class Database():
    pass


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SQLDatabase(Database, metaclass=Singleton):
    _db_conn = None

    def _get_conn(self):
        if SQLDatabase()._db_conn is None or SQLDatabase()._db_conn.closed > 0:
            SQLDatabase()._db_conn = get_local_connection()
        return SQLDatabase()._db_conn

    def _get_cur(self):
        return SQLDatabase()._get_conn().cursor()

    def insert(self, query, values) -> None:
        psycopg2.extras.execute_values(SQLDatabase()._get_cur(), query, values, template=None, page_size=len(values))

    def get(self):
        pass

    def get_as_df(self, sql, values=None) -> pd.DataFrame:
        return pd.read_sql_query(sql, SQLDatabase()._get_conn(), params=values)

    def commit(self) -> None:
        SQLDatabase()._get_conn().commit()


def get_local_connection():
    return psycopg2.connect(
        host=project.POSTGRES_HOST,
        port=project.POSTGRES_PORT,
        database=project.PROJECT_DB,
        user=project.POSTGRES_USER,
        password=project.POSTGRES_PASSWORD)


def add_countries(country2count: List[Dict[str, Any]], year: str, month: str) -> None:
    values = []
    for record in country2count:
        values.append((record['country'], record['count'], year, month,))
    query = """INSERT INTO Country (
                country,
                _count,
                _year,
                _month
            ) VALUES %s;"""
    SQLDatabase().insert(query, values)


def add_companies(companies2count: List[Dict[str, Any]], year: str, month: str) -> None:
    values = []
    for record in companies2count:
        values.append((record['company'], record['count'], year, month,))
    query = """INSERT INTO Companies (
                company,
                _count,
                _year,
                _month
            ) VALUES %s;"""
    SQLDatabase().insert(query, values)


def add_email(email2count: List[Dict[str, Any]], year: str, month: str) -> None:
    values = []
    for record in email2count:
        values.append((record['email'], record['count'], year, month,))
    query = """INSERT INTO UserBase (
                email,
                _count,
                _year,
                _month
            ) VALUES %s;"""
    SQLDatabase().insert(query, values)


def add_timestamp(time2count: List[Dict[str, Any]], year: str, month: str) -> None:
    values = []
    for record in time2count:
        values.append((record['timestamp'], record['count'], year, month,))
    query = """INSERT INTO DateEvent (
                _date,
                _count,
                _year,
                _month
            ) VALUES %s;"""
    SQLDatabase().insert(query, values)


def add_record_ingestion(year: str, month: str) -> None:
    values = [(year, month,)]
    query = """INSERT INTO RecordIngestion (
                _year,
                _month
            ) VALUES %s;"""
    SQLDatabase().insert(query, values)


def check_if_data_in_db(year, month) -> bool:
    query = """SELECT * FROM RecordIngestion WHERE _year=%(year)s AND _month=%(month)s;"""
    res = SQLDatabase().get_as_df(query, {"year": year, "month": month})
    if len(res) > 0:
        return True
    return False


def get_max_user_activity(limit=10) -> pd.DataFrame:
    query = """SELECT email,
              SUM(_count) AS total_count
            FROM userbase
            GROUP BY email
            ORDER BY SUM(_count) DESC
            LIMIT %(limit)s;"""

    return SQLDatabase().get_as_df(query, {"limit": limit})


def get_max_country_activity(limit=10) -> pd.DataFrame:
    query = """SELECT country,
              SUM(_count) AS total_count
            FROM Country
            GROUP BY country
            ORDER BY SUM(_count) DESC
            LIMIT %(limit)s;"""

    return SQLDatabase().get_as_df(query, {"limit": limit})


def get_max_companies_activity(limit=10) -> pd.DataFrame:
    query = """SELECT company,
              SUM(_count) AS total_count
            FROM Companies
            GROUP BY company
            ORDER BY SUM(_count) DESC
            LIMIT %(limit)s;"""

    return SQLDatabase().get_as_df(query, {"limit": limit})
