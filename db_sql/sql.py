import psycopg2
from psycopg2 import Error
from contextlib import closing
from datetime import datetime
import pandas as pd

class DbSql:
    def __init__(self, host, user, password, db, port="5432"):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db
        #conn_and_cursor = self.__connect_db()
        #self.connection = conn_and_cursor[0]
        #self.cursor = conn_and_cursor[1]

    def __connect_db(self):
        try:
            connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.db)
            return (connection, connection.cursor())
        except (Exception, Error) as error:
            print(f"Error connection to host: {self.host}, db: {self.db}")

    def sql_select(self, sql=None, params=None):
        con, cursor = self.__connect_db()
        try:
            with closing(cursor) as cur:
                if sql:
                    cur.execute(sql, params)
                else:
                    cur.execute('''select 1 as col;''')
                result = cur.fetchall()

        except Exception as e:
            status = False
            message = str(e)
            print(f"Status: {status}, !!!!!!!!!!!message: {message}!!!!!!!!!!!!")

        return result

    def sql_execute(self, sql):
        con, cursor = self.__connect_db()
        try:
            with closing(cursor) as cur:
                cur.execute(sql)
                # if cur.rowcount >= 0:
                # print("Rows affected: %s:", cur.rowcount)
                # list_obj = cur.fetchall()
        except Exception as e:
            message = str(e)
            print(f"!!!!!!!!!!!message: {message}!!!!!!!!!!")

    def get_pandas_df(self, sql, params=None, **kwargs):
        con, cursor = self.__connect_db()
        try:
            from pandas.io import sql as psql
        except ImportError:
            raise Exception(
                "pandas library not installed, run: pip install"
                "'apache-airflow-providers-common-sql[pandas]'."
            )
        try:
            with closing(cursor) as cur:
                return psql.read_sql(sql, con=con, params=params, **kwargs)
        except Exception as e:
            message = str(e)
            print(f"!!!!!!!!!!!message: {message}!!!!!!!!!!")


    def _generate_insert_sql(self, schema, table, values, target_fields, **kwargs):
        placeholders = [
                           "%s",
                       ] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ''

        sql = "INSERT INTO "
        if kwargs:
            if kwargs['primary_col']:
                sql += f"{schema}.{table} {target_fields} VALUES ({','.join(placeholders)}) on conflict({kwargs['primary_col']}) do nothing; commit;"
        else:
            sql += f"{schema}.{table} {target_fields} VALUES ({','.join(placeholders)}); commit;"

        return sql

    def insert_rows(self, schema, table, rows, target_fields, appnd_lst: list = None):
        con, cursor = self.__connect_db()
        with closing(cursor) as cur:
            for row in rows:
                lst = []
                if appnd_lst:
                    lst = appnd_lst.copy()
                for cell in row:
                    lst.append(self._serialize_cell(cell))
                values = tuple(lst)
                sql = self._generate_insert_sql(schema, table, values, target_fields)
                # print(f"values: {values}")
                # print("--------------")
                cur.execute(sql, values)

    def _bulk_state_insert_rows(self, schema, table, rows, target_fields, appnd_lst: list = None):
        all_value = []
        for row in rows:
            lst = []
            if appnd_lst:
                lst = appnd_lst.copy()
            for cell in row:
                lst.append(self._serialize_cell(cell))
            values = tuple(lst)
            sql_statement = self._generate_insert_sql(schema, table, values, target_fields)
            all_value.append(values)

        return sql_statement, all_value

    def bulk_insert_rowsbulk_insert_rows(self, schema, table, rows, target_fields, appnd_lst: list = None):
        sql, values = self._bulk_state_insert_rows

        with closing(self.cursor) as cur:
            cur.execute(sql, values)

    def _serialize_cell(self, cell):
        if cell is None or cell == 'nan':
            return None
        if isinstance(cell, int):
            return cell
        if isinstance(cell, float):
            if pd.isna(cell):
                return 0.0
            else:
                return cell
        if isinstance(cell, bool):
            return cell
        if isinstance(cell, datetime):
            return str(cell.isoformat())
        return str(cell).replace("'", '').replace("\\", '').replace("\"", '')

    def generate_create_table_sql(self, schema, table, columns: list):
        if columns:
            target_columns = '\n'.join(f"{str(col).replace('.', '_')} integer," for col in columns)
        else:
            target_columns = ''

        sql = f"CREATE TABLE {schema}.{table} ("
        for col in target_columns:
            sql += f"{col}"
        sql += f")"
        sql += f"\n"
        sql += f";"
        print(f"sql: {str(sql).replace(',)', ')')}")