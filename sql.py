import psycopg2
from contextlib import closing
from datetime import datetime
import pandas as pd
from decimal import Decimal


db_kwargs = {
    "database":"nhl",
    "user":"nhl_superuser",
    "password":"nhl_superuser",
    "host":"localhost",
    "port":"5432"
}

def connect_db():
    con = psycopg2.connect(
        database=db_kwargs['database'],
        user=db_kwargs['user'],
        password=db_kwargs['password'],
        host=db_kwargs['host'],
        port=db_kwargs['port']
    )
    #print("Database opened successfully")

    return con

def connect_db_test():
    con = connect_db()
    try:
        with closing(con.cursor()) as cur:
            cur.execute('''select 1 as col;''')
            if cur.fetchone():
                status = True
                message = "Connection successfully tested"
    except Exception as e:
        status = False
        message = str(e)

    print(f"Status: {status}, message: {message}")

def db_execute(sql):
    con = connect_db()

    try:
        with closing(con.cursor()) as cur:
            cur.execute(sql)
            #if cur.rowcount >= 0:
                #print("Rows affected: %s:", cur.rowcount)
                #list_obj = cur.fetchall()
    except Exception as e:
        message = str(e)
        print(f"message: {message}")

def db_exec_get_df(sql):
    con = connect_db()

    try:
        from pandas.io import sql as psql
    except ImportError:
        raise Exception(
            "pandas library not installed"
        )

    with closing(con.cursor()) as cur:
        return psql.read_sql(sql, con=con)

def _generate_insert_sql(schema, table, values, target_fields, **kwargs):
    placeholders = [
        "%s",
    ] * len(values)

    if target_fields:
        target_fields = ", ".join(target_fields)
        target_fields = f"({target_fields})"
    else:
        target_fields = ''

    sql = "INSERT INTO "
    sql += f"{schema}.{table} {target_fields} VALUES ({','.join(placeholders)}); commit;"
    return sql

def insert_rows(schema, table, rows, target_fields, appnd_lst:list=None):
    con = connect_db()
    with closing(con.cursor()) as cur:
        for row in rows:
            lst = []
            if appnd_lst:
                lst = appnd_lst.copy()
            for cell in row:
                lst.append(_serialize_cell(cell))
            values = tuple(lst)
            sql = _generate_insert_sql(schema, table, values, target_fields)
            cur.execute(sql, values)

def bulk_state_insert_rows(schema, table, rows, target_fields, appnd_lst:list=None):
    all_value = []
    for row in rows:
        lst = []
        if appnd_lst:
            lst = appnd_lst.copy()
        for cell in row:
            lst.append(_serialize_cell(cell))
        values = tuple(lst)
        sql_statement = _generate_insert_sql(schema, table, values, target_fields)
        all_value.append(values)

    return sql_statement, all_value

def bulk_insert_rowsbulk_insert_rows(schema, table, rows, target_fields, appnd_lst:list=None):
    sql, values = bulk_state_insert_rows
    con = connect_db()
    with closing(con.cursor()) as cur:
        cur.execute(sql, values)

def _serialize_cell(cell):
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

def generate_create_table_sql(schema, table, columns):

    target_columns = []
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


