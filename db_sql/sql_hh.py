from contextlib import closing
from datetime import datetime
import pandas as pd

from db_sql.sql import DbSql

class DbSql_hh(DbSql):
    def __init__(self, host, user, password, db, port="5432"):
        super(DbSql_hh, self).__init__(host=host,
                                    user=user,
                                    password=password,
                                    db=db,
                                    port=port)

    def insert_rows(self, schema, table, rows, target_fields, appnd_lst: list = None):
        con, cursor = super.__connect_db()
        with closing(cursor) as cur:
            for row in rows:
                lst = []
                if appnd_lst:
                    lst = appnd_lst.copy()
                for cell in row:
                    lst.append(self._serialize_cell(cell))
                values = tuple(lst)
                sql = self._generate_insert_sql(schema, table, values, target_fields, primary_col="id")
                # print(f"values: {values}")
                # print("--------------")
                cur.execute(sql, values)

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
        if kwargs['primary_col']:
            sql += f"{schema}.{table} {target_fields} VALUES ({','.join(placeholders)}) on conflict({kwargs['primary_col']}) do nothing; commit;"
        else:
            sql += f"{schema}.{table} {target_fields} VALUES ({','.join(placeholders)}); commit;"

        return sql

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

    def _serialize_cell(self, cell):
        # print(cell)
        # print("----")
        new_cell = {}
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
        if isinstance(cell, list):
            if len(cell) != 0:
                return str(cell[0]).replace("'", '"')
            return ""
        if isinstance(cell, dict):
            for key, value in cell.items():
                if isinstance(value, str) or isinstance(value, bool):
                    new_cell[key] = self._clear_value(
                        str(value).replace("<highlighttext>", '').replace("</highlighttext>", '').replace("None",'0')).replace('\xad', '')
                else:
                    new_cell[key] = value
            return str(new_cell).replace("'", '"').replace("None", '0')
        return str(cell).replace("'", '').replace("\\", '').replace("\"", '').replace("None", "")