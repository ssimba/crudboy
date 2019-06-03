import os
import sys
import argparse
import pymysql

parser = argparse.ArgumentParser(description='Generate Insert or Update Sql script from connected database.')
parser.add_argument("-t", "--table", help="Table name which you want to generate sql")
parser.add_argument("-m", "--model", help="update or insert")

create_time_fields= ['created', 'create_time']
update_time_fields= ['modified', 'last_modify_time']


def connect_db():
    return pymysql.connect(host="localhost", port=3306, 
    user="user", password="password", database="instance")

def run_sql(sql):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows

def get_tables():
    sql_str = "SHOW TABLES;"
    return run_sql(sql_str)

def get_table_columns(table: str) -> list:
    sql_str = """SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'pm' 
                AND TABLE_NAME = '{0}';""".format(table)
    columns = run_sql(sql_str)
    return [col[0] for col in list(columns)]

def generate(tableName: str, columns: list, argModel: str):

    def insert_sql(tableName, columns):
        expr = lambda x: x != "id" and x not in update_time_fields
        sql = f"""INSERT INTO {tableName}({', '.join([col for col in columns if expr(col)])}) 
        VALUES({', '.join(['now()' if col in create_time_fields else '@'+col for col in columns if expr(col)])});"""

        output = os.path.join(os.getcwd(), "output", "{}-insert.txt".format(tableName))
        with open(output.format(tableName), 'w') as f:
            f.write(sql)
    
    def update_sql(tableName, columns):
        sql = f"""UPDATE {tableName} SET 
        {', '.join([str(col)+"=@"+str(col) for col in columns if col!="id"])} WHERE id=@id;"""

        sql = f"UPDATE {tableName} SET\r".format(tableName)
        for col in columns:
            if col == "id" or col in create_time_fields:
                continue
            tmp = f"{col}=@{col},\r"
            if col in update_time_fields:
                tmp = f"{col}=now(),\r"
            sql += tmp
        sql = sql.rstrip(",\r")+" \rWHERE id=@id;"

        output = os.path.join(os.getcwd(), "output", "{}-update.txt".format(tableName))
        with open(output.format(tableName), 'w') as f:
            f.write(sql)

    global create_time_fields
    global update_time_fields

    if argModel:
        if argModel.lower() == "insert":
            insert_sql(tableName, columns)
        elif argModel.lower() == "update":
            update_sql(tableName, columns)
    else:
        insert_sql(tableName, columns)
        update_sql(tableName, columns)

if __name__ == "__main__":
    args = parser.parse_args()

    tables = get_tables()
    for table in tables:
        table_name = table[0]
        if args.table != None and args.table != table_name:
            continue
        columns = get_table_columns(table_name)
        generate(table_name, columns, args.model)
