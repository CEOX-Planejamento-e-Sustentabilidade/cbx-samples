import psycopg2

def connect_to_db(prod = False):
    if prod:
        conn = psycopg2.connect(
            host="plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com",
            database="cbx_prd",
            user="postgres",
            password="84iuPbpQnCF5vze"
        )
        return conn
    else:
        conn = psycopg2.connect(
            host="localhost",
            database="cbx_dev",
            user="postgres",
            password="local123"
        )
        return conn

def get_total_rows(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]

def insert_into_db(engine, df, table, schema='cbx', if_exis='append'):
    df.to_sql(table, engine, schema=schema, if_exists=if_exis, index=False)

def format_like(field, values_arr):
    likes = ""
    for val in values_arr:
        if val == "":
            continue
        if likes == "":
            likes += f" ({field} like '%{val.lower()}%' "
        else:
            likes += f" or {field} like '%{val.lower()}%' "
    return likes + ')' if likes != "" else ""

def format_in(field, values_arr):
    inn = ""
    for index, val in enumerate(values_arr):
        if val == "":
            continue
        if index == 0:
            inn += f" {field} in ({val}"
        else:
            inn += f",{val}"
    return inn + ')' if inn != "" else "" 
