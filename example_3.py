# This is my function library

from sqlalchemy import exc
import pandas as pd
import sqlalchemy
import boto3
import sys

def engine():
    return sqlalchemy.create_engine("postgresql://login:password@host.token.eu-north-1.rds.amazonaws.com/db_name")

def s3():
    return boto3.client('s3', aws_access_key_id='key', aws_secret_access_key='secret_key')

def response_processing(response, date_start, date_stop, name, category, s3):
    response.encoding = 'utf-8'
    if len(response.text) < 1000:
        sys.exit(f'ir: {response.text}')
        log_write(f'fail {response.text}', name, date_start, date_stop)
    s3.put_object(Body=response.text, Bucket='zif', Key=f'raw/{name}{category}_{date_start}_{date_stop}.txt')
    with open('raw.csv', 'w') as raw:
        raw.write(f'{response.text}\n')
    del response
    return pd.read_csv('raw.csv', dtype='category')

def send_raw_to_s3(raw, name, date_start, date_stop, s3):
    s3.put_object(Body=raw, Bucket='zif', Key=f'raw/{name}_{date_start}_{date_stop}.txt')
    
def log_write(result, name, date_start, date_stop):
    err_msg = f'{result}, {str(pd.Timestamp.now())[:19]}, {name}, {date_start}, {date_stop}'
    if sys.exc_info()[0] is not None:
        err_msg += f', {sys.exc_info()[:2]}'
    print(err_msg)
    with open('log', 'a+') as log:
        log.write(f'{err_msg}\n')
        
def log_write_add_column(name, col):
    msg = f'column {col} added to {name}'
    print(msg)
    with open('log', 'a+') as log:
        log.write(f'{msg}\n')

def add_column_to_sql(df_date, name, engine):
    df_sql = pd.read_sql(f"SELECT * from {name} limit 0", engine)
    for col in df_date.columns:
        if col not in df_sql.columns:
            engine.execute(f"ALTER TABLE {name} ADD {col} {dtype}")
            log_write_add_column(name, col)
    df_date.to_sql(name, engine, if_exists="append", index=False)
    
def rename_columns_for_sql(df):
    dd = {}
    for col in df.columns:
        dd.update([(col, col.lower().translate({ord(c): "_" for c in " !@#$%^&*()[]{};:,./<>?\|`~-=_+"}))])
    df = df.rename(columns=dd)
    return df
    
def remove_from_sql(name, date_start, date_stop, engine, date_col):
    date_range = "'" + "', '".join(list(map(lambda x: str(x)[:10], list(pd.date_range(start=date_start, end=date_stop))))) + "'"
    try:
        engine.execute(f"""DELETE FROM {name} WHERE {date_col} IN ({date_range})""")
    except sqlalchemy.exc.ProgrammingError:
        if f'(psycopg2.errors.UndefinedTable) relation "{name}" does not exist' in str(sys.exc_info()[1]):
            print(f'database for {name} will be created')
    
def df_to_sql(df, name, date_start, date_stop, engine, date_col):
    for date in pd.date_range(start=date_start, end=date_stop):
        print(str(date)[:10], str(pd.Timestamp.now())[:19])
        df_date = df[df[date_col] == str(date)[:10]]
        try:
            df_date.to_sql(name, engine, if_exists="append", index=False)
        except exc.SQLAlchemyError:
            if f'of relation "{name}" does not exist' in str(sys.exc_info()[:2]):
                add_column_to_sql(df_date, name, engine)
        df = df[df[date_col] != str(date)[:10]]
    log_write('success', name, date_start, date_stop)

def get_full_len(name, engine):
    try:
        full_len = engine.execute(f"""SELECT count(*) FROM {name}""").fetchall()[0][0]
    except sqlalchemy.exc.ProgrammingError:
        if f'(psycopg2.errors.UndefinedTable) relation "{name}" does not exist' in str(sys.exc_info()[1]):
            full_len = 0
    return full_len