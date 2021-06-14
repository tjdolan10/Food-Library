from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session
from urllib.parse import urlparse
from dotenv import load_dotenv
import os, sys, logging
import pandas as p

def get_env_var(vars):
    #create a dictonary of environment variables from input list.
    environment = os.environ.get('ENVIRO', 'failure')
    if environment =='failure':
        load_dotenv(os.path.join(os.getcwd(), '.env'))
    var_dict = {}
    for var in vars:
        var_dict[var] = os.environ.get(var, 'failure')
    return var_dict

def connection_string_func():
    #return sqlalchemy connection string from .env variables
    con_vars = ['LOCAL_PG_DBNAME',
        'LOCAL_PG_DBUSER',
        'LOCAL_PG_DBPASS',
        'LOCAL_PG_DBHOST',
        'LOCAL_PG_DBPORT']
    con_dict = get_env_var(con_vars)
    connection_string = (f"postgresql+psycopg2://{con_dict['LOCAL_PG_DBUSER']}:{con_dict['LOCAL_PG_DBPASS']}@{con_dict['LOCAL_PG_DBHOST']}:{con_dict['LOCAL_PG_DBPORT']}/{con_dict['LOCAL_PG_DBNAME']}")
    return connection_string

def session_func(type):
    #return database session and engine
    connection_string = connection_string_func()
    #logging setup
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    #deprecated warning for encoding and convert_unicode: sqlAlchemy 1.4.18
    #engine = create_engine(connection_string, encoding="utf8", convert_unicode=True)
    engine = create_engine(connection_string)
    session = sessionmaker()
    session.configure(bind=engine)
    db = session()
    if type == 'session':
        return db
    if type == 'engine':
     return engine
     
     return

def write(sql_string):
    db, engine = session_func()
    db.execute(sql_string)
    db.commit()
    #db.flush()
    db.close()
    engine.dispose()
    return

def query(sql_string):
    db, engine = session_func()
    result = pd.read_sql(sql_string, db.connection())
    db.close()
    engine.dispose()
    return result
