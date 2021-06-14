from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session
from urllib.parse import urlparse
import os, sys
import pandas as pd
from dotenv import load_dotenv
from . import db_logger
import logging



def connection_string_func(db_location,log=True):
    #connect to psql locally
    if db_location == 'local':
        if log:
            db_logger.info('local connection selected.')
        database = get_env_var('LOCAL_PG_DBNAME')
        #database = os.environ.get('LOCAL_PG_DBNAME','failure')
        if database == 'failure':
            db_logger.info('Failure to import env varabiles. Defaulting to hard coded connection in session.py')
            database = 'tg_infra'
            user = ""
            password = ""
            host = "localhost"
            port = "5432"
        else:
            if log:
                db_logger.info('importing env varabiles.')
            user = os.environ.get('LOCAL_PG_DBUSER','failure')
            password = os.environ.get('LOCAL_PG_DBPASS','failure')
            host = os.environ.get('LOCAL_PG_DBHOST','failure')
            port = os.environ.get('LOCAL_PG_DBPORT','failure')
        connection_string = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )

    #connect to heroku hosted psql
    if db_location == 'heroku':
        if log:
            db_logger.info('heroku connection selected.')
        #url = urlparse(os.environ.get('HEROKU_POSTGRESQL_WHITE_URL','failure'))
        url = urlparse(get_env_var('HEROKU_POSTGRESQL_WHITE_URL'))
        #if inital environment variable pull fails, try to redownload environment
        if url.path[:] == 'failure':
            load_dotenv(os.path.join(os.getcwd(), '.env'))
            url = urlparse(os.environ.get('HEROKU_POSTGRESQL_WHITE_URL','failure'))
            if url.path[:] == 'failure':
                db_logger.error('DB Connection Error')
            else:
                db_logger.error('DB Connection Success')
        database = url.path[1:]
        user = url.username
        password = url.password
        host = url.hostname
        port = url.port
        production = True
        connection_string = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )
        if log:
            db_logger.info('Success - {}'.format(db_location))
    return connection_string, database, user, password, host, port

def session_func(db,log=True):
    if log:
        db_logger.info('Connecting to PostgreSQL...')
    connection_string, database, user, password, host, port  = connection_string_func(db,log)
    #logging setup
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    engine = create_engine(connection_string, encoding="utf8", convert_unicode=True)
    session = sessionmaker()
    session.configure(bind=engine)
    db = session()
    return db, engine

def write(sql_string,log=True):
    state = get_enviro(log)

    db, engine = session_func(state,log)
    db.execute(sql_string)
    db.commit()
    #db.flush()
    db.close()
    engine.dispose()
    return

def query(sql_string,log=True):
    state = get_enviro(log)
    db, engine = session_func(state,log)
    result = pd.read_sql(sql_string, db.connection())
    db.close()
    engine.dispose()
    return result

def get_enviro(log=True):
    state = os.environ.get('ENVIRO', 'failure')
    if state == 'failure':
        #load env from app
        load_dotenv(os.path.join(os.getcwd(), '.env'))
        #try again, default to local if no environment loaded.
        state = os.environ.get('ENVIRO', 'local')
    return state

def get_env_var(var,log=True):
    output = os.environ.get(var, 'failure')
    if output == 'failure':
        #load env from app
        load_dotenv(os.path.join(os.getcwd(), '.env'))
        #try again, default to local if no environment loaded.
        output = os.environ.get(var, 'failure')
    return output
