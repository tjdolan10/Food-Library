#libraries
import os, time, sys, re
import pandas as pd
import numpy as np
from dotenv import load_dotenv

#local
from ... import models
from ...models import base
from . import session

def test_build():
    table_list = [
        models.Tables.Recipes
        ,models.Tables.Ingredients
        ,models.Tables.Measures
        ,models.Tables.RecipeIngredient
        ]
    engine = session.session_func('engine')

    with engine.connect() as conn:
        for x in table_list:
            x.__table__.create(conn, checkfirst=True)

    return


# def build_schema(engine,schema):
#     try:
#         engine.execute(f'DROP SCHEMA {schema} CASCADE;')
#     except:
#         print(f"""{schema} schema doesn't exist.""")
#     engine.execute(f"CREATE SCHEMA {schema};")
#     return


# def db_runSQL(scriptName):
#     fname = scriptName + '.pgsql'
#     os.system("cat {} | psql {}".format(os.path.join(os.getcwd(),'app','sql','scripts',fname),url))
#     return



def sdwan_build():
    schema = 'sdwan'
    path = os.path.join(os.getcwd(),'app','sql','sd_wan')
    table_list = [models.Providers,models.Partnerships,models.Contacts,models.Profiles]

    #establish DB connection
    state = session.get_enviro()
    db, engine = session.session_func(state)

    #drop all and build schema
    build_schema(engine,schema)


    #make sure web monitor table exists. build if not.
    try:
        web_df = session.query('SELECT * FROM public.web_monitor;')
    except:
        db_runSQL('web_db')


    for x in table_list:
        x.__table__.create(engine, checkfirst=True)
        db_logger.info('{}: created {}'.format(schema,x.__table__.name))

    #get data, push to db
    for x in table_list:
        print('processing ',x.__table__.name)
        file = x.__table__.name + '.tab'
        filepath = os.path.join(path,file)
        df = pd.read_csv(
            filepath
            ,sep='\t'
            ,lineterminator='\r'
            ,header=None
            ,names = x.__table__.columns.keys()
            ,low_memory=False)
        df.to_sql(x.__table__.name,engine,index=False,if_exists='append',method='multi',chunksize=500,schema=x.__table__.schema)

    #close connections
    db.close()
    return

# def gig_build():
#     #gig vars
#     schema = 'internet_search'
#     path = os.path.join(os.getcwd(),'app','sql','gig_sql_sync')
#
#     #inital models
#     table_list = [
#         models.Cities,
#         models.Countries,
#         models.Iisps,
#         models.MetroAreas,
#         models.Regions,
#         models.Subregions,
#         models.TempRouteStatistics]
#
#     #establish DB connection
#     state = session.get_enviro()
#     db, engine = session.session_func(state)
#
#     #drop and build schema
#     build_schema(engine,schema)
#
#     #make sure web monitor table exists. build if not.
#     try:
#         web_df = session.query('SELECT * FROM public.web_monitor;')
#     except:
#         db_runSQL('web_db')
#
#     #build tables
#     for x in table_list:
#         x.__table__.create(engine, checkfirst=True)
#         db_logger.info('{}: created {}'.format(schema,x.__table__.name))
#
#     #get data, push to db
#     for x in table_list:
#         db_logger.info('{}: processing {}'.format(schema,x.__table__.name))
#         #if/else to catch temp_routes
#         if x.__table__.name == 'temp_route_statistics':
#             file = 'routes.tab'
#         else:
#             file = x.__table__.name + '.tab'
#         filepath = os.path.join(path,file)
#
#         #read data from tab file
#         df = pd.read_csv(
#             filepath
#             ,sep='\t'
#             ,lineterminator='\r'
#             ,header=None
#             ,names = x.__table__.columns.keys()
#             ,low_memory=False)
#
#         #cleaning blanks
#         df = blank_check(df,gig_import_dict,file)
#         #gig specific cleaning:
#         if file == 'routes.tab':
#             df = clean_df(df,error_vals,audit_cols,fix_val)
#
#         #push to db
#         df.to_sql(x.__table__.name,engine,index=False,if_exists='append',method='multi',chunksize=500,schema=x.__table__.schema)
#         db_logger.info('{}: written {}'.format(schema,x.__table__.name))
#
#     #early 2021 bad data fix
#     db_logger.info('DELETING 2021 DATA')
#     print('DELETING 2021 DATA')
#     sql_str = """
#         DELETE
#         FROM internet_search.temp_route_statistics
#         WHERE
#             year = 2021;
#           """
#     session.write(sql_str)
#
#
#     #run transformation scripts
#     db_runSQL('gig_import')
#     db_runSQL('gig_process_summaries')
#
#     #close connections
#     db.close()
#     return
#
# def dcrs_build():
#     schema = 'dc'
#     path = os.path.join(os.getcwd(),'app','sql','dcrs_sql_sync')
#     id_drop_list = [
#         'dc_ix.tab','ix_building.tab','ix_iisps.tab',
#         'provider_presence.tab','site_data.tab']
#     table_list = [models.DcIx,
#         models.Ix,
#         models.IxBuilding,
#         models.IxIisps,
#         models.Operators,
#         models.Sites,
#         models.ProviderPresence,
#         models.SiteData]
#
#     #establish DB connection
#     state = session.get_enviro()
#     db, engine = session.session_func(state)
#
#     #drop all tables and rebuild schema
#     build_schema(engine,schema)
#
#     #check if web monitor table exists, build if not
#     try:
#         web_df = session.query('SELECT * FROM public.web_monitor;')
#     except:
#         db_runSQL('web_db')
#
#     #create tables
#     for x in table_list:
#         x.__table__.create(engine, checkfirst=True)
#         db_logger.info('{}: created {}'.format(schema,x.__table__.name))
#
#     #get data, push to db
#     for x in table_list:
#         print('processing ',x.__table__.name)
#         file = x.__table__.name + '.tab'
#         filepath = os.path.join(path,file)
#         columns = x.__table__.columns.keys()
#
#         #remove id column and create later
#         if file in id_drop_list:
#             columns.remove('id')
#
#         #site_data remove calc colums
#         if file == 'site_data.tab':
#             columns.remove('real_plus_est_floor_space_ft_sq')
#             columns.remove('real_plus_est_usable_floor_space_ft_sq')
#         #provider_presence remove column add later
#         if file == 'provider_presence.tab':
#             columns.remove('campus_id')
#         if file == 'operators.tab':
#             columns.remove('operator_name_group')
#
#         df = pd.read_csv(
#             filepath
#             ,sep='\t'
#             ,lineterminator='\r'
#             ,header=None
#             ,names = columns
#             ,low_memory=False)
#
#         #site cleaning
#         if file == 'sites.tab':
#             #drop site id duplicates
#             init_row_count = df.shape[0]
#             dup_list = df[df.duplicated(['site_id'])]['site_id'].tolist()
#             dup_df = df[df['site_id'].isin(dup_list)]
#             dups_to_drop = dup_df[(dup_df['name'].isnull()) | (dup_df['operator_id'].isnull())].index
#             df.drop(dups_to_drop,inplace=True)
#             print('sites.tab: rows dropped:',init_row_count-df.shape[0])
#
#             #clean text to numeric columns
#             df['cooling_capability'] = textToNumber(df['cooling_capability'])
#             df['kw_per_rack_cooling'] = textToNumber(df['kw_per_rack_cooling'])
#             df['pue'] = textToNumber(df['pue'])
#
#             #date column cleaning
#             df['operational_since_date'] = pd.to_datetime(df['operational_since_date'])
#
#         #site_data cleaning
#         if file == 'site_data.tab':
#             #remove comma+space,or single commas
#             df['gross_floor_space'] = df['gross_floor_space'].str.replace(', ','')
#             df['gross_floor_space'] = df['gross_floor_space'].str.replace(',','')
#             df['usable_floor_space'] = df['usable_floor_space'].str.replace(',','')
#             #clean text to numeric columns
#             df['gross_floor_space'] = textToNumber(df['gross_floor_space'])
#             df['usable_floor_space'] = textToNumber(df['usable_floor_space'])
#             df['total_wattage'] = textToNumber(df['total_wattage'])
#             #calculated columns
#             df['real_plus_est_floor_space_ft_sq'] = np.where(~df['est_floor_space_ft_sq'].isnull(),df['est_floor_space_ft_sq'],df['calc_floor_space_ft_sq'])
#             df['real_plus_est_usable_floor_space_ft_sq'] = np.where(~df['est_usable_floor_space_ft_sq'].isnull(),df['est_usable_floor_space_ft_sq'],df['calc_usable_floor_space_ft_sq'])
#
#         if file == 'provider_presence.tab':
#             #copy df to avoid overwrite errors
#             copy_df = df.copy()
#             #get site and campus id from sites
#             cross_walk = pd.DataFrame(db.query(models.Sites.site_id,models.Sites.campus_id).all())
#             #pull into provider table
#             df = pd.merge(copy_df,cross_walk, how = 'left',on=['site_id','site_id'])
#
#         if file == 'operators.tab':
#             df['operator_name_group'] = df['operator_id'].map(operator_dict).fillna(df['operator_name'])
#
#         #create sequentail ID column
#         if file in id_drop_list:
#             df.reset_index(inplace=True)
#             df.rename(columns={'index':'id'},inplace=True)
#
#         df.to_sql(x.__table__.name,engine,index=False,if_exists='append',method='multi',chunksize=500,schema=x.__table__.schema)
#
#     #run transformations
#     db_runSQL('dcrs_sqltransform')
#
#     #close connections
#     db.close()
#     return
#
