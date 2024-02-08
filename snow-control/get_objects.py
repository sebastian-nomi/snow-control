from queries import * 
from styling import time_func
from sf_object_structures import * 
from load import * 
import snowflake.connector as snowcon
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor
import json 
from time import localtime,strftime
from control_state import ControlState
from itertools import repeat

@time_func
def object_scan(state:ControlState, method = 'conc') -> dict:
    objects = {}
    conn = state.connection
    tp_executor = state.executor
    def individual_object_scan(item: Tuple[str,list[str]]):
        obj_type, key = item
        cur = conn.cursor()
        formatted_query = (INTEGRATION_SHOW_QUERY if obj_type.upper().endswith('INTEGRATION') else SHOW_QUERY).format(obj_type)
        cur.execute(formatted_query)
        qid = cur.sfqid
        panda = cur.execute(
            NAME_QUERY.format(
                qid = qid, 
                key = ','.join([f'"{s}"' for s in key])
            )
        ).fetch_pandas_all()
        if obj_type.upper() in ('PROCEDURE','FUNCTION'):
            panda = panda[panda['is_builtin'] == 'N']
        panda['FULL_NAME'] = panda['FULL_NAME'].apply(
            lambda x: process_name(x.replace(' RETURN ',':'), obj_type.upper())
        )
        return (obj_type,panda)

    if method == 'seq':
        for obj_type,full_name_columns in GET_FULL_NAME.items():
            print(obj_type)
            print(full_name_columns)
            _, result_df = individual_object_scan((obj_type,full_name_columns))
            objects[obj_type] = result_df
    else: 
        results = tp_executor.map(individual_object_scan,GET_FULL_NAME.items())
        for obj_type,result_df in results:
            objects[obj_type] = result_df

    return objects

def filter_objects(state:ControlState, objects:dict[str,pd.DataFrame], method:str) -> dict[str,pd.DataFrame]:
    dbs = objects['database']
    # Shared Databases
    objects['shared database'] = dbs[dbs['kind']== 'IMPORTED DATABASE'].copy()
    # Application DBs
    objects['application database'] = dbs[dbs['kind'] == 'APPLICATION'].copy()
    shared_dbs = set(objects['shared database']['name'])
    application_dbs = set(objects['application database']['name'])
    ignore_dbs = shared_dbs | application_dbs

    # Special Consideration Stage
    objects['internal stage'] = objects['stage'][objects['stage']['type']=='INTERNAL'].copy()
    objects['external stage'] = objects['stage'][objects['stage']['type']=='EXTERNAL'].copy()

    # Special Consideration: Information Schema Views
    objects['view'] = objects['view'][objects['view']['schema_name']!='INFORMATION_SCHEMA'].copy()
    # Special Consideration: View
    objects['materialized view'] = objects['view'][objects['view']['is_materialized']=='true'].copy()
    objects['view'] = objects['view'][objects['view']['is_materialized']=='false'].copy()


    # Special Consideration: xtab
    objects['external table'] = objects['table'][objects['table']['is_external']=='Y'].copy()
    objects['table'] = objects['table'][objects['table']['is_external']=='N'].copy()

    # Special Consideration: Objects where db/container is a shared/app db
    if method == 'seq':
        return dict(
            filter_function(obj_typ,object_df,ignore_dbs)
            for obj_typ, object_df in objects.items() 
        )
    else:
        return { 
            sf_type:df 
            for sf_type,df in  
            state.executor.map(
                filter_function,
                *zip(*objects.items()),repeat(ignore_dbs)
            )
        }


def filter_function(obj_type:str, obj_df:pd.DataFrame, ignore_dbs:set, ignore_pattern =  r'.*_(DEV|QA|PROD)_[0-9]{1,5}') -> Tuple[str,pd.DataFrame]:
    identifier = 'database_name'
    if obj_type.lower() in FNCs:
        identifier = 'catalog_name'
    elif obj_type.lower() == 'database':
        identifier = 'name'
    elif obj_type.lower() in ALOs or obj_type.lower() in ('shared database','application database'):
        identifier = None
    
    if identifier:
        filter = lambda db: db not in ignore_dbs and not re.match(ignore_pattern,db)
        return obj_type,obj_df[obj_df[identifier].apply(filter)]
    return obj_type,obj_df
    

def save_cache(st:ControlState, objects:dict[str,pd.DataFrame]):
    with open(os.path.join(CONFIG_DIR,'config',st.account,'.snowcache'),'w') as f:

        f.write(json.dumps(
            {
               'local_cached_time': strftime("%Y-%m-%d %H:%M:%S",localtime()),
               'objects':{
                   # wHy DoNt u UsE to_DiCt, because the stupid to_dict function 
                   # isn't fucking json safe
                   k:(
                       json.loads(df.set_index('FULL_NAME',drop = True).to_json(orient='index'))
                        if not df.empty 
                        else {}
                   )
                   for k,df in objects.items()
               }
            }, indent = 4
        )
        )