from control_state import ControlState
from styling import GREEN_CHECKMARK, RED_X, print_execution
from functools import reduce
from typing import Tuple,Callable
import snowflake.connector.errors as snow_errors

def apply(state:ControlState, executables:list[str], plan_id:int, method = 'seq'):
    cur = state.connection.cursor()
    current_role = list(cur.execute('SELECT CURRENT_ROLE()'))[0][0]
    grant_results = {}
    if current_role != 'ACCOUNTADMIN':
        print('Control plans can only be executed by an ACCOUNTADMIN') # TODO: change? "CONTROL ROLE"
        return 
    if method == 'seq':
        for query in executables:
            qid, result = sequential_query_execute(state = state, executable_query = query)
            grant_results[qid] = {
                'text':query,
                'result':result
            }
            if result:
                print('\n')
            else:
                print(GREEN_CHECKMARK)
    else:
        single_grant_func = lambda q: execute_and_retrieve_grant_result(state,q)
        result_iterator = state.executor.map(single_grant_func,executables)
        grant_results = reduce(lambda x,y: x|y, result_iterator)
        for qid,info in grant_results.items():
            result_symbol = RED_X if info['result'] else GREEN_CHECKMARK
    
def snowflake_query_error_handling(query_calling_function:Callable):
    try:
        query_calling_function()
        result = 0
    except snow_errors.ProgrammingError as pe:
        result = pe.errno
    finally:
        return result

def sequential_query_execute(state:ControlState, executable_query:str, print_seq = True) -> dict: 
    result = -1
    cursor = state.connection.cursor()
    try:
        cursor.execute(executable_query)
        if print_seq:
            print_execution(executable_query, result = '+')
        result = 0 
    except snow_errors.ProgrammingError as pe:
        if print_seq:
            print_execution(executable_query, result = '-')
        result = pe.errno
    finally:
        return (cursor.sfqid,result)

def execute_and_retrieve_grant_result(state:ControlState, executable_query:str) -> Tuple[str,int]:
    cursor = state.connection.cursor()
    qid, errno = sequential_query_execute(state,executable_query,print_seq=False )
    return {
        qid:{
            'text':executable_query,
            'result':errno
        }
    }

def log_executed_quereis(state:ControlState,plan_id:int,grant_results:dict):
    pass