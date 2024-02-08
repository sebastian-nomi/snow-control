
SHOW_QUERY = "show {}s in account"

INTEGRATION_SHOW_QUERY = "show {}s"

NAME_QUERY = """select *, concat_ws('.',{key}) as full_name
from table(result_scan('{qid}'))
where "name" not like '%SNOWFLAKE_KAFKA_CONNECTOR%'
and "name" != 'INFORMATION_SCHEMA' """


GRANTS_TO_USER_QUERY = """
    show grants to user "{user}"
"""

RETRIEVE_GARNTS_TO_USER_QUERY = """
    select "role" from table(result_scan('{qid}'))
"""

SET_SEARCH_PATH = """
alter session set search_path = '$current, $public, snowflake.ml, snowflake.core'
"""