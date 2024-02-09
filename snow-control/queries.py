
SHOW_QUERY = "show {}s in account"

INTEGRATION_SHOW_QUERY = "show {}s"

NAME_QUERY = """select *, concat_ws('.',{key}) as full_name
from table(result_scan('{qid}'))
where "name" not like '%SNOWFLAKE_KAFKA_CONNECTOR%'
and "name" != 'INFORMATION_SCHEMA' """


GRANTS_TO_USER_QUERY = """
    show grants to user "{user}"
"""

RETRIEVE_GRANTS_TO_USER_QUERY = """
    select "role" from table(result_scan('{qid}'))
"""

SET_SEARCH_PATH = """
alter session set search_path = '$current, $public, snowflake.ml, snowflake.core'
"""


CURRENT_GRANTS_TO_ROLE = """
    select "privilege", replace("granted_on",'_',' '), "name" from table(result_scan('{qid}'))
    where "name" not like "%SNOWFLAKE_KAFKA_CONNECTOR%"
    and "name" != 'INFORMATION_SCHEMA'
    and "privilege" not in ('OWNERSHIP')
    and "granted_on" != 'ROLE'
    and "name" not like 'SNOWFLAKE%'
"""

FUTURE_GRANTS_TO_ROLE = """
    select "privilege", replace("grant_on",'_',' '), 
    regexp_replace("name" ,'[.][<].*[>]$','') as root_obj
    from table(result_scan('{qid}'))
    where "name" not like "%SNOWFLAKE_KAFKA_CONNECTOR%"
    and "name" != 'INFORMATION_SCHEMA'
    and "privilege" not in ('OWNERSHIP')
    and "granted_on" != 'ROLE'
    and "name" not like 'SNOWFLAKE%'
"""