from ..common.db import connection

def query_timestamp(column, table_name, start_timestamp, end_timestamp):
    query = f'SELECT DISTINCT "{column}" FROM "DBO"."{table_name}" WHERE "{column}" BETWEEN \'{start_timestamp}\' AND \'{end_timestamp}\''
    result = connection.execute(sql_cmd=query)
    return result