from flask import Response
import json

from sql_connector import sql_requester


def run_health_check():
    request = {'title': 'check_mysql_connection'}
    mysql_connection_status = sql_requester(request)

    request = {'title': 'check_ms_db_connection'}
    ms_db_connection_status = sql_requester(request)

    statuses = {'mysql_connection_status': mysql_connection_status,
                'ms_db_connection_status': ms_db_connection_status}
    if mysql_connection_status['status'] and ms_db_connection_status['status']:
        status_code = 200
    else:
        status_code = 503
    response = Response(response=json.dumps(statuses),
                        status=status_code,
                        headers={'Content-Type': 'application/json'})
    return response
