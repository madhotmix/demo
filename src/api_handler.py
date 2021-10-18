from flask import Response
import json
from os import environ

from sql_connector import sql_requester


def api_authorize(token_from_front):
    token = environ['ms_api_token']
    if token_from_front != token:
        message = {'message': 'unauthorized'}
        response = Response(response=json.dumps(message),
                            status=403,
                            headers={'Content-Type': 'application/json'})
        return response
    return True


def read_ms_db(request):
    request = request.json
    authorization_result = api_authorize(request['token'])
    if isinstance(authorization_result, Response):
        return authorization_result
    sql_request = {'title': 'read_ms_db',
                   'query': request['query']}
    df = sql_requester(sql_request)
    df = json.dumps(df)
    return df


def api_requester(request):
    request = request.json
    authorization_result = api_authorize(request['token'])
    if isinstance(authorization_result, Response):
        return authorization_result
    action = request['action']
    if action == 'add_promocodes':
        sql_request = {'title': action,
                       'data': request['data']}
        sql_requester(sql_request)
    elif action == 'direct_sql_query':
        sql_request = {'title': action,
                       'query': request['query']}
        df = sql_requester(sql_request)
    return 'done'
