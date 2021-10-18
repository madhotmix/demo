from sql_connector import sql_requester


def buy_offer(request):
    promocode_type_id = request.values['promocode_type_id']
    sql_query = {'title': 'get_offer_price',
                 'promocode_type_id': promocode_type_id}
    offer_price = sql_requester(sql_query)
    sql_query = {'title': 'client_data',
                 'client_login': request.values['client_login']}
    client_data = sql_requester(sql_query)

    """
    The situation is not possible due to the locking of the button at the front. 
    Contract below for backend anti-fraud
    """
    if offer_price > client_data['balance']:
        return

    sql_query = {'title': 'validation',
                 'promocode_type_id': promocode_type_id,
                 'client_id': client_data['client_id']}
    sql_requester(sql_query)
