import os
import re

from html_lib import (get_purchases_header, get_purchase_with_promocode_body, get_purchase_without_promocode_body,
                      get_offers_header, get_offer_body)
from sql_connector import sql_requester


def get_purchases(client_data):
    purchases_html = ''
    purchases = client_data['purchases']
    if purchases:
        purchases_html = get_purchases_header()
    for purchase in purchases:
        promocode = purchase['promocode']
        text = purchase['text']
        if promocode and promocode == promocode:
            purchase_html = get_purchase_with_promocode_body()
            purchase_html = purchase_html.format(source=purchase['source'],
                                                 title=purchase['title'],
                                                 promocode=promocode,
                                                 index=purchases.index(purchase))
        elif text and text == text:  # offer without promocode
            purchase_html = get_purchase_without_promocode_body()
            purchase_html = purchase_html.format(source=purchase['source'],
                                                 title=purchase['title'],
                                                 text=text,
                                                 index=purchases.index(purchase))
        purchases_html += purchase_html
    return purchases_html


def get_offer(offer, balance, offers_html):
    price = offer['price']
    price_message = f'{price} балл'
    price_message = correct_word_endings(price_message, price)

    status = ''
    if price > balance:
        status = 'disabled'

    offer_html = get_offer_body()
    offer_html = offer_html.format(source=offer['source'],
                                   title=offer['title'],
                                   price_message=price_message,
                                   status=status,
                                   promocode_type_id=offer['promocode_type_id'])
    offers_html += offer_html
    return offers_html


def get_offers(balance):
    sql_query = {'title': 'offers'}
    offers = sql_requester(sql_query)
    if offers.empty:
        return ''

    offers_html = get_offers_header()
    for i, offer in offers.iterrows():
        offers_html = get_offer(offer, balance, offers_html)
    return offers_html


def correct_word_endings(message, points):
    points = str(points)
    if 5 <= int(points[-1]) <= 9 or int(points[-1]) == 0 or 11 <= int(points[-2:]) <= 14:
        message = message.replace('балл', 'баллов')
    elif 2 <= int(points[-1]) <= 4:
        message = message.replace('балл', 'балла')
    return message


def generate_html(client_data):
    with open(os.path.join(os.path.dirname(__file__),
                           'templates',
                           'second_loyalty_form_template.html'
                           ), 'r', encoding='utf-8') as f:
        second_loyalty_form = f.read()

    balance = client_data['balance']
    balance_message = f'У тебя {balance} балл'
    balance_message = correct_word_endings(balance_message, balance)
    second_loyalty_form = second_loyalty_form.replace('{balance_message}', balance_message)

    purchases = get_purchases(client_data)
    second_loyalty_form = second_loyalty_form.replace('{purchases}', purchases)

    offers = get_offers(balance)
    second_loyalty_form = second_loyalty_form.replace('{offers}', offers)

    client_login = client_data['client_login']
    second_loyalty_form = second_loyalty_form.replace('{client_login}', f'"{client_login}"')

    return second_loyalty_form


def get_second_loyalty_form(request):
    client_login = request.args.get('client_login')
    if not client_login:
        return 'client_login is a required URL parameter'
    client_login = re.sub('[^A-Za-z0-9]', '', client_login)

    sql_query = {'title': 'client_data',
                 'client_login': client_login}
    client_data = sql_requester(sql_query)
    if not client_data:
        return f'{client_login} – no such client_login'
    second_loyalty_form = generate_html(client_data)
    return second_loyalty_form
