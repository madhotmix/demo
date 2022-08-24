from flask import Flask, request
from os import environ

from api_handler import read_ms_db, api_requester
from health_check import run_health_check
from html_lib import waiting_animation
from purchase_processor import buy_offer
from second_loyalty_form_generator import get_second_loyalty_form


app = Flask(__name__)


@app.route('/second_loyalty_form', methods=['GET'])
def second_loyalty_form():
    return get_second_loyalty_form(request)


@app.route('/buy_offer', methods=['POST'])
def purchase_process():
    return buy_offer(request)


@app.route('/waiting_animation', methods=['GET'])
def load_animation():
    return waiting_animation()


@app.route('/api', methods=['GET'])
def api_get():
    return read_ms_db(request)


@app.route('/api', methods=['POST'])
def api_post():
    return api_requester(request)


@app.route('/health_check', methods=['GET'])
def health_check():
    return run_health_check()


app.run(host='0.0.0.0', port=environ['port'])
