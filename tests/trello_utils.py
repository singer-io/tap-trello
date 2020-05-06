"""
Harvest API Response, Access Token and Refresh Token
"""

import os
import json
import requests
import logging
from datetime import datetime as dt

def get_credentials(self):
    return {
        'testing_key': os.getenv('TAP_TRELLO_CONSUMER_KEY'),
        'testing_token': os.getenv('TAP_TRELLO_TESTING_TOKEN')
    }

BASE_URL = "https://api.trello.com/1"
HEADERS = {
    'Content-Type': 'application/json',
}
PARAMS = (
    ('key', '{}'.format(os.getenv('TAP_TRELLO_CONSUMER_KEY'))),
    ('token', '{}'.format(os.getenv('TAP_TRELLO_SERVER_TOKEN')))
)

def create_object(obj_type, data):
    resp = requests.post(url="{}/{}".format(BASE_URL, obj_type),
                         headers=HEADERS, params=PARAMS, json=data)
    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None
    return resp

def get_objects(obj_type, obj_id: str = ""):
    resp = requests.get(url="{}/members/me/{}/{}".format(BASE_URL, obj_type, obj_id),
                        headers=HEADERS, params=PARAMS)
    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None
    return resp

# existing_obj = get_objects(obj_type='boards')

# data = { "name": "Test Board {}".format(dt.utcnow().timestamp())}

# new_obj = create_object(obj_type='boards', data=data)

# if new_obj:
#     print("I created an object: \n{}".format(new_obj.text))
