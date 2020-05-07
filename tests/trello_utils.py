"""
Harvest API Response, Access Token and Refresh Token
"""

import os
import json
import requests
import random
import logging
from datetime import timedelta, date
from datetime import datetime as dt
from enum import Enum


BASE_URL = "https://api.trello.com/1"
HEADERS = {
    'Content-Type': 'application/json',
}
PARAMS = (
    ('key', '{}'.format(os.getenv('TAP_TRELLO_CONSUMER_KEY'))),
    ('token', '{}'.format(os.getenv('TAP_TRELLO_SERVER_TOKEN')))
)


##########################################################################
### Utils for retrieving existing test data 
##########################################################################
def get_objects(obj_type: str, obj_id: str = ""):
    """
    get all objects for a given object
    -  or -
    get a specific obj by id
    """
    print("Running a GET on /{}/{}".format(obj_type, obj_id))
    resp = requests.get(url=get_url_string("get", obj_type, obj_id), headers=HEADERS, params=PARAMS)

    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None

    return resp.json()


def get_random_object_id(obj_type: str):
    """Return the id of a random object for a specified object_type"""
    objects = get_objects(obj_type)
    random_object = objects[random.randint(0, len(objects) -1)]

    return random_object.get('id')


def get_url_string(req: str, obj_type: str, obj_id: str = ""):
    """"""
    url_string = BASE_URL

    if obj_type == 'boards':
        if req == "get":
            url_string += "/members/me/{}/{}".format(obj_type, obj_id)
        else:
            url_string += "/{}/{}".format(obj_type, obj_id)
        return url_string

    random_board_id = get_random_object_id('boards') # Needed for accessing nested structure
    # TODO will need a get all objects method regardless of top level object....

    if obj_type == 'users':
        url_string += "/boards/{}/members/{}".format(random_board_id, obj_id)

    elif obj_type == 'cards':
        if req == "get":
            url_string += "/boards/{}/cards/{}".format(random_board_id, obj_id)
        else:
            url_string += "/cards/{}".format(obj_id)
    elif obj_type == 'lists':
        url_string += "/boards/{}/lists/{}".format(random_board_id, obj_id)

    else:
        raise NotImplementedError

    return url_string

##########################################################################
### Test Data
##########################################################################
tstamp = dt.utcnow().timestamp() # this is used to genereate unique data

class TestData(Enum):
    BOARDS = { "name": "Test Board {}".format(tstamp)}
    USERS = ""  # {"fullName":"xae a12","username":"singersongwriterd42"}
    CARDS = {
        "name":"Card {}".format(tstamp),
        "desc": "This is a description.",
        "pos": "{}".format(random.choice(["top", "bottom", random.randint(1,20)])),  # card position [top,bottom,positive float]
        "due": "{}".format((dt.today() + timedelta(days=5)).date()),  # A due date for the card (Format: date)
        "dueComplete": random.choice([True, False]),  # boolean
        "idList": "{}".format(get_random_object_id('lists')),  # REQUIRED ID of list card is created in ^[0-9a-fA-F]{32}$
        "idMembers": [], # Array<string> comma-separated list of member IDs to add to the card
        "idLabels": [],# Array<string> Comma-separated list of label IDs to add to the card
        "urlSource": "", # A URL starting with http:// or https:// Format: url
        "fileSource": "", # Format: binary
        "idCardSource": "",#"{}".format(get_random_object_id('cards')), # The ID of a card to copy into the new card Pattern: ^[0-9a-fA-F]{32}$
        "address": "", # For use with/by the Map Power-Up
        "locationName": "", #For use with/by the Map Power-Up
        "coordinates": "", # For use with/by the Map Power-Up. Should take the form latitude,longitude
        "keepFromSource": "string", # If using idCardSource you can specify which properties to copy over. all or comma-separated list of: attachments,checklists,comments,due,labels,members,stickers. Style: form, Default: all. Valid values: all, attachments, checklists, comments, due, labels, members, stickers
    }
    LISTS = ""


##########################################################################
### Utils for creating new test data
##########################################################################
stream_to_data_mapping = {
    "boards": TestData.BOARDS,
    "users": TestData.USERS,
    "lists": TestData.LISTS,
    "cards": TestData.CARDS,
}
def create_object(obj_type):
    """
    create a single record for a given object
    return that object or none if create fails
    """
    print("Running a POST on /{}/".format(obj_type))
    # Update the current timestamp to ensure record is unique
    tstamp = dt.utcnow().timestamp()
    data = stream_to_data_mapping[obj_type].value
    if data:    
        resp = requests.post(url=get_url_string("post", obj_type), headers=HEADERS, params=PARAMS, json=data)
        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return resp

        return resp.json()

    raise NotImplementedError


##########################################################################
### Testing the utils above
##########################################################################
stream = 'cards'
# existing_obj = get_objects(stream)
# print(existing_obj)
new_obj = create_object(obj_type=stream)
print(new_obj)
existing_obj = get_objects(obj_type=stream)
print(existing_obj)


# existing_obj = get_objects(obj_type='actions')
# print(existing_obj.text)
# data = { "name": "Test Board {}".format(dt.utcnow().timestamp())}

# new_obj = create_object(obj_type='boards', data=data)

# if new_obj:
#     print("I created an object: \n{}".format(new_obj.text))

# record = {"id":"5eb2193393a06a2d30f3e3fc","idMemberCreator":"5ea9e34cbc3f72317f964c45","data":{"creationMethod":"automatic","board":{"id":"5eb2193393a06a2d30f3e3f7","name":"Test Board 1588744562.556537","shortLink":"ceATiT4r"}},"type":"createBoard","date":"2020-05-06T01:56:03.030Z","limits":{},"memberCreator":{"id":"5ea9e34cbc3f72317f964c45","username":"singersongwriter3","activityBlocked":false,"avatarHash":null,"avatarUrl":null,"fullName":"Singer Songwriter","idMemberReferrer":null,"initials":"SS","nonPublic":{},"nonPublicAvailable":false}}
