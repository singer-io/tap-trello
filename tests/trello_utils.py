""" Trello API utils for retrieving, and manipulating test data """

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
    ('token', '{}'.format(os.getenv('TAP_TRELLO_SERVER_TOKEN'))),
    ('since', '2020-03-01T00:00:00Z')
)

#('since', '{}'.format(dt.utcnow()))

##########################################################################
### Utils for retrieving existing test data 
##########################################################################
def get_objects(obj_type: str, obj_id: str = "", parent_id: str = ""):
    """
    get all objects for a given object
    -  or -
    get a specific obj by id
   """
    print(" * Test Data |  Request: GET on /{}/{}".format(obj_type, obj_id))
    endpoint = get_url_string("get", obj_type, obj_id, parent_id)
    resp = requests.get(url=endpoint, headers=HEADERS, params=PARAMS)

    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None

    return resp.json()


def get_random_object_id(obj_type: str):
    """Return the id of a random object for a specified object_type"""
    objects = get_objects(obj_type)
    random_object = objects[random.randint(0, len(objects) -1)]

    return random_object.get('id')


def get_url_string(req: str, obj_type: str, obj_id: str = "", parent_id: str = ""):
    """
    Form the endpoint for a request based on the following:
     :param req: Type of request, oneOf['get', 'post', 'delete'].
     :param obj_type: The stream you are making the reqest on.
     :param obj_id: The specific id of a record in the stream you are making a request on.
     :param parent_id: The specific id of a record that is the parent of the stream you are making a request on.
                       If you do not specific a parent id, a random one will be selected if needed
    eg. 
    Get all actions for a given baord:
        get_url_string(req='get', obj_type='actions', obj_id='', parent_id=<id-of-a-specific-board>)
    Create a card anywhere:
        get_url_string(req='post', obj_type='cards', obj_id='', parent_id='')
    """
    
    url_string = BASE_URL

    if obj_type == 'boards': # TODO may need to parameterize a members parent obj here
        if req == "get":
            url_string += "/members/me/{}/{}".format(obj_type, obj_id)
        else:
            url_string += "/{}/{}".format(obj_type, obj_id)
        return url_string

    if not parent_id: # Needed for accessing nested structure
        parent_id = get_random_object_id('boards')
    # TODO may need to add a 'parent_obj' param if we don't want to use baords

    if obj_type == 'users':
        url_string += "/boards/{}/members/{}".format(parent_id, obj_id)

    elif obj_type == 'actions':
        url_string += "/boards/{}/actions/{}".format(parent_id, obj_id)

    elif obj_type == 'cards':
        if req == "get":
            url_string += "/boards/{}/cards/{}".format(parent_id, obj_id)
        else:
            url_string += "/cards/{}".format(obj_id)

    elif obj_type == 'lists':
        if req == 'get':
            url_string += "/boards/{}/lists".format(parent_id)
        else:
            url_string += "/lists/{}".format(obj_id)
    else:
        raise NotImplementedError

    return url_string


##########################################################################
### Test Data
##########################################################################
tstamp = dt.utcnow().timestamp() # this is used to genereate unique dat
print(" * Test Data | INITIALIZING tstamp to {}".format(tstamp))

def get_test_data():
    global tstamp
    tstamp = dt.utcnow().timestamp() # this is used to genereate unique data

    TEST_DATA = {
        "BOARDS": {"name": "Test Board {}".format(tstamp)},
        "USERS": "",  # TODO {"fullName":"xae a12","username":"singersongwriterd42"}
        "CARDS": {
            "name":"Card {}".format(tstamp),
            "desc": "This is a description.",
            "pos": "{}".format(random.choice(["top", "bottom", random.randint(1,20)])),  # card pos [top,bottom,positive float]
            "due": "{}".format((dt.today() + timedelta(days=5)).date()),  # A due date for the card (Format: date)
            "dueComplete": random.choice([True, False]),  # boolean
            "idList": "{}".format(get_random_object_id('lists')),  # REQUIRED ID of list card is created in ^[0-9a-fA-F]{32}$
            "idMembers": [], # Array<string> comma-separated list of member IDs to add to the card
            "idLabels": [],# Array<string> Comma-separated list of label IDs to add to the card
            "urlSource": "", # A URL starting with http:// or https:// Format: url
            "fileSource": "", # Format: binary
            "idCardSource": "",#"{}".format(get_random_object_id('cards')), # ID of card to copy into new card ^[0-9a-fA-F]{32}$
            "address": "", # For use with/by the Map Power-Up
            "locationName": "", #For use with/by the Map Power-Up
            "coordinates": "", # For use with/by the Map Power-Up. Should take the form latitude,longitude
            "keepFromSource": "string", # If using idCardSource you can specify which properties to copy over. all or comma-separated list of: attachments,checklists,comments,due,labels,members,stickers. Style: form, Default: all. Valid values: all, attachments, checklists, comments, due, labels, members, stickers
        },
        "LISTS" : {
            "name":"Card {}".format(tstamp),
            "idBoard": "{}".format(get_random_object_id('boards')),  # The long ID of the board the list should be created on
            "idListSource":"",  # ID of the List to copy into the new List
            "pos": "{}".format(random.choice(["top", "bottom", random.randint(1,20)])),  # card pos [top,bottom,positive float]

        }
    }

    return TEST_DATA


##########################################################################
### Utils for updating existing test data
##########################################################################
def update_object(obj_type: str, obj_id: str = '', parent_id: str = '', field_to_update: str = 'name'):
    """
    update an existing object in order to genereate a new 'actions' record
    """
    print(" * Test Data | Request: PUT on /{}/".format(obj_type))

    if not obj_id:
        obj_id = get_random_object_id(obj_type)
    
    data = stream_to_data_mapping(obj_type)
    if data:
        data_to_update = {field_to_update: data.get(field_to_update)} # just change the name
        endpoint = get_url_string("put", obj_type, obj_id, parent_id)
        print(" * Test Data | Changing: {} ".format(data_to_update))
        resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return resp

        return resp.json()

    raise NotImplementedError


##########################################################################
### Utils for creating new test data
##########################################################################
def stream_to_data_mapping(stream):
    data = get_test_data()
    mapping = {
        'actions': "BOARDS", # random.choice(data) TODO
        "boards": "BOARDS",
        "cards": "CARDS",
        "lists": "LISTS",
        "users": "USERS",
    }
    return data[mapping[stream]]


def create_object(obj_type, obj_id: str = "", parent_id: str = ""):
    """
    Create a single record for a given object

    To create an actions record, we will call update for another object stream

    return that object or none if create fails
    """
    if obj_type == 'actions':
        return update_object('boards', obj_id=parent_id)

    print(" * Test Data | Request: POST on /{}/".format(obj_type))

    data = stream_to_data_mapping(obj_type)
    if data:    
        endpoint = get_url_string("post", obj_type)
        resp = requests.post(url=endpoint, headers=HEADERS, params=PARAMS, json=data)
        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return resp

        return resp.json()

    raise NotImplementedError


##########################################################################
### Testing the utils above
##########################################################################
if __name__ == "__main__":
    test_creates = False
    test_updates = False
    test_gets = True

    objects_to_test = ['actions', 'cards', 'lists'] #'boards'

    print("********** Testing basic functions of utils **********")
    if test_creates:
        for obj in objects_to_test:
            print("Testing CREATE: {}".format(obj))
            created_obj = create_object(obj)
            if created_obj:
                print("SUCCESS")
                continue
            print("FAILED")
    if test_updates:
        for obj in objects_to_test:
            print("Testing UPDATE: {}".format(obj))
            updated_obj = update_object(obj)
            if created_obj:
                print("SUCCESS")
                continue
            print("FAILED")
    if test_gets:
        for obj in objects_to_test:
            print("Testing GET: {}".format(obj))
            existing_objs = get_objects(obj)
            if existing_objs:
                print("SUCCESS")
                continue
            print("FAILED")

