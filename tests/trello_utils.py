""" Trello API utils for retrieving, and manipulating test data """

import os
import json
import requests
import random
import logging
from datetime import timedelta, date
from datetime import datetime as dt
from enum import Enum

PARENT_STREAM = {
    'actions': 'boards',
    'boards': 'boards',
    'lists': 'boards',
    'users': 'boards'
}
REPLICATION_METHOD = {
    'actions': 'INCREMENTAL',
    'boards': 'FULL_TABLE',
    'lists': 'FULL_TABLE',
    'users': 'FULL_TABLE'
}
TEST_USERS = {
    'Test User 1': {'username': os.getenv('TRELLO_TEST_USER_1_EMAIL'), 'password': os.getenv('TRELLO_TEST_USER_1_PW')},
    'Test User 2': {'username': os.getenv('TRELLO_TEST_USER_2_EMAIL'), 'password': os.getenv('TRELLO_TEST_USER_2_PW')},
    'Test User 3': {'username': os.getenv('TRELLO_TEST_USER_3_EMAIL'), 'password': os.getenv('TRELLO_TEST_USER_3_PW')}
}
BASE_URL = "https://api.trello.com/1"
HEADERS = {
    'Content-Type': 'application/json',
}
PARAMS = (
    ('key', '{}'.format(os.getenv('TAP_TRELLO_CONSUMER_KEY'))),
    ('token', '{}'.format(os.getenv('TAP_TRELLO_TESTING_TOKEN'))),
)
#    ('before',)
#    ('since',)

##########################################################################
### Utils for retrieving existing test data 
##########################################################################
def get_replication_method(stream):
    if stream in REPLICATION_METHOD.keys():
        return REPLICATION_METHOD.get(stream)

    raise NotImplementedError(
        "The expected replication method for {} has not been not set".format(stream)
    )
def get_parent_stream(stream):
    if stream in PARENT_STREAM.keys():
        return PARENT_STREAM.get(stream)

    raise NotImplementedError(
        "The expected replication method for {} has not been not set".format(stream)
    )
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

    # elif obj_type == 'cards':
    #     if req == "get":
    #         url_string += "/boards/{}/cards/{}".format(parent_id, obj_id)
    #     else:
    #         url_string += "/cards/{}".format(obj_id)

    elif obj_type == 'lists':
        if req == 'get' or 'delete':
            url_string += "/boards/{}/lists".format(parent_id)
        else:
            url_string += "/lists/{}".format(obj_id)
    else:
        raise NotImplementedError

    return url_string


def get_total_record_count_and_objects(parent_stream: str, child_stream: str):
    """Return the count and all records of a given child stream"""
    parent_objects = get_objects(obj_type=parent_stream)

    # If true, then this stream is top level so just need 1 get ^
    if parent_stream == child_stream:
        return len(parent_objects), parent_objects

    count = 0
    existing_objects = []
    for obj in parent_objects:
        objects = get_objects(obj_type=child_stream, parent_id=obj.get('id'))
        for obj in objects:
            already_tracked = False
            for e_obj in existing_objects:
                if obj['id'] == e_obj['id']:
                    already_tracked = True
                    break
            if not already_tracked:
                existing_objects.append(obj)
        count += len(objects)

    return count, existing_objects

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
        "USERS": {"type": ""},  # TODO {"fullName":"xae a12","username":"singersongwriterd42"}
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
        if data.get(field_to_update):
            data_to_update = {field_to_update: data.get(field_to_update)} # just change the name for baords (actions)
        data_to_update = {field_to_update: "admin"} # add member to a board for users
        endpoint = get_url_string("put", obj_type, obj_id, parent_id)
        print(" * Test Data | Changing: {} ".format(data_to_update))
        resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return None

        return resp.json()

    raise NotImplementedError

def update_object_user(obj_id: str = '', parent_id: str = '', field_to_update: str = 'type'):
    """Update a user by adding them to a board"""
    if not obj_id:
        obj_id = get_random_object_id('users')
    
    data = stream_to_data_mapping('users')
    if data:
        data_to_update = {field_to_update: "admin"} # add member to a board for users
        endpoint = get_url_string('put', 'users', obj_id, parent_id)
        print(" * Test Data | Changing: {} ".format(data_to_update))
        resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            data_to_update = {field_to_update: "normal"} # add member to a board for users
            print(" * Test Data | Changing: {} ".format(data_to_update))
            resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
            if resp.status_code >= 400:
                return None

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

def get_action_fields_by_stream():
    """
    returns the a dict of objects with a list of fields that can be altered in order to generate new actions
    """
    return {
        'boards': ['name'],
    }

# TODO add a parent id for updating child streams
def create_object_actions(recipient_stream: str = "boards", obj_id: str = ""):
    """
    method for generating specifc actions
    : param obj_id: id of action object
    : param parent_id: id of parent object (this will most likely be a board) # TODO may not need??
    : param recipient_stream: stream to update in order to genereate an action, important to track for setting expectations

    return the new actions object that was just completed
    """
    if not obj_id:
        obj_id = get_random_object_id(recipient_stream)

    pot_fields_to_update = get_action_fields_by_stream().get(recipient_stream)
    field_to_update = pot_fields_to_update[random.randint(0, len(pot_fields_to_update) - 1)]
    
    data = stream_to_data_mapping(recipient_stream)
    if data:
        if data.get(field_to_update):
            data_to_update = {field_to_update: data.get(field_to_update)} # just change the name for baords (actions)

            endpoint = get_url_string("put", recipient_stream, obj_id)
            print(" * Test Data | Changing: {} ".format(data_to_update))
            resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
            if resp.status_code >= 400:
                logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
                return None
            recipient_object_id = resp.json().get('id')
            actions_objects = get_objects(obj_type='actions', parent_id=recipient_object_id)

            return actions_objects[0]

    raise NotImplementedError
    
def create_object(obj_type, obj_id: str = "", parent_id: str = ""):
    """
    Create a single record for a given object

    Creates are not available for:
    : actions: we will call update for baords object instea
    : users: we will call update the user instead (add them to a baord)n

    return that object or none if create fails
    """
    if obj_type == 'actions':
        print(" * Test Data | DIRECT CREATES ARE UNAVAILABLE for {}. ".format(obj_type) +\
              "UPDATING another stream to generate new record")
        return create_object_actions('boards', obj_id=parent_id)
    if obj_type == 'users':
        print(" * Test Data | CREATES ARE UNAVAILABLE for {}".format(obj_type))
        return update_object_user(obj_id=obj_id, parent_id=parent_id,)

    print(" * Test Data | Request: POST on /{}/".format(obj_type))

    data = stream_to_data_mapping(obj_type)

    if data:
        if obj_type == 'lists' and parent_id: # TODO create separate 'create_object_lists' method
            data['idBoard'] = parent_id

        endpoint = get_url_string("post", obj_type)
        resp = requests.post(url=endpoint, headers=HEADERS, params=PARAMS, json=data)

        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return None

        return resp.json()

    raise NotImplementedError

def delete_object(obj_type, obj_id: str = "", parent_id: str = ""):
    print(" * Test Data | Request: DELETE on /{}/".format(obj_type))
    # TODO | WIP | do a delete for boards, then try for lists | can't delete actions or users

    if not obj_id:
        obj_id = get_random_object_id(obj_type)

    endpoint = get_url_string("delete", obj_type, obj_id, parent_id)
    resp = requests.delete(url=endpoint, headers=HEADERS, params=PARAMS)
    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None

    return resp.json()

##########################################################################
### Testing the utils above
##########################################################################
if __name__ == "__main__":
    test_creates = False
    test_updates = False
    test_gets = False
    test_deletes = True

    print_objects = True

    objects_to_test = ['actions'] # ['actions', 'cards', 'lists'] #'boards'

    print("********** Testing basic functions of utils **********")
    if test_creates:
        for obj in objects_to_test:
            print("Testing CREATE: {}".format(obj))
            created_obj = create_object(obj)
            if created_obj:
                print("SUCCESS")
                if print_objects:
                    print(created_obj)
                continue
            print("FAILED")
    if test_updates:
        for obj in objects_to_test:
            print("Testing UPDATE: {}".format(obj))
            updated_obj = update_object(obj)
            if updated_obj:
                print("SUCCESS")
                if print_objects:
                    print(updated_obj)
                continue
            print("FAILED")
    if test_gets:
        for obj in objects_to_test:
            print("Testing GET: {}".format(obj))
            existing_objs = get_objects(obj)
            if existing_objs:
                print("SUCCESS")
                if print_objects:
                    print(existing_objs)
                continue
            print("FAILED")

