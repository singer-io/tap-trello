""" Trello API utils for retrieving, and manipulating test data """

import os
import json
import requests
# import backoff # TODO add backoff if necessary
import random
import logging
from datetime import timedelta, date
from datetime import datetime as dt
from time import sleep


PARENT_OBJECTS = []
LIST_OBJECTS = []
NEVER_DELETE_BOARD_ID = "5eb080e9d9e0f56e8311ab5f"
PARENT_STREAM = {
    'actions': 'boards',
    'boards': 'boards',
    'lists': 'boards',
    'cards': 'boards',
    'checklists': 'boards',
    'users': 'boards'
}
MAX_API_LIMIT = 1000 # this is the smallest
# MAX_API_LIMIT = {
#     'actions': 1000
#     'boards': None,
#     'cards': 20000,
#     'lists': None,
#     'users': None
# }
REPLICATION_METHOD = {
    'actions': 'INCREMENTAL',
    'boards': 'FULL_TABLE',
    'cards': 'FULL_TABLE',
    'checklists': 'FULL_TABLE',
    'lists': 'FULL_TABLE',
    'users': 'FULL_TABLE'
}
TEST_USERS = {
    'Test User 1': {'username': os.getenv('TRELLO_TEST_USER_1_EMAIL'), 'password': os.getenv('TRELLO_TEST_USER_1_PW'), 'id': os.getenv('TRELLO_TEST_USER_1_ID')},
    'Test User 2': {'username': os.getenv('TRELLO_TEST_USER_2_EMAIL'), 'password': os.getenv('TRELLO_TEST_USER_2_PW'), 'id': os.getenv('TRELLO_TEST_USER_2_ID')},
    'Test User 3': {'username': os.getenv('TRELLO_TEST_USER_3_EMAIL'), 'password': os.getenv('TRELLO_TEST_USER_3_PW'), 'id': os.getenv('TRELLO_TEST_USER_3_ID')}
}
BASE_URL = "https://api.trello.com/1"
HEADERS = {
    'Content-Type': 'application/json',
}
DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
SINCE = dt.strftime(dt.utcnow(), DATE_FORMAT)
PARAMS = (
    ('key', '{}'.format(os.getenv('TAP_TRELLO_CONSUMER_KEY'))),
    ('token', '{}'.format(os.getenv('TAP_TRELLO_TESTING_TOKEN'))),
    ('limit', '{}'.format(MAX_API_LIMIT)),
)

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

def get_objects_users(obj_type: str='users', obj_id: str = "", parent_id: str = ""):
    """Get all members on a specific parent object, add that parent obj_id to the returned resp."""
    print(" * Test Data |  Request: GET on /{}/{}".format(obj_type, obj_id))

    if not parent_id: # needs to execute here so we can grab the board_id for the ret_val
        parent_id = get_random_object_id('boards')

    endpoint = get_url_string("get", obj_type, obj_id, parent_id)
    resp = requests.get(url=endpoint, headers=HEADERS, params=PARAMS)

    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))

        return None

    # Add baord id as tap-defined field 'boardId'
    user_objects = resp.json()
    for user in user_objects:
        user['boardId'] = parent_id

    return user_objects

def get_objects(obj_type: str, obj_id: str = "", parent_id: str = "", since = None, custom_fields=False):
    """
    get all objects for a given object
    -  or -
    get a specific obj by id
   """
    if obj_type == 'users': # Dispatch b/c this requires additional logic
        return get_objects_users(obj_id=obj_id, parent_id=parent_id)
        
    print(" * Test Data |  Request: GET on /{}/{}".format(obj_type, obj_id))

    endpoint = get_url_string("get", obj_type, obj_id, parent_id)
    if custom_fields:
        if not obj_id:
            raise Exception("Must specify an obj_id to acquire customFields")
        endpoint = BASE_URL + "/{}/{}/customFieldItems".format(obj_type, obj_id)

    parameters = PARAMS
    if since:
        parameters = PARAMS + (('since', since),)
    resp = requests.get(url=endpoint, headers=HEADERS, params=parameters)

    if resp.status_code >= 400:
       logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))

       return None

    return resp.json()

def get_random_object_id(obj_type: str, parent_id: str = ""):
    """Return the id of a random object for a specified object_type"""
    global PARENT_OBJECTS  #, LIST_OBJECTS

    if obj_type == 'users':
        user_ids = [user.get('id') for user in TEST_USERS.values()]
        return random.choice(user_ids)

    elif obj_type in ['cards', 'lists']:
        if not parent_id:
            objects = ""
            while not objects: # Protect against boards without cards or lists
                objects = get_objects(obj_type)
            random_object = objects[random.randint(0, len(objects) -1)]
            return random_object.get('id')

        objects = get_objects(obj_type, parent_id=parent_id)
        random_object = objects[random.randint(0, len(objects) -1)]
        return random_object.get('id')
        

    elif obj_type == get_parent_stream(obj_type): # if boards
        if not PARENT_OBJECTS: # if we have not already done a get on baords
            PARENT_OBJECTS = get_objects(obj_type)

        objects = PARENT_OBJECTS
        random_object = objects[random.randint(0, len(objects) -1)]

        return random_object.get('id')

    # elif obj_type == 'lists':
    # if not LIST_OBJECTS: # if we have not already done a get on baords
        #     LIST_OBJECTS = get_objects(obj_type)
        # objects = LIST_OBJECTS

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
        if obj_id:
            url_string += "/boards/{}/members/{}".format(parent_id, obj_id)
        else:
            url_string += "/boards/{}/members/all".format(parent_id)
    elif obj_type == 'actions':
        url_string += "/boards/{}/actions/{}".format(parent_id, obj_id)

    elif obj_type == 'cards':
        if req == "get":
            if obj_id:
                url_string += "/boards/{}/cards/{}".format(parent_id, obj_id)
            else:
                url_string += "/boards/{}/cards/all".format(parent_id)
        else:
            url_string += "/cards/{}".format(obj_id)

    elif obj_type == 'checklists':
        if req == 'get':
            url_string += "/boards/{}/checklists/{}".format(parent_id, obj_id)
        else:
            url_string += "/checklists"

    elif obj_type == 'lists':
        if req == 'get' or 'delete':
            url_string += "/boards/{}/lists".format(parent_id)
        else:
            url_string += "/lists/{}".format(obj_id)
    else:
        raise NotImplementedError

    return url_string


def get_total_record_count_and_objects(child_stream: str="", since = None):
    """
    : param child_stream: 
    Return the count and all records of a given child stream
    """
    global PARENT_OBJECTS

    parent_stream = get_parent_stream(child_stream)

    if not PARENT_OBJECTS:
        PARENT_OBJECTS = get_objects(obj_type=parent_stream, since=since)

    # If true, then this stream is top level so just need 1 get ^
    if parent_stream == child_stream:
        return len(PARENT_OBJECTS), PARENT_OBJECTS

    count = 0
    existing_objects = []
    for obj in PARENT_OBJECTS:
        objects = get_objects(obj_type=child_stream, parent_id=obj.get('id'), since=since)
        for obj in objects:
            already_tracked = False
            for e_obj in existing_objects:
                if obj['id'] == e_obj['id'] and child_stream != 'users':
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
        "BOARDS": {
            "name": "Board {}".format(tstamp),
            "defaultLabels": False,
            "defaultLists": False,
            "desc": "desciptive description",
            "idOrganization": "",
            "idBoardSource": "",  # The id of a board to copy into the new board.
            "keepFromSource": "",  # To keep cards from the original board pass in the value cards (Valid values: cards, none)
            "powerUps": "customFields",  # One of: all, calendar, cardAging, recap, voting.
            "prefs_permissionLevel": "private",  # One of: org, private, public.
            "prefs_voting": "disabled",  # Who can vote on this board. One of disabled, members, observers, org, public.
            "prefs_comments": "members",  # Who can comment on cards on board. Oneof: disabled, members, observers, org, public.
            "prefs_invitations": "members",  # Determines what types of members can invite users to join. One of: admins, members.
            "prefs_selfJoin": True,  # whether users can join the boards or have to be invited. Default: true
            "prefs_cardCovers": True,  # Determines whether card covers are enabled.
            "prefs_background": "blue",  # id of  background or oneof: blue, orange, green, red, purple, pink, lime, sky, grey
            "prefs_cardAging": "regular", # type of card aging on the board (if enabled) One of: pirate, regular. Default: regular
        },
        "USERS": {"type": ""},  # {"fullName":"xae a12","username":"singersongwriterd42"}
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
            "keepFromSource": "string", # If using idCardSource you can specify which properties to copy over.
        },
        "CHECKLISTS": {
            "name": "Checklist {}".format(tstamp),
            "idCard": "{}".format(get_random_object_id('cards')),
            "pos": "{}".format(random.choice(["top", "bottom", random.randint(1,20)])),  # [top,bottom,positive float]
            "idChecklistSource": "",
        },
        "LISTS" : {
            "name":"List {}".format(tstamp),
            "idBoard": "{}".format(get_random_object_id('boards')),  # The long ID of the board the list should be created on
            "idListSource":"",  # ID of the List to copy into the new List
            "pos": "{}".format(random.choice(["top", "bottom", random.randint(1,20)])),  # card pos [top,bottom,positive float]
        }
    }

    return TEST_DATA

##########################################################################
### Utils for updating existing test data
##########################################################################
def update_object(obj_type: str, obj_id: str = '', parent_id: str = '', field_to_update: str = ''):
    """
    update an existing object in order to genereate a new 'actions' record
    """
    print(" * Test Data | Request: PUT on /{}/".format(obj_type))
    
    if not obj_id:
        obj_id = get_random_object_id(obj_type)

    if obj_type == "user":
        return update_object_user(obj_id, parent_id, field_to_update)

    elif obj_type == "boards":
        return update_object_board(obj_id, field_to_update)

    elif obj_type == "actions":
        return update_object_board(obj_id)

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

def update_object_action(obj_id: str = ''):  #, field_to_update: str = 'name'):
    """update comment actions"""
    if not obj_id:
        raise Exception("You must specify a comment object action")
    endpoint = BASE_URL + "/actions/{}/text".format(obj_id)
    data = {"value": get_action_comment(update=True)}
    resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data)
    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None

    return resp.json()

def update_object_board(obj_id: str = '', field_to_update: str = 'name'):
    """Update a user by adding them to a board, parent_id should be specified"""
    while not obj_id or obj_id == NEVER_DELETE_BOARD_ID:
        obj_id = get_random_object_id('boards')

    if obj_id == NEVER_DELETE_BOARD_ID:
        logging.warn("Request Ignored |  You tried to change a board that other tests rely on |  " +\
                     "Board (id={}) should not be altered".format(NEVER_DELETE_BOARD_ID))
        raise Exception("We do not have enough boards. We are being forced to update board named 'NEVER DELETE'")

    data = stream_to_data_mapping('boards')
    if data:
        data_to_update = {field_to_update: data.get(field_to_update)}
        endpoint = get_url_string('put', 'boards', obj_id, '')
        print(" * Test Data | Changing: {} ".format(data_to_update))
        resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return None

        return resp.json()

    raise NotImplementedError

def update_object_user(obj_id: str = '', parent_id: str = '', field_to_update: str = 'type'):
    """Update a user by adding them to a board, parent_id should be specified"""
    if not obj_id:
        obj_id = get_random_object_id('users')

    while not parent_id or parent_id == NEVER_DELETE_BOARD_ID:
        parent_id = get_random_object_id('boards')

    data = stream_to_data_mapping('users')
    if data:
        data_to_update = {field_to_update: "normal"} # add member to a board for users
        endpoint = get_url_string('put', 'users', obj_id, parent_id)
        print(" * Test Data | Changing: {} ".format(data_to_update))
        resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            data_to_update = {field_to_update: "admin"} # add member to a board for users
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
        "checklists": "CHECKLISTS",
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
        'cards': ['text'],
    }

def get_action_comment(update: bool = False):
    global tstamp
    tstamp = dt.utcnow().timestamp()

    comment_type = ""
    if update:
        comment_type = "UPDATE "
    comment = "{}Here is a comment for ya at approx: {}".format(comment_type, tstamp)
    return comment

def create_object_actions(recipient_stream: str = "boards", obj_id: str = "", action_type = None):
    """
    method for generating specifc actions
    : param obj_id: id of parent stream object
    : param recipient_stream: stream to update in order to genereate an action, important to track for setting expectations

    return the new actions object that was just completed
    """
    if not obj_id:
        if recipient_stream in ["cards"]:
            obj_id = get_random_object_id(recipient_stream)

        elif recipient_stream == "boards":
            attempts = 0
            while not obj_id or (obj_id == NEVER_DELETE_BOARD_ID) and attempts < 50:
                obj_id = get_random_object_id(recipient_stream)
                attempts += 1
                
            if obj_id == NEVER_DELETE_BOARD_ID:
                logging.warn("Request Ignored |  You tried to change a board that other tests rely on |  " +\
                             "Board (id={}) should not be altered".format(NEVER_DELETE_BOARD_ID))
                raise Exception("We do not have enough boards. We are being forced to update board named 'NEVER DELETE'")

        else:
          raise NotImplementedError

    pot_fields_to_update = get_action_fields_by_stream().get(recipient_stream)
    field_to_update = pot_fields_to_update[random.randint(0, len(pot_fields_to_update) - 1)]
    
    data = stream_to_data_mapping(recipient_stream)
    if data:
        if action_type == "comment":
            data_to_update = {field_to_update: get_action_comment()}

            endpoint = BASE_URL + "/{}/{}/actions/comments".format(recipient_stream, obj_id)
            print(" * Test Data | Changing: {} ".format(data_to_update))
            resp = requests.post(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
            if resp.status_code >= 400:
                logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))

                return None

            recipient_object_id = resp.json().get('id')
            endpoint = BASE_URL + "/{}/{}/board".format(recipient_stream, obj_id)
            resp = requests.get(url=endpoint, headers=HEADERS, params=PARAMS)

            if resp.status_code >= 400:
                logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
                return None

            parent_id = resp.json().get('id')
            actions_objects = get_objects(obj_type='actions', parent_id=parent_id)

        elif not action_type and data.get(field_to_update):
            data_to_update = {field_to_update: data.get(field_to_update)} # just change the name for baords (actions)

            endpoint = get_url_string("put", recipient_stream, obj_id)
            print(" * Test Data | Changing: {} ".format(data_to_update))
            resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS, json=data_to_update)
            if resp.status_code >= 400:
                logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))

                return None

            recipient_object_id = resp.json().get('id')
            actions_objects = get_objects(obj_type='actions', parent_id=recipient_object_id)

        else:
            raise NotImplementedError

        return actions_objects[0]

    raise NotImplementedError

def create_object_boards(obj_type: str='boards'):
    global PARENT_OBJECTS
    print(" * Test Data | Request: POST on /{}/".format(obj_type))

    data = stream_to_data_mapping(obj_type)
    if data:
        endpoint = get_url_string("post", obj_type)
        resp = requests.post(url=endpoint, headers=HEADERS, params=PARAMS, json=data)

        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return None

        print(" * Test Data | Tracking {} object in PARENT_OBJECTS".format(obj_type))
        PARENT_OBJECTS.append(resp.json())

        return resp.json()

    raise NotImplementedError

def create_object_lists(obj_type: str='lists', parent_id=''):
    print(" * Test Data | Request: POST on /{}/".format(obj_type))

    data = stream_to_data_mapping(obj_type)
    if data:
        if parent_id: # if not specified it will grab a random parent_id
            data['idBoard'] = parent_id

        endpoint = get_url_string("post", obj_type)
        resp = requests.post(url=endpoint, headers=HEADERS, params=PARAMS, json=data)

        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return None

        return resp.json()

    raise NotImplementedError

def create_object_cards(obj_type: str = 'cards', parent_id = '', custom = False, field_type = None):
    print(" * Test Data | Request: POST on /{}/".format(obj_type))

    data = stream_to_data_mapping(obj_type)
    if data:
        if custom:
            if field_type:
                cfield = get_specific_custom_field_value(parent_id=parent_id, field_type=field_type)
                field = field_type
                value = cfield
            else:
                field, value = get_random_custom_field_and_value(parent_id=parent_id)
            data[field] = value

        endpoint = get_url_string("post", obj_type)
        resp = requests.post(url=endpoint, headers=HEADERS, params=PARAMS, json=data)

        if resp.status_code >= 400:
            logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
            return None

        return resp.json()

    raise NotImplementedError

def create_object(obj_type, obj_id: str = "", parent_id: str = "",
                  action_type = None, custom = False, field_type = None):
    """
    Create a single record for a given object
    : param action_type: type of aciton to generate, default None will result in board name change
    : param custom: used for generating an object with a custom field
    : param field_type: if custom=True pass in

    Creates are not available for:
    : actions: we will call update for baords object instea
    : users: we will call update the user instead (add them to a baord)n

    return that object or none if create fails
    """
    if obj_type == 'actions':
        print(" * Test Data | DIRECT CREATES ARE UNAVAILABLE for {}. ".format(obj_type) +\
              "UPDATING another stream to generate new record")
        if action_type:
            return create_object_actions('cards', obj_id=parent_id, action_type=action_type)

        return create_object_actions('boards', obj_id=parent_id, action_type=action_type)

    elif obj_type == 'boards':
        return create_object_boards()

    elif obj_type == 'cards':
        if not custom and field_type:
            raise Exception("You cannot specify a field type without the custom flag set to true.")
        return create_object_cards(parent_id=parent_id, custom=custom, field_type=field_type)

    elif obj_type == 'lists':
        return create_object_lists(parent_id=parent_id)

    elif obj_type == 'users':
        print(" * Test Data | CREATES ARE UNAVAILABLE for {}".format(obj_type))
        return update_object_user(obj_id=obj_id, parent_id=parent_id,)

    print(" * Test Data | Request: POST on /{}/".format(obj_type))

    data = stream_to_data_mapping(obj_type)

    if data:

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

    # Don't delete that one board we don't want to delete because all users are on it
    if not obj_id:
        attempts = 0
        while not obj_id or (obj_id == NEVER_DELETE_BOARD_ID and attempts < 50):
            obj_id = get_random_object_id(obj_type)
            attempts += 1

    if obj_id == NEVER_DELETE_BOARD_ID:
        logging.warn("Request Ignored |  You tried to delete a board that other tests rely on |  " +\
                     "Board (id={}) should not be deleted".format(NEVER_DELETE_BOARD_ID))
        return None

    endpoint = get_url_string("delete", obj_type, obj_id, parent_id)
    resp = requests.delete(url=endpoint, headers=HEADERS, params=PARAMS)
    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None

    #trim_parent_objects(obj_id) # we want to track when a PARENT OBJECT has been removed
    sleep(5)
    return resp.json()

def reset_tracked_parent_objects():  # TODO Reset all tracked data if we end up tracking child streams
    global PARENT_OBJECTS
    PARENT_OBJECTS = ""
    print(" * Test Data | RESETTING TRACKED PARENT OBJECTS")
    return
    
##########################################################################
### Methods for custom fields
##########################################################################
def create_custom_field(obj_type: str = "boards", obj_id : str = "", field_type : str = "checkbox"):
    """Create a new Custom Field on a board."""
    global tstamp
    tstamp = dt.utcnow().timestamp() # this is used to genereate unique data

    if not field_type in ["checkbox", "list", "number", "text", "date"]:
        raise NotImplementedError

    while not obj_id :#or obj_id == NEVER_DELETE_BOARD_ID:
        obj_id = get_random_object_id('boards')

    endpoint = BASE_URL + "/customFields"
    data = {
        "idModel": "{}".format(obj_id),  # REQUIRED Pattern: ^[0-9a-fA-F]{32}$
        "modelType": "board",  # REQUIRED type of model that the Custom Field is being defined on. This should always be board.
        "name": "Custom {} {}".format(field_type, tstamp),  # REQUIRED The name of the Custom Field
        "type": "{}".format(field_type),  # REQUIRED The type of Custom Field to create. Valid values: checkbox, list, number, text, date
        "pos": "{}".format(random.randint(1,9999)),  # REQUIRED oneOf [string, number]
        "display_cardFront": True,  # Whether this Custom Field should be shown on the front of Cards
    }

    if field_type =="list":
        data['options'] = []
        for i in range(random.randint(1,5)):
            tstamp = dt.utcnow().timestamp()
            data['options'].append(
                {"value": {"text":"Option {} {}".format(i, tstamp)},
                 "color":"{}".format(random.choice(["blue", "orange", "green", "red", "purple", "pink"])),
                 "pos": "{}".format(random.randint(1,9999))},
            )

    print(" * Test Data | Creating: {} on board {}".format(data.get('name'), obj_id))
    resp = requests.post(url=endpoint, headers=HEADERS, params=PARAMS, json=data)
    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None
    return resp.json()

# def assign_custom_field(obj_type, obj_id, field_id, value):
#     """A field can have up to 50 custom fields"""

#     print(" * Test Data | Update: use customField {} on {} {}".format(field_id, obj_type, obj_id))
#     endpoint = BASE_URL + "/{}/{}/customField/{}/{}".format(obj_type, obj_id, field_id, value)
#     resp = requests.put(url=endpoint, headers=HEADERS, params=PARAMS)

#     if resp.status_code >= 400:
#         logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
#         return None

#     return resp.json()

def get_custom_fields(obj_type, obj_id):
    print(" * Test Data | GET: get customFieldItems on {} {}".format(obj_type, obj_id))
    endpoint = BASE_URL + "/{}/{}/customFields".format(obj_type, obj_id)
    # parameters = PARAMS + (("customFieldItems", True),)
    resp = requests.get(url=endpoint, headers=HEADERS, params=PARAMS)

    if resp.status_code >= 400:
        logging.warn("Request Failed {} \n    {}".format(resp.status_code, resp.text))
        return None

    return resp.json()

# def get_random_custom_field_and_value(obj_type="boards", parent_id=""):
#     global tstamp
#     tstamp = dt.utcnow().timestamp()

#     custom_field_data_map = {
#         "checkbox": "checkbox value {}".format(tstamp),
#         "date": "{}".format(dt.strftime(dt.utcnow(), DATE_FORMAT)),
#         "list": [],
#         "number": "{}".format(random.randint(0,9999)),
#         "text": "text value {}".format(tstamp),
#     }

#     cfields = get_custom_fields(obj_type, parent_id)
#     if not cfields:
#         raise NotImplementedError("Custom fields have not been created for this board")

#     random_cfield = cfields[random.randint(0, len(cfields) -1)]
#     field_type = random_cfield['type']
#     value = custom_field_data_map[field_type]
#     if field_type == 'list':
#         value = random.choice(random_cfield['options'])

#     return field_type, value

# def get_specific_custom_field_value(obj_type="boards", parent_id="", field_type=""):
#     global tstamp
#     tstamp = dt.utcnow().timestamp()

#     if not field_type:
#         raise Exception("You did not pass in a field type.")

#     cfields = get_custom_fields(obj_type, parent_id)
#     if not cfields:
#         raise NotImplementedError("This board has no custom fields.")

#     specific_cfields = [field for field in cfields if field.get('type') == field_type]
#     if not specific_cfields:
#         print(" * Test Data | Board does not have custom field of type: {} ".format(field_type) +\
#               " creating one now.")
#         specific_cfields = list(create_custom_field(obj_id=parent_id, field_type=field_type))

#     chosen_cfield = random.choice(specific_cfields)

#     # if field_type == 'list':
#     #     return random.choice(chosen_cfield['options'])

#     return chosen_cfield

##########################################################################
### Testing the utils above
##########################################################################
if __name__ == "__main__":
    test_creates = False
    test_updates = False
    test_gets = False
    test_deletes = False

    print_objects = True

    objects_to_test = ['boards'] # ['actions', 'boards', 'cards', 'lists', 'users', 'checklists']

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
    if test_deletes:
        for obj in objects_to_test:
            print("Testing DELETE: {}".format(obj))
            deleted_obj = delete_object(obj)
            if deleted_obj:
                print("SUCCESS")
                if print_objects:
                    print(deleted_obj)
                continue
            print("FAILED")
