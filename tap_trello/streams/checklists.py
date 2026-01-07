from tap_trello.streams.abstracts import ChildStream
from tap_trello.streams.boards import Boards


class Checklists(ChildStream):
    stream_id = "checklists"
    stream_name = "checklists"
    endpoint = "/boards/{}/checklists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent = Boards
    params = {'fields': 'all', 'checkItem_fields': 'all'}
