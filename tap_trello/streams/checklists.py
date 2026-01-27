from tap_trello.streams.abstracts import ChildStream


class Checklists(ChildStream):
    stream_id = "checklists"
    stream_name = "checklists"
    endpoint = "/boards/{}/checklists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent = "boards"
    params = {'fields': 'all', 'checkItem_fields': 'all'}
