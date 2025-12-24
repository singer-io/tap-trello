from tap_trello.streams.abstracts import DateWindowPaginated, ChildStream
from tap_trello.streams.boards import Boards


class Actions(DateWindowPaginated, ChildStream):
    # TODO: If an action is completed on a board, does the board's dateLastActivity get updated?
    stream_id = "actions"
    stream_name = "actions"
    endpoint = "/boards/{}/actions"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date"]
    parent = Boards
    MAX_API_RESPONSE_SIZE = 1000
    params = {'limit': 1000}
