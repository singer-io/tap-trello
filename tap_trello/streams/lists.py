from tap_trello.streams.abstracts import Unsortable, ChildStream
from tap_trello.streams.boards import Boards


class Lists(Unsortable, ChildStream):
    # TODO: If a list is added to a board, does the board's dateLastActivity get updated?
    stream_id = "lists"
    stream_name = "lists"
    endpoint = "/boards/{}/lists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent = Boards
