from tap_trello.streams.abstracts import Unsortable, AddBoardId, ChildStream
from tap_trello.streams.boards import Boards


class Users(Unsortable, AddBoardId, ChildStream):
    # TODO: If a user is added to a board, does the board's dateLastActivity get updated?
    # TODO: Should this assoc the board_id to the user records? Seems pretty useless without it
    stream_id = "users"
    stream_name = "users"
    endpoint = "/boards/{}/members"
    key_properties = ["id", "boardId"]
    replication_method = "FULL_TABLE"
    parent = Boards
