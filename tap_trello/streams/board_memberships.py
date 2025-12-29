from tap_trello.streams.abstracts import AddBoardId, FullTableStream

class BoardMemberships(FullTableStream, AddBoardId):
    tap_stream_id = "board_memberships"
    key_properties = ["id", "boardId"]
    replication_method = "FULL_TABLE"
    path = "/boards/{id}/memberships"
    parent = "boards"
