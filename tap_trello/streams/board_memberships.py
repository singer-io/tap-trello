from tap_trello.streams.abstracts import FullTableStream

class BoardMemberships(FullTableStream):
    tap_stream_id = "board_memberships"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/boards/{id}/memberships"
    parent = "boards"

