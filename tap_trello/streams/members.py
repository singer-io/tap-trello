from tap_trello.streams.abstracts import FullTableStream

class Members(FullTableStream):
    tap_stream_id = "members"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/members/{id}"
    parent = "boards"

