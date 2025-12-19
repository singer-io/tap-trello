from tap_trello.streams.abstracts import FullTableStream

class Organizations(FullTableStream):
    tap_stream_id = "organizations"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/organizations/{id}"

