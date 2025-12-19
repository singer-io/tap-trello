from tap_trello.streams.abstracts import FullTableStream

class BoardCustomFields(FullTableStream):
    tap_stream_id = "board_custom_fields"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/boards/{id}/customFields"
    parent = "boards"

