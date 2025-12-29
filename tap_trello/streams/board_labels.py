from tap_trello.streams.abstracts import FullTableStream

class BoardLabels(FullTableStream):
    tap_stream_id = "board_labels"
    key_properties = ["id", "boardId"]
    replication_method = "FULL_TABLE"
    path = "/boards/{id}/labels"
    parent = "boards"
