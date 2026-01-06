from tap_trello.streams.abstracts import FullTableStream

class BoardLabels(FullTableStream):
    tap_stream_id = "board_labels"
    key_properties = ["id", "boardId"]
    replication_method = "FULL_TABLE"
    path = "/boards/{id}/labels"
    parent = "boards"

    def modify_object(self, record, parent_record=None):
        """Add boardId to board label records."""
        if parent_record and 'id' in parent_record:
            record["boardId"] = parent_record['id']
        return record
