from tap_trello.streams.abstracts import FullTableStream

class BoardCustomFields(FullTableStream):
    tap_stream_id = "board_custom_fields"
    key_properties = ["id", "boardId"]
    replication_method = "FULL_TABLE"
    path = "/boards/{id}/customFields"
    parent = "boards"

    def modify_object(self, record, parent_record=None):
        """Add boardId to board custom field records."""
        if parent_record and 'id' in parent_record:
            record["boardId"] = parent_record['id']
        return record
