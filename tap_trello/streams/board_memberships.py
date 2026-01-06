from tap_trello.streams.abstracts import FullTableStream

class BoardMemberships(FullTableStream):
    tap_stream_id = "board_memberships"
    key_properties = ["id", "boardId"]
    replication_method = "FULL_TABLE"
    path = "/boards/{id}/memberships"
    parent = "boards"

    def modify_object(self, record, parent_record=None):
        """Add boardId to board membership records."""
        if parent_record and 'id' in parent_record:
            record["boardId"] = parent_record['id']
        return record
