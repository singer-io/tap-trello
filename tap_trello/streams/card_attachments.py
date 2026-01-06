from tap_trello.streams.abstracts import FullTableStream

class CardAttachments(FullTableStream):
    tap_stream_id = "card_attachments"
    key_properties = ["id", "card_id"]
    replication_method = "FULL_TABLE"
    path = "/cards/{id}/attachments"
    parent = "cards"

    def modify_object(self, record, parent_record=None):
        """Add card_id to card attachment records."""
        if parent_record and 'id' in parent_record:
            record["card_id"] = parent_record['id']
        return record
