from tap_trello.streams.abstracts import FullTableStream

class CardCustomFieldItems(FullTableStream):
    tap_stream_id = "card_custom_field_items"
    key_properties = ["id", "card_id"]
    replication_method = "FULL_TABLE"
    path = "/cards/{id}/customFieldItems"
    parent = "cards"

    def modify_object(self, record, parent_record=None):
        """Add card_id to card custom field item records."""
        if parent_record and 'id' in parent_record:
            record["card_id"] = parent_record['id']
        return record
