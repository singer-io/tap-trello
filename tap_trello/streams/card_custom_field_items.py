from tap_trello.streams.abstracts import FullTableStream

class CardCustomFieldItems(FullTableStream):
    tap_stream_id = "card_custom_field_items"
    key_properties = ["id", "card_id"]
    replication_method = "FULL_TABLE"
    path = "/cards/{id}/customFieldItems"
    parent = "cards"
