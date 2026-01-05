from tap_trello.streams.abstracts import FullTableStream

class CardAttachments(FullTableStream):
    tap_stream_id = "card_attachments"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/cards/{id}/attachments"
    parent = "cards"
