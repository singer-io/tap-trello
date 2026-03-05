from tap_trello.streams.abstracts import Unsortable, ChildStream


class Lists(Unsortable, ChildStream):
    stream_id = "lists"
    stream_name = "lists"
    endpoint = "/boards/{}/lists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent = "boards"
