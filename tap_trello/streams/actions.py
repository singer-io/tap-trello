from tap_trello.streams.abstracts import DateWindowPaginated, ChildStream


class Actions(DateWindowPaginated, ChildStream):
    stream_id = "actions"
    stream_name = "actions"
    endpoint = "/boards/{}/actions"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date"]
    parent = "boards"
    MAX_API_RESPONSE_SIZE = 1000
    params = {'limit': 1000}
