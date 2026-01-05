from tap_trello.streams.abstracts import ChildBaseStream

class OrganizationActions(ChildBaseStream):
    tap_stream_id = "organization_actions"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date"]
    path = "/organizations/{id}/actions"
    parent = "organizations"
    bookmark_value = None
