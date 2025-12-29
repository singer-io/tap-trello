from tap_trello.streams.abstracts import FullTableStream

class OrganizationMembers(FullTableStream):
    tap_stream_id = "organization_members"
    key_properties = ["id", "organization_id"]
    replication_method = "FULL_TABLE"
    path = "/organizations/{id}/members"
    parent = "organizations"
