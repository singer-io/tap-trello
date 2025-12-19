from tap_trello.streams.abstracts import FullTableStream

class OrganizationMemberships(FullTableStream):
    tap_stream_id = "organization_memberships"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/organizations/{id}/memberships"
    parent = "organizations"

