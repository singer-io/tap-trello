from tap_trello.streams.abstracts import FullTableStream

class OrganizationMembers(FullTableStream):
    tap_stream_id = "organization_members"
    key_properties = ["id", "organization_id"]
    replication_method = "FULL_TABLE"
    path = "/organizations/{id}/members"
    parent = "organizations"

    def modify_object(self, record, parent_record=None):
        """Add organization_id to organization member records."""
        if parent_record and 'id' in parent_record:
            record["organization_id"] = parent_record['id']
        return record
