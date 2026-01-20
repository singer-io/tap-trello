from base import TrelloBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {

}


class TrelloAllFields(AllFieldsTest, TrelloBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_trello_all_fields_test"

    def streams_to_test(self):
        # Exclude streams with insufficient records to test all fields
        streams_to_exclude = {
            "board_memberships",
            "checklists",
            "members",
            "organizations",
            "organization_actions",
            "organization_members",
            "organization_memberships",
            "users"
        }
        return self.expected_stream_names().difference(streams_to_exclude)
