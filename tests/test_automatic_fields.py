"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import TrelloBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class TrelloAutomaticFields(MinimumSelectionTest, TrelloBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_trello_automatic_fields_test"

    def streams_to_test(self):
        # Exclude streams with insufficient records to test automatic fields
        streams_to_exclude = {
            "board_memberships",
            "board_custom_fields",
            "card_custom_field_items",
            "members",
            "organizations",
            "organization_actions",
            "organization_members",
            "organization_memberships",
            "users"
        }
        return self.expected_stream_names().difference(streams_to_exclude)
