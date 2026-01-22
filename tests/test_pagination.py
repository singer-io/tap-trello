from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import TrelloBaseTest


class TrelloPaginationTest(PaginationTest, TrelloBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_trello_pagination_test"

    def streams_to_test(self):
        # Exclude streams with insufficient records to test pagination
        streams_to_exclude = {
            "board_memberships",
            "board_custom_fields",
            "board_labels",
            "card_attachments",
            "card_custom_field_items",
            "checklists",
            "members",
            "organizations",
            "organization_actions",
            "organization_members",
            "organization_memberships",
            "users"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def get_properties(self, original: bool = True):
        """Configuration with reduced page_size to test pagination logic."""
        return {
            "start_date": self.start_date,
            "page_size": 4
        }

    def expected_page_size(self, stream):
        """
        Return the expected page size for pagination testing.

        Overrides the default API_LIMIT to use the configured page_size.
        This allows pagination testing with smaller datasets by setting
        a lower page limit than the API default (100).
        """
        return 4
