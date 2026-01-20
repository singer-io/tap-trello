from base import TrelloBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class TrelloStartDateTest(StartDateTest, TrelloBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_trello_start_date_test"

    def streams_to_test(self):
        # Exclude streams with insufficient records to test pagination
        streams_to_exclude = {
            "board_memberships",
            "board_custom_fields",
            "board_labels",
            "card_attachments",
            "card_custom_field_items",
            "checklists",
            "lists",
            "members",
            "organizations",
            "organization_actions",
            "organization_members",
            "organization_memberships",
            "users"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2015-03-25T00:00:00Z"
    @property
    def start_date_2(self):
        return "2017-01-25T00:00:00Z"
