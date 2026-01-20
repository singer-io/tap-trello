from base import TrelloBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class TrelloBookMarkTest(BookmarkTest, TrelloBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "actions": { "date" : "2026-01-01T00:00:00Z"}
        }
    }
    @staticmethod
    def name():
        return "tap_tester_trello_bookmark_test"

    def streams_to_test(self):
        # Excluding below streams, as these streams use full table replication
        streams_to_exclude = {
            "board_memberships",
            "board_custom_fields",
            "board_labels",
            "cards",
            "card_attachments",
            "card_custom_field_items",
            "checklists",
            "lists",
            "members",
            "organizations",
            "organization_actions", # stream doesn't have enough test data
            "organization_members",
            "organization_memberships",
            "users"
        }
        return self.expected_stream_names().difference(streams_to_exclude)
