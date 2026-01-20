from base import TrelloBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class TrelloInterruptedSyncTest(InterruptedSyncTest, TrelloBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_trello_interrupted_sync_test"

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


    def manipulate_state(self):
        return {
            "currently_syncing": "boards",
            "bookmarks": {
                "actions": { "date" : "2026-01-01T00:00:00Z"},
        }
    }
