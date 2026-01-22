from base import TrelloBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class TrelloBookMarkTest(BookmarkTest, TrelloBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "actions": {"date": "2026-01-20T14:51:29.834000Z"}
        }
    }

    full_table_parent_stream = {"boards"}

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
            "organization_actions",  # stream doesn't have enough test data
            "organization_members",
            "organization_memberships",
            "users"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "actions": {"window_start": "2026-01-20T14:51:29.834000Z"}
        }

        return new_bookmarks

    # NOTE: Overriding the test from the base class to handle full-table parent stream boards.
    def test_syncs_were_successful(self):
        with self.subTest(msg="validating syncs have correct data"):
            # Verify sync is not interrupted by checking currently_syncing in state for sync 1
            self.assertIsNone(self.state_1.get("currently_syncing"))
            # Verify state is saved
            self.assertIsNotNone(self.state_1)

            # Verify sync is not interrupted by checking currently_syncing in state for sync 2
            self.assertIsNone(self.state_2.get("currently_syncing"))
            # Verify bookmarks are saved
            self.assertIsNotNone(self.state_2)

            # Verify ONLY streams under test have bookmark entries in state for sync 1
            # NOTE: full table parent stream boards excluded from this check
            unexpected_streams_1 = {
                self.get_stream_name(stream_id) for stream_id
                in self.state_1.get("bookmarks", {}).keys()
                if self.get_stream_name(stream_id) not in self.streams_to_test().difference(self.full_table_parent_stream)}
            bookmarked_streams_1 = {
                self.get_stream_name(stream_id) for stream_id
                in self.state_1.get("bookmarks", {}).keys()}
            self.assertSetEqual(set(), unexpected_streams_1)
            self.assertSetEqual(bookmarked_streams_1, self.streams_to_test().difference(self.full_table_parent_stream))

            # Verify ONLY streams under test have bookmark entries in state for sync 2
            # NOTE: full table parent stream boards excluded from this check
            unexpected_streams_2 = {
                self.get_stream_name(stream_id) for stream_id
                in self.state_2.get("bookmarks", {}).keys()
                if self.get_stream_name(stream_id) not in self.streams_to_test().difference(self.full_table_parent_stream)}
            bookmarked_streams_2 = {
                self.get_stream_name(stream_id) for stream_id
                in self.state_2.get("bookmarks", {}).keys()}
            self.assertSetEqual(set(), unexpected_streams_2)
            self.assertSetEqual(bookmarked_streams_2, self.streams_to_test().difference(self.full_table_parent_stream))