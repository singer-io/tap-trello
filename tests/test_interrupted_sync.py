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
            "organization_actions",  # stream doesn't have enough test data
            "organization_members",
            "organization_memberships",
            "users"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        return {
            "currently_syncing": "boards",
            "bookmarks": {
                "actions": {"window_start": "2026-01-01T11:06:02.764417Z"}
            }
        }

    # NOTE: Overriding the test from the base class to handle full-table parent stream boards.
    # The boards stream doesn't have any replication keys (being full-table).
    # So skipping few assertions for that stream.s
    def test_full_replication_streams(self):
        """Verify full replication streams don't have bookmarks and sync as normal"""
        full_streams = {s for s, m in self.expected_replication_method().items()
                        if m == self.FULL_TABLE}
        for stream in self.streams_to_test().intersection(full_streams):
            with self.subTest(stream=stream):

                # gather expectations and results
                # ######### Assertion commented out for boards stream ############
                # expected_replication_key = self.expected_replication_keys(stream)
                # Make sure this is not a compound replication key
                # assert len(expected_replication_key) == 1
                # expected_replication_key = next(iter(expected_replication_key))
                # expected_replication_method = self.expected_replication_method(stream)

                first_sync_records = [
                    record['data'] for record in
                    self.first_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']
                resuming_sync_records = [
                    record['data'] for record in
                    self.resuming_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                # Verify full table streams do not save bookmarked values
                self.assertNotIn(stream, self.first_sync_state['bookmarks'].keys())
                self.assertNotIn(stream, self.resuming_sync_state['bookmarks'].keys())

                # remove any records that got added after the first sync
                # filtered_resuming_sync_records = [
                #     record for record in resuming_sync_records
                #     if self.parse_date(record[expected_replication_key]) <=
                #     self.parse_date(self.get_bookmark_value(self.first_sync_state, stream))]
                # self.assertEqual(len(first_sync_records), len(filtered_resuming_sync_records))

                # Verify first and second sync have the same records
                self.assertEqual(first_sync_records, resuming_sync_records,
                                 msg="There is a difference in records between syncs")
