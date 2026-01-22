from base import TrelloBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest
from tap_tester.logger import LOGGER


class TrelloStartDateTest(StartDateTest, TrelloBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_trello_start_date_test"

    def streams_to_test(self):
        # Exclude streams with insufficient records
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
            "users",
            "cards",  # Full table stream
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2026-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2026-01-20T00:00:00Z"

    # NOTE: Overriding the test from the base class to handle full-table parent stream boards.
    # This stream is needed to replicate the incremental child stream actions
    def test_replication_key_values(self):

        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                if self.expected_replication_method(stream) != self.INCREMENTAL:
                    LOGGER.info(f"Stream {stream} is not incremental. Skipping stream...")
                    continue

                # expected values
                # TODO - is the look back supposed to affect the start date?
                stream_obeys_start_date = self.expected_start_date_behavior(stream)

                # collect information specific to incremental streams from syncs 1 & 2
                # compound replication key not accounted for
                expected_replication_key = self.expected_replication_keys(stream)
                assert len(expected_replication_key) == 1
                expected_replication_key = next(iter(expected_replication_key))

                replication_dates_1 = {
                    record['data'].get(expected_replication_key) for record in
                    StartDateTest.synced_messages_by_stream_1.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}
                replication_dates_2 = {
                    record['data'].get(expected_replication_key) for record in
                    StartDateTest.synced_messages_by_stream_2.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}

                if stream_obeys_start_date:

                    # Verify replication key is greater or equal to start_date for sync 1
                    for replication_date in replication_dates_1:
                        with self.subTest(sync="sync1", replication_date=replication_date):
                            self.assertGreaterEqual(
                                self.parse_date(replication_date),
                                self.parse_date(self.start_date_1),
                                msg=f"Record for a date prior to our start date.\n"
                                    f"Sync start_date: {self.start_date_1}\n"
                                    f"Record date: {replication_date}")

                    # Verify replication key is greater or equal to start_date for sync 2
                    for replication_date in replication_dates_2:
                        with self.subTest(sync="sync2", replication_date=replication_date):
                            self.assertGreaterEqual(
                                self.parse_date(replication_date),
                                self.parse_date(self.start_date_2),
                                msg=f"Report pertains to a date prior to our start date.\n"
                                    f"Sync start_date: {self.start_date_2}\n"
                                    f"Record date: {replication_date}")
                else:
                    # Test streams that do not obey start date
                    oldest_sync_1_date = min(self.parse_date(rd) for rd in replication_dates_1)
                    oldest_sync_2_date = min(self.parse_date(rd) for rd in replication_dates_2)

                    # oldest record for each sync has the same replication_date
                    self.assertEqual(oldest_sync_1_date, oldest_sync_2_date,
                                     msg=f"Stream {stream} does not obey start date. "
                                         f"Both syncs are expected to start at the same date "
                                         f"and have the same oldest record")

                    # oldest record for sync 2 is less than start date 2
                    self.assertLess(oldest_sync_2_date, self.parse_date(self.start_date_2),
                                    msg=f"Oldest record in sync 2: {oldest_sync_2_date} "
                                        f"is not older than start date: {self.start_date_2}")

    # NOTE: Overriding the test from the base class to handle full-table parent stream boards.
    # This stream is needed to replicate the incremental child stream actions
    def test_replicated_records(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                if self.expected_replication_method(stream) != self.INCREMENTAL:
                    LOGGER.info(f"Stream {stream} is not incremental. Skipping stream...")
                    continue

                # expected values
                expected_primary_keys = self.expected_primary_keys(stream)
                stream_obeys_start_date = self.expected_start_date_behavior(stream)

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)

                # collect information to allow filtering
                # of any records added between syncs for clean comparison

                # compound replication key not accounted for
                assert len(self.expected_replication_keys().get(stream)) == 1
                expected_replication_key = next(iter(self.expected_replication_keys().get(stream)))
                replication_dates_1 = {
                    record['data'].get(expected_replication_key) for record in
                    StartDateTest.synced_messages_by_stream_1.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}

                # all pks in sync 2 except those added after sync 1 was completed
                primary_keys_sync_2 = {
                    tuple(message['data'][expected_pk] for expected_pk in expected_primary_keys)
                    for message in
                    StartDateTest.synced_messages_by_stream_2.get(stream, {}).get('messages', [])
                    if message.get('action') == 'upsert'
                    and self.parse_date(message['data'][expected_replication_key])
                    <= self.parse_date(max(replication_dates_1))}

                if stream_obeys_start_date:

                    # all pks in sync 1 that should have been synced in sync 2
                    primary_keys_sync_1 = {
                        tuple(message['data'][expected_pk] for expected_pk in expected_primary_keys)
                        for message in StartDateTest.synced_messages_by_stream_1.get(
                            stream, {}).get('messages', [])
                        if message.get('action') == 'upsert'
                        and self.parse_date(message['data'][expected_replication_key])
                        >= self.parse_date(self.start_date_2)}

                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2.
                    # NOTE: The start date must be far enough back to account for
                    #   new records between sync 1 and sync 2
                    self.assertGreater(record_count_sync_1, record_count_sync_2)

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)

                else:

                    # all pks in sync 1
                    primary_keys_sync_1 = {
                        tuple(message['data'][expected_pk] for expected_pk in expected_primary_keys)
                        for message in StartDateTest.synced_messages_by_stream_1.get(
                            stream, {}).get('messages', [])
                        if message.get('action') == 'upsert'}

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
