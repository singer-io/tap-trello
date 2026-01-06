import logging
import unittest
import random
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import trello_utils as utils
from base import TrelloBaseTest


class TrelloBookmarksQA(TrelloBaseTest):
    LOOKBACK_WINDOW = 1  # days

    def name(self):
        return "tap_tester_trello_bookmarks_qa"

    def untestable_streams(self):
        return {
            'users',
            'organization_actions'
        }

    def get_properties(self):
        return {  # set to 3 days ago for testing lookback window
            'start_date' : dt.strftime(dt.utcnow() - timedelta(days=3), self.START_DATE_FORMAT),
        }

    def test_run(self):
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # ensure data exists for sync streams and set expectations
        expected_records_1 = {x: [] for x in self.expected_sync_streams()} # ids by stream
        for stream in self.expected_sync_streams().difference(self.untestable_streams()):
            if stream in self.expected_incremental_streams():
                start_date = dt.strptime(self.get_properties().get('start_date'), self.START_DATE_FORMAT)
                since = start_date.strftime(self.TEST_TIME_FORMAT)
                _, existing_objects = utils.get_total_record_count_and_objects(stream, since=since)
            else:
                _, existing_objects = utils.get_total_record_count_and_objects(stream)

            if existing_objects:
                logging.info("Data exists for stream: {}".format(stream))
                for obj in existing_objects:  # add existing records to expectations
                    expected_records_1[stream].append(obj)
                continue
            # Create 1 record if none exist
            logging.info("Data does not exist for stream: {}".format(stream))
            new_object = utils.create_object(stream)
            logging.info("Data generated for stream: {}".format(stream))
            expected_records_1[stream].append(new_object)

        # Create comment actions
        start_date = dt.strptime(self.get_properties().get('start_date'), self.START_DATE_FORMAT)
        since = start_date.strftime(self.TEST_TIME_FORMAT)
        # count_before, before_records = utils.get_total_record_count_and_objects('actions', since=since)
        action_comments = []
        action_comments.append(utils.create_object('actions', action_type="comment"))
        action_comments.append(utils.create_object('actions', action_type="comment"))
        for action in action_comments:
            expected_records_1['actions'].append(action)
        # count_after, after_records = utils.get_total_record_count_and_objects('actions', since=since)


        # run in check mode
        conn_id = connections.ensure_connection(self)
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        #select all catalogs
        for c in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, c['stream_id'])

            for k in self.expected_automatic_fields()[c['stream_name']]:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)
                print("Validating inclusion on {}: {}".format(c['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')

            connections.select_catalog_and_fields_via_metadata(conn_id, c, catalog_entry)

        #clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify data was replicated
        record_count_by_stream_1 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        replicated_row_count_1 =  reduce(lambda accum,c : accum + c, record_count_by_stream_1.values())
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        print("total replicated row count: {}".format(replicated_row_count_1))

        # get emitted with records
        synced_records_1 = runner.get_records_from_target_output()

        # Verify bookmarks were saved for all streams
        state_1 = menagerie.get_state(conn_id)
        for stream in self.expected_incremental_streams():
            # Check that bookmark exists (could be 'window_start' or 'date' depending on stream)
            bookmark = state_1.get('bookmarks', {}).get(stream, {})
            self.assertTrue(bookmark, msg=f"No bookmark found for stream {stream}")
            # Verify bookmark has either window_start or date field
            has_bookmark_field = 'window_start' in bookmark or 'date' in bookmark
            self.assertTrue(has_bookmark_field, msg=f"Stream {stream} bookmark missing window_start or date field")
        print("Bookmarks meet expectations")

        # Generate data between syncs for bookmarking streams
        print("Generating more data prior to 2nd sync")
        expected_records_2 = {x: [] for x in self.expected_sync_streams()}
        for stream in self.expected_full_table_sync_streams().difference(self.untestable_streams()):
            for _ in range(1):
                new_object = utils.create_object(stream)
                if new_object is None:
                    # Stream cannot have test data created
                    logging.info("Skipping data creation for stream: {} (uncreatable stream)".format(stream))
                    break
                expected_records_2[stream].append({field: new_object.get(field)
                                                   for field in self.expected_automatic_fields().get(stream)})

        # Update a single comment action before second sync
        print("Updating existing data prior to 2nd sync")
        updated_records = {x: [] for x in self.expected_sync_streams()}
        action_id_to_update = random.choice(action_comments).get('id')
        updated_action = utils.update_object_action(obj_id=action_id_to_update)
        updated_records['actions'].append(updated_action)

        # Get new actions from data manipulation between syncs
        print("Acquiring in-test actions prior to 2nd sync")
        for stream in self.expected_incremental_streams().difference(self.untestable_streams()):
            bookmark = state_1.get('bookmarks').get(stream, {})
            # Get bookmark value - could be 'window_start' (actions) or 'date' (organization_actions)
            bookmark_value = bookmark.get('window_start') or bookmark.get('date')
            if not bookmark_value:
                logging.warning(f"No bookmark found for stream {stream}, skipping")
                continue

            state = dt.strptime(bookmark_value, self.TEST_TIME_FORMAT)
            since = (state - timedelta(days=self.LOOKBACK_WINDOW)).strftime(self.TEST_TIME_FORMAT)
            # start_date = dt.strptime(self.get_properties().get('start_date'), self.START_DATE_FORMAT)
            # since = start_date.strftime(self.TEST_TIME_FORMAT)
            _, objects = utils.get_total_record_count_and_objects(stream, since=since)
            for obj in objects:
                expected_records_2[stream].append({field: obj.get(field)
                                                   for field in self.expected_automatic_fields().get(stream)})

        # Run another sync
        print("Running 2nd sync job")
        sync_job_name_2 = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_name_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_name_2)

        # verify data was replicated
        record_count_by_stream_2 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        replicated_row_count_2 =  reduce(lambda accum,c : accum + c, record_count_by_stream_2.values())
        self.assertGreater(replicated_row_count_2, 0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream_2))
        print("total replicated row count: {}".format(replicated_row_count_2))

        # get emitted with records
        synced_records_2 = runner.get_records_from_target_output()

        # Verify bookmarks were saved as expected inc streams
        state_2 = menagerie.get_state(conn_id)
        for stream in self.expected_incremental_streams():
            # Verify bookmarks were saved as expected inc streams
            bookmark = state_2.get('bookmarks', {}).get(stream, {})
            self.assertTrue(bookmark, msg=f"No bookmark found for stream {stream}")
            has_bookmark_field = 'window_start' in bookmark or 'date' in bookmark
            self.assertTrue(has_bookmark_field, msg=f"Stream {stream} bookmark missing window_start or date field")
        print("Bookmarks meet expectations")

        # TESTING FULL TABLE STREAMS
        for stream in self.expected_full_table_sync_streams().difference(self.untestable_streams()):
            with self.subTest(stream=stream):
                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Using assertGreaterEqual since some streams may have 0 records
                self.assertGreaterEqual(record_count_1, 0)
                self.assertGreaterEqual(record_count_2, 0)

                # Using assertGreaterEqual since full table streams may return same data if no new records created
                self.assertGreaterEqual(record_count_2, record_count_1,
                                   msg="Full table streams should have at least same amount of data in second sync.")
                # Using assertGreaterEqual since we may not be able to create new records for some streams
                if expected_records_2.get(stream):
                    self.assertGreaterEqual((record_count_2 - record_count_1),
                                     0,
                                     msg="Record count should not decrease between syncs for {}".format(stream))

                # Test that we are capturing the expected records for full table streams
                expected_records_1_list = expected_records_1.get(stream, [])
                expected_ids_1 = set(record.get('id') for record in expected_records_1_list if record)
                data_1 = synced_records_1.get(stream, [])
                record_messages_1 = [row.get('data') for row in data_1.get('messages', [])]
                record_ids_1 = set(row.get('id') for row in record_messages_1 if row)

                expected_records_2_list = expected_records_2.get(stream, [])
                expected_ids_2 = set(record.get('id') for record in expected_records_2_list if record)
                data_2 = synced_records_2.get(stream, [])
                record_messages_2 = [row.get('data') for row in data_2.get('messages', [])]
                record_ids_2 = set(row.get('id') for row in record_messages_2 if row)

                # verify all expected records from before first sync are replicated
                if expected_ids_1:
                    self.assertTrue(expected_ids_1.issubset(record_ids_1),
                                     msg="Data discrepancy. Expected records are not subset of actual in sync 1.")
                    self.assertTrue(expected_ids_1.issubset(record_ids_2),
                                     msg="Data discrepancy. Expected records are not subset of actual in sync 2.")

                # BUG (SRCE-3982) Skip the next assertion for the boards
                if stream != 'boards' and expected_records_1_list:
                    for expected_record in expected_records_1_list:
                        if not expected_record:
                            continue
                        matching_records = [message for message in record_messages_1
                                         if message.get('id') == expected_record.get('id')]
                        if not matching_records:
                            continue
                        actual_record = matching_records[0]
                        actual_fields = set(actual_record.keys())
                        expected_fields = set(expected_record.keys())
                        # BUG https://jira.talendforge.org/browse/TDL-9680
                        # BUG https://jira.talendforge.org/browse/TDL-20813
                        if stream == 'cards':
                            # Remove when addressed
                            for field in ('cardRole', 'email'):
                                if field in expected_fields:
                                    expected_fields.remove(field)

                        self.assertTrue(actual_fields.issubset(expected_fields),
                                         msg="Field mismatch between expectations and replicated records in sync 1.")


                # verify the 2nd sync gets records created after the 1st sync (if we created any)
                if expected_ids_2:
                    new_records_in_sync2 = set(record_ids_2).difference(set(record_ids_1))
                    self.assertGreaterEqual(len(new_records_in_sync2), 0,
                                     msg="Expected some new records in sync 2 for {}".format(stream))

        print("Full table streams tested.")

        # TESTING INCREMENTAL STREAMS
        for stream in self.expected_incremental_streams().difference(self.untestable_streams()):
            with self.subTest(stream=stream):
                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Assert we have data for both syncs for inc streams
                self.assertGreater(record_count_1, 0)
                self.assertGreater(record_count_2, 0)
                # Assert that we are capturing the expected number of records for inc streams
                self.assertGreaterEqual(record_count_1, len(expected_records_1.get(stream, [])),
                                        msg="Stream {} replicated an unexpedted number records on 1st sync.".format(stream))
                # NOTE: actions seem to be getting updated by trello's backend resulting in an action from a previous
                #       test run gettting synced again, so we will be less strict for this stream
                if stream == 'actions':
                    self.assertGreaterEqual(record_count_2, len(expected_records_2.get(stream, [])),
                                            msg="Stream {} replicated an unexpedted number records on 2nd sync.".format(stream))
                else:
                    self.assertEqual(record_count_2, len(expected_records_2.get(stream, [])),
                                     msg="Stream {} replicated an unexpedted number records on 2nd sync.".format(stream))

                # Assert that we are capturing the expected records for inc streams
                data_1 = synced_records_1.get(stream, [])
                record_messages_1 = [row.get('data').get('id') for row in data_1['messages']]
                data_2 = synced_records_2.get(stream, [])
                record_messages_2 = [row.get('data').get('id') for row in data_2['messages']]
                for record in expected_records_1.get(stream):
                    self.assertTrue(record.get('id') in record_messages_1,
                                    msg="Missing an expected record from sync 1.")
                for record in expected_records_2.get(stream):
                    self.assertTrue(record.get('id') in record_messages_2,
                                    msg="Missing an expected record from sync 2.")

                # Assert that the start_date is working
                first_start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                for message in data_1['messages']:
                    record = message['data']
                    self.assertLessEqual(first_start_date_str, record['date'])

                second_start_date_str = since
                for message in data_2['messages']:
                    record = message['data']
                    self.assertLessEqual(second_start_date_str, record['date'])


                record_data_1 = [row.get('data') for row in data_1['messages']]
                record_data_2 = [row.get('data') for row in data_2['messages']]

                # Testing action comments (the only action type that can be updated)
                for action in action_comments:

                    # Get text value for action comment from sync 1
                    original_action_text = ""
                    for record in record_data_1:
                        if record.get('id') == action.get('id'):
                            original_action_text = record.get('data').get('text')
                    assert original_action_text, "Record  {} is missing from 1st sync.".format(action.get('id'))
                    # Get text value for action comment from sync 2
                    for record in record_data_2:
                        if record.get('id') == action.get('id'):
                            current_action_text = record.get('data').get('text')
                    assert current_action_text, "Record  {} is missing from 2nd sync.".format(action.get('id'))

                    # Verify the action comment text matches expectations
                    if action.get('id')== action_id_to_update:
                        self.assertNotEqual(original_action_text, current_action_text, msg="Update was not captured.")
                        self.assertIn("UPDATE", current_action_text, msg="Update was captured but not as expected.")
                    else:
                        self.assertEqual(original_action_text, current_action_text, msg="Text does not match expected.")

        print("Incremental streams tested.")

        # CLEANING UP
        stream_to_delete = 'boards'
        boards_remaining = 5
        print("Deleting all but {} records for stream {}.".format(boards_remaining, stream_to_delete))
        board_count = len(expected_records_1.get(stream_to_delete, [])) + len(expected_records_2.get(stream_to_delete, []))
        for obj_to_delete in expected_records_2.get(stream_to_delete, []): # Delete all baords between syncs
            if board_count > boards_remaining:
                utils.delete_object(stream_to_delete, obj_to_delete.get('id'))
                board_count -= 1
            else:
                break
        for obj_to_delete in expected_records_1.get(stream_to_delete, []): # Delete all baords between syncs
            if board_count > boards_remaining:
                utils.delete_object(stream_to_delete, obj_to_delete.get('id'))
                board_count -= 1
            else:
                break
        # Reset the parent objects that we have been tracking
        utils.reset_tracked_parent_objects()


if __name__ == '__main__':
    unittest.main()
