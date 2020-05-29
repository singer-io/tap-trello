import os
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


class TrelloBookmarksQA(unittest.TestCase):
    START_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_TRELLO_CONSUMER_KEY",
            "TAP_TRELLO_CONSUMER_SECRET",
            "TAP_TRELLO_ACCESS_TOKEN",
            "TAP_TRELLO_ACCESS_TOKEN_SECRET",
        ] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_trello_bookmarks_qa"

    def get_type(self):
        return "platform.trello"

    def get_credentials(self):
        return {
            'consumer_key': os.getenv('TAP_TRELLO_CONSUMER_KEY'),
            'consumer_secret': os.getenv('TAP_TRELLO_CONSUMER_SECRET'),
            'access_token': os.getenv('TAP_TRELLO_ACCESS_TOKEN'),
            'access_token_secret': os.getenv('TAP_TRELLO_ACCESS_TOKEN_SECRET'),
        }

    def untestable_streams(self):
        return {
            'users',
        }

    def expected_check_streams(self):
        return {
            'actions',
            'boards',
            'cards',
            'checklists',
            'lists',
            'users'
        }

    def expected_sync_streams(self):
        return self.expected_check_streams()

    def expected_full_table_sync_streams(self):
        return {
            'boards',
            'cards',
            'checklists',
            'lists',
            'users',
        }

    def expected_incremental_sync_streams(self):
        return {
            'actions'
        }

    def tap_name(self):
        return "tap-trello"

    def expected_pks(self):
        return {
            'boards' : {'id'},
            'users' : {'id', "boardId"},
            'lists' : {'id'},
            'actions' : {'id'},
            'cards' : {'id'},
            'checklists' : {'id'}
        }

    def expected_automatic_fields(self):
        return {
            'boards' : {'id'},
            'users' : {'id', 'boardId'},
            'lists' : {'id'},
            'actions' : {'id'},
            'cards' : {'id', 'date'},
            'checklists' : {'id'}
        }

    def get_properties(self):
        return {
            'start_date' : dt.strftime(dt.utcnow(), self.START_DATE_FORMAT),  # set to utc today
        }

    def test_run(self):
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # ensure data exists for sync streams and set expectations
        expected_records_1 = {x: [] for x in self.expected_sync_streams()} # ids by stream
        for stream in self.expected_sync_streams().difference(self.untestable_streams()):
            _, existing_objects = utils.get_total_record_count_and_objects(stream)
            if existing_objects:
                logging.info("Data exists for stream: {}".format(stream))
                for obj in existing_objects:  # add existing records to expectations
                    expected_records_1[stream].append(
                        {field: obj.get(field)
                         for field in self.expected_automatic_fields().get(stream)}
                    )
                continue
            # Create 1 record if none exist
            logging.info("Data does not exist for stream: {}".format(stream))
            new_object = utils.create_object(stream)
            logging.info("Data generated for stream: {}".format(stream))
            expected_records_1[stream].append({field: new_object.get(field)
                                               for field in self.expected_automatic_fields().get(stream)})
        # Create comment actions
        action_comments = []
        action_comments.append(utils.create_object('actions', action_type="comment"))
        action_comments.append(utils.create_object('actions', action_type="comment"))

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
        for stream in self.expected_incremental_sync_streams():
            self.assertTrue(state_1.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
        print("Bookmarks meet expectations")

        # Generate data between syncs for bookmarking streams
        print("Generating more data prior to 2nd sync")
        expected_records_2 = {x: [] for x in self.expected_sync_streams()}
        for stream in self.expected_sync_streams().difference(self.untestable_streams()):
            for _ in range(1):
                new_object = utils.create_object(stream)
                expected_records_2[stream].append({field: new_object.get(field)
                                                   for field in self.expected_automatic_fields().get(stream)})

        print("Updating existing data prior to 2nd sync")
        # Update a single comment action before second sync
        updated_records = {x: [] for x in self.expected_sync_streams()}
        obj_id_to_update = random.choice(action_comments).get('id')
        updated_object = utils.update_object(stream, obj_id=obj_id_to_update)
        updated_records[stream].append(updated_object)

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
        for stream in self.expected_incremental_sync_streams():
            self.assertTrue(state_2.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
        print("Bookmarks meet expectations")

        # TESTING FULL TABLE STREAMS
        for stream in self.expected_full_table_sync_streams().difference(self.untestable_streams()):
            with self.subTest(stream=stream):
                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Assert we have data for both syncs for full table streams
                self.assertGreater(record_count_1, 0)
                self.assertGreater(record_count_2, 0)

                # Assert that we are capturing the expected number of records for full table streams
                self.assertGreater(record_count_2, record_count_1,
                                   msg="Full table streams should have more data in second sync.")
                self.assertEqual((record_count_2 - record_count_1),
                                 len(expected_records_2.get(stream, [])),
                                 msg="The differnce in record counts between syncs should " +\
                                 "equal the number of records we created between syncs.\n" +\
                                 "This is not the case for {}".format(stream))

                # Assert that we are capturing the expected records for full table streams
                data_1 = synced_records_1.get(stream, [])
                record_messages_1 = [row.get('data').get('id') for row in data_1['messages']]
                data_2 = synced_records_2.get(stream, [])
                record_messages_2 = [row.get('data').get('id') for row in data_2['messages']]
                for record in expected_records_1.get(stream):
                    self.assertTrue(record.get('id') in record_messages_1,
                                    msg="Missing an expected record from sync 1.")
                    self.assertTrue(record.get('id') in record_messages_2,
                                    msg="Missing an expected record from sync 2.")
                expected_records_2_set = set(record.get('id') for record in expected_records_2.get(stream))
                self.assertEqual(set(record_messages_2).difference(set(record_messages_1)),
                                 expected_records_2_set,
                                 msg="We did not get the new record(s)")

                # TODO assertions for updated data for each stream

        print("Full table streams tested.")

        # TESTING INCREMENTAL STREAMS
        for stream in self.expected_incremental_sync_streams().difference(self.untestable_streams()):
            with self.subTest(stream=stream):
                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Assert we have data for both syncs for inc streams
                self.assertGreater(record_count_1, 0)
                self.assertGreater(record_count_2, 0)

                if stream == 'actions':
                    # Assert that we are capturing the expected number of records for inc streams
                    self.assertLessEqual(record_count_1, len(expected_records_1.get(stream, [])),
                                         msg="Stream {} replicated an unexpedted number records on 1st sync.".format(stream))
                    # TODO Update 'since' in PARAMS to equal self.start_date. Then this ^ can change back to assertEqual
                    self.assertGreaterEqual(record_count_2, len(expected_records_2.get(stream, [])),
                                     msg="Stream {} replicated an unexpedted number records on 2nd sync.".format(stream))
                    # TODO track created objects as actions in order to change ^ back to assertEqual
                    continue

                # Assert that we are capturing the expected number of records for inc streams
                self.assertLessEqual(record_count_1, len(expected_records_1.get(stream, [])),
                                     msg="Stream {} replicated an unexpedted number records on 1st sync.".format(stream))
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
                    self.assertFalse(record.get('id') in record_messages_2,
                                     msg="This record does not belong in this sync:" +\
                                     "{}".format(record.get('id')))

                for record in expected_records_2.get(stream):
                    self.assertTrue(record.get('id') in record_messages_2,
                                    msg="Missing an expected record from sync 1.")
                    self.assertFalse(record.get('id') in record_messages_1,
                                     msg="This record does not belong in this sync:" +\
                                     "{}".format(record.get('id')))

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
        print("\n\n---------- TODOs still present. Not all streams are fully tested ----------\n\n")


if __name__ == '__main__':
    unittest.main()
