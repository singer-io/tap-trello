import os
import logging
import unittest
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import trello_utils as utils


class TrelloBookmarkStates(unittest.TestCase):
    START_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    TEST_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    LOOKBACK_WINDOW = 1  # days
    TEST_BOARD_ID = utils.NEVER_DELETE_BOARD_ID

    """
    Below we define the formatting for the various bookmark states. The actual values
    are set in the test. These states should be considered as different possibilities
    for a given sync NOT as continuations of each other.
      e.g. sync_0 is a standard sync, sync_1 simulates a killed job in the middle of a sync
    """
    ACTIONS_STATES = {
        "state_0": {  # Final state after standard sync
            "window_start": 0,
        },
        "state_1": {  # State of interrupted sync for Inc streams
            "parent_id": TEST_BOARD_ID,
            "window_start": 0, "sub_window_end": 0, "window_end": 0
        },
        "state_2": {  # State of interrupted sync for Full Table streams
            "window_start": 0, "window_end": 0,"parent_id": TEST_BOARD_ID
        },
    }

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

    def get_tap_sorted_stream(self, stream: str = 'boards'):
        """The tap sorts parent objects in created at ascending order"""
        objs = utils.get_objects(obj_type=stream)
        obj_id_list = [obj.get('id') for obj in objs]

        id_created_dict = {obj_id: dt.fromtimestamp(int(obj_id[0:8],16))
                           for obj_id in obj_id_list}

        return sorted(id_created_dict.items())

    def untestable_streams(self):
        return {
            'users',
            'boards'
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
            'users' : {'id', 'boardId'},
            'lists' : {'id'},
            'actions' : {'id'},
            'cards' : {'id'},
            'checklists':  {'id'}
        }

    def expected_automatic_fields(self):
        return {
            'boards' : {'id'},
            'users' : {'id', 'boardId'},
            'lists' : {'id'},
            'actions' : {'id', 'date'},
            'cards' : {'id'},
            'checklists':  {'id'}
        }

    def get_properties(self):
        return {
            'start_date' : dt.strftime(dt.utcnow() - timedelta(days=2), self.START_DATE_FORMAT),  # set to utc today
        }

    def get_states_formatted(self, index: int):
        state_index = "state_{}".format(index)
        states = { "bookmarks": { "actions": self.ACTIONS_STATES[state_index]}}
        for stream in self.expected_incremental_sync_streams().difference({'boards'}):
            states['bookmarks'][stream] = dict()
        return states

    def test_run(self):
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Initialize start date prior to first sync
        self.START_DATE = self.get_properties().get('start_date')

        # ensure data exists for sync streams and set expectations
        records_to_create = 3
        expected_records = {x: [] for x in self.expected_sync_streams()} # ids by stream
        for stream in self.expected_sync_streams().difference(self.untestable_streams()):
            if stream in self.expected_incremental_sync_streams():
                since = dt.strptime(self.START_DATE, self.START_DATE_FORMAT).strftime(self.TEST_TIME_FORMAT)
                _, existing_objects = utils.get_total_record_count_and_objects(stream, since=since)
            else:
                _, existing_objects = utils.get_total_record_count_and_objects(stream)

            if existing_objects:
                logging.info("Data exists for stream: {}".format(stream))
                for obj in existing_objects:  # add existing records to expectations
                    expected_records[stream].append(
                        {field: obj.get(field)
                         for field in self.expected_automatic_fields().get(stream)}
                    )
            else:
                logging.info("Data does not exist for stream: {}".format(stream))
            while len(expected_records.get(stream)) < records_to_create:
                # Create more records if necessary
                new_object = utils.create_object(stream)
                logging.info("Data generated for stream: {}".format(stream))
                expected_records[stream].append({field: new_object.get(field)
                                                 for field in self.expected_automatic_fields().get(stream)})

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

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify data was replicated
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))
        synced_records = runner.get_records_from_target_output()

        # Verify bookmarks were saved for all streams
        state = menagerie.get_state(conn_id)
        for stream in self.expected_incremental_sync_streams():
            self.assertTrue(state.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
        print("Bookmarks meet expectations")

        # Grab the empty formatted states to test
        states_to_test = [self.get_states_formatted(i) for i in range(len(self.ACTIONS_STATES))]

        ##########################################################################
        ### Testing standard sync state_0
        ##########################################################################
        version_0 = menagerie.get_state_version(conn_id)

        # Set window_start to start_date 
        window_start_0 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT)
        states_to_test[0]['bookmarks']['actions']['window_start'] = window_start_0.strftime(self.TEST_TIME_FORMAT)

        print("Interjecting test state:\n{}".format(states_to_test[0]))
        menagerie.set_state(conn_id, states_to_test[0], version_0)

        # Run another sync
        print("Running sync job 0")
        sync_job_name_0 = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status_0 = menagerie.get_exit_status(conn_id, sync_job_name_0)
        menagerie.verify_sync_exit_status(self, exit_status_0, sync_job_name_0)

        # verify data was replicated
        record_count_by_stream_0 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        replicated_row_count_0 =  reduce(lambda accum,c : accum + c, record_count_by_stream_0.values())
        self.assertGreater(replicated_row_count_0, 0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream_0))
        print("total replicated row count: {}".format(replicated_row_count_0))
        synced_records_0 = runner.get_records_from_target_output()

        # Test state_0
        print("Testing State 0")
        state_0 = menagerie.get_state(conn_id)
        for stream in self.expected_incremental_sync_streams():
            # Verify bookmarks were saved as expected inc streams
            self.assertTrue(state_0.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
            print("Bookmarks meet expectations")
        for stream in self.expected_sync_streams().difference(self.untestable_streams()):
            data = synced_records.get(stream)
            record_messages = [set(row['data']) for row in data['messages']]

            data_0 = synced_records_0.get(stream)
            record_messages_0 = [set(row['data']) for row in data_0['messages']]

            # Verify we got the same number of records as the first sync
            self.assertEqual(record_count_by_stream_0.get(stream), record_count_by_stream.get(stream),
                             msg="Syncs should replicate the samee number of records")
            self.assertEqual(record_messages_0, record_messages,
                             msg="Syncs should replicate the samee number of records")

            # Verify we got the exact same records as the first sync
            for record_message in record_messages:
                self.assertTrue(record_message in record_messages_0,
                                msg="Expected {} to be in this sync.".format(record_message))

        ##########################################################################
        ### Testing interrupted sync state_1 with date-windowing
        ##########################################################################
        version_1 = menagerie.get_state_version(conn_id)

        # Set parent_id to id of second-to-last baord the tap will replicate
        sorted_parent_objs = self.get_tap_sorted_stream()
        penultimate_created_parent_id, _ = sorted_parent_objs[-2]
        last_created_parent_id, _ = sorted_parent_objs[-1]
        states_to_test[1]['bookmarks']['actions']['parent_id'] = penultimate_created_parent_id

        # Set window_end based off current time
        window_end_1 = dt.utcnow().strftime(self.TEST_TIME_FORMAT)
        # window_end_1 = state['bookmarks']['actions']['window_start']
        states_to_test[1]['bookmarks']['actions']['window_end'] = window_end_1

        # Set sub_window_end to today
        sub_window_end_1 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT) + timedelta(days=2)
        states_to_test[1]['bookmarks']['actions']['sub_window_end'] = sub_window_end_1.strftime(self.TEST_TIME_FORMAT)

        # Set window_start to start_date
        window_start_1 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT)
        states_to_test[1]['bookmarks']['actions']['window_start'] = window_start_1.strftime(self.TEST_TIME_FORMAT)

        print("Interjecting test state:\n{}".format(states_to_test[1]))
        menagerie.set_state(conn_id, states_to_test[1], version_1)

        # Run another sync (state_1)
        print("Running sync job 1")
        sync_job_name_1 = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_name_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_name_1)

        # verify data was replicated
        record_count_by_stream_1 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        replicated_row_count_1 =  reduce(lambda accum,c : accum + c, record_count_by_stream_1.values())
        self.assertGreater(replicated_row_count_1, 0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        print("total replicated row count: {}".format(replicated_row_count_1))

        synced_records_1 = runner.get_records_from_target_output()
        
        # Test state_1
        print("Testing State 1")
        state_1 = menagerie.get_state(conn_id)
        for stream in self.expected_incremental_sync_streams():
            # Verify bookmarks were saved as expected inc streams
            self.assertTrue(state_1.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
            print("Bookmarks for {} meet expectations".format(stream))

            # Verify the original sync catches more data since current test state bookmarks on the second most recent board
            self.assertGreater(record_count_by_stream.get(stream, 0),
                               record_count_by_stream_1.get(stream, 0),
                               msg="Expected to have more records for {}".format(stream)
            )

            # Verify sync 1 only replicates data from the bookmarked parent object (the most recently creted board)
            record_count_last_board = len(utils.get_objects(stream, parent_id=last_created_parent_id, since=window_start_1))

            record_count_penult_window_start = len(utils.get_objects(stream, parent_id=penultimate_created_parent_id, since=window_start_1))
            record_count_penult_sub_window = len(utils.get_objects(stream, parent_id=penultimate_created_parent_id, since=sub_window_end_1))
            record_count_penult_board = record_count_penult_window_start - record_count_penult_sub_window

            expected_record_count_1 = record_count_penult_board + record_count_last_board
            self.assertEqual(expected_record_count_1, record_count_by_stream_1.get(stream, 0),
                             msg="Sync 1 should only replicate data from the most recently creted board.")

        ##########################################################################
        ### Testing interrupted sync state_2 without date-windowing
        ##########################################################################
        version_2 = menagerie.get_state_version(conn_id)

        # Set parent_id to id of last baord the tap will replicate
        # Set window_end based off current time
        window_end_2 = dt.utcnow().strftime(self.TEST_TIME_FORMAT)
        # Set window_start to today at midnight
        window_start_2 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT) + timedelta(days=2)
        states_to_test[2]['bookmarks']['actions'] = {}
        for stream in self.expected_full_table_sync_streams().difference({'boards'}):
            states_to_test[2]['bookmarks'][stream] = {'window_start': window_start_2.strftime(self.TEST_TIME_FORMAT),
                                                      'window_end':  window_end_2,
                                                      'parent_id': last_created_parent_id}

        print("Interjecting test state:\n{}".format(states_to_test[2]))
        menagerie.set_state(conn_id, states_to_test[2], version_2)

        # Run another sync
        print("Running sync job 2")
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
        synced_records_2 = runner.get_records_from_target_output()

        # Test state_2
        print("Testing State 2")
        state_2 = menagerie.get_state(conn_id)
        for stream in self.expected_full_table_sync_streams().difference(self.untestable_streams()):
            # Verify bookmarks were saved as expected inc streams
            self.assertTrue(state_2.get('bookmarks', {}).get(stream, {}).get('window_start', {}),
                            msg="{} should have a bookmark value".format(stream))
            print("Bookmarks meet expectations")

            # Verify the smaller window replicates less data 
            self.assertLessEqual(record_count_by_stream_2.get(stream, 0),
                                 record_count_by_stream.get(stream, 0),
                                 msg="Expected to have more records for {}".format(stream)
            )

            # Verify the actions from today are caught in this sync
            expected_record_count_2 = len(utils.get_objects(stream, parent_id=last_created_parent_id))
            self.assertEqual(expected_record_count_2, record_count_by_stream_2.get(stream, 0),
                                 msg="Should have less than or equal number of records based on whether we lookback.")

        ##########################################################################
        ### CLEAN UP
        ##########################################################################
        stream_to_delete = 'boards'
        boards_remaining = 5
        print("Deleting all but {} records for stream {}.".format(boards_remaining, stream_to_delete))
        board_count = len(expected_records.get(stream_to_delete, []))
        for obj_to_delete in expected_records.get(stream_to_delete, []): # Delete all baords between syncs
            if board_count > boards_remaining:
                utils.delete_object(stream_to_delete, obj_to_delete.get('id'))
                board_count -= 1
            else:
                break

        # Reset the parent objects that we have been tracking
        utils.reset_tracked_parent_objects()


if __name__ == '__main__':
    unittest.main()
