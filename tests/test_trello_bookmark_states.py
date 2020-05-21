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
import trello_bookmark_states as trello_states

class TrelloBookmarkStates(unittest.TestCase):
    START_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    TEST_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    ACTIONS_STATES = {  # TODO determine if these should always run TODO see if menagerie accepts these
        "state_0": {  # State of interrupted sync for Inc streams
            "parent_id": utils.NEVER_DELETE_BOARD_ID,
            "window_start": 0, "sub_window_end": 0, "window_end": 0
        },
        "state_1": {  # State of interrupted sync for Full Table streams
            "window_start": 0, "window_end": 0,"parent_id": utils.NEVER_DELETE_BOARD_ID
        },
        "state_2": {  # Final state after standard sync
            "window_start": 0,
        },  # ERROR STATES BELOW
        "state_3": {  # Set window_start = window_end
            "parent_id": utils.NEVER_DELETE_BOARD_ID,
            "window_start": 0, "window_end": 0
        },        
        "state_4": {  # Valid bookmark states but missing a parent_id
            "window_start": 0, "sub_window_end": 0, "window_end": 0
        },
        "state_5": {  # Set all bookmarks to None to exploit a potential oversight in 'on_window_started()'
            "parent_id": utils.NEVER_DELETE_BOARD_ID
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
        return "ta_tester_trello_bookmarks_qa"

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
            'lists',
            'users'
        }

    def expected_sync_streams(self):
        return self.expected_check_streams()

    def expected_full_table_sync_streams(self):
        return {
            'boards',
            'cards',
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
            'cards' : {'id'}
        }

    def expected_automatic_fields(self):
        return self.expected_pks()

    def get_properties(self):
        return {
            'start_date' : dt.strftime(dt.utcnow() - timedelta(days=2), self.START_DATE_FORMAT),  # set to utc today
        }

    def get_states_formatted(self, index: int):
        state_index = "state_{}".format(index)
        return { "bookmarks": { "actions": self.ACTIONS_STATES[state_index], "boards": dict(), "cards": dict(), "lists": dict(), "users": dict() } }

    def test_run(self):
        # TODO add "with self.subTest(stream=stream):" to for loops so assertions run for all streams

        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Initialize start date prior to first sync
        self.START_DATE = self.get_properties().get('start_date')

        # ensure data exists for sync streams and set expectations
        expected_records = {x: [] for x in self.expected_sync_streams()} # ids by stream
        for stream in self.expected_sync_streams().difference(self.untestable_streams()):
            _, existing_objects = utils.get_total_record_count_and_objects(stream)
            if existing_objects:
                logging.info("Data exists for stream: {}".format(stream))
                for obj in existing_objects:  # add existing records to expectations
                    expected_records[stream].append(
                        {field: obj.get(field)
                         for field in self.expected_automatic_fields().get(stream)}
                    )
                continue
            # Create 1 record if none exist
            logging.info("Data does not exist for stream: {}".format(stream))
            new_object = utils.create_object(stream)
            logging.info("Data generated for stream: {}".format(stream))
            expected_records[stream].append({field: obj.get(field)
                                            for field in self.expected_automatic_fields().get(stream)})

        # set time for comparing recent records once bookmark state is changed
        test_start_time = dt.utcnow().strftime(self.TEST_TIME_FORMAT)  # "date": "2020-05-15T11:41:37.432Z"

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

        # TODO fix index nums in test and in ACTIONS_STATES above
        ##########################################################################
        ### Testing standard sync state_2
        ##########################################################################
        version_2 = menagerie.get_state_version(conn_id)

        # Set window_start to start_date 
        window_start_2 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT)
        states_to_test[2]['bookmarks']['actions']['window_start'] = window_start_2.strftime(self.TEST_TIME_FORMAT)

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
        for stream in self.expected_incremental_sync_streams():
            # Verify bookmarks were saved as expected inc streams
            self.assertTrue(state_2.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
            print("Bookmarks meet expectations")
        for stream in self.expected_sync_streams().difference(self.untestable_streams()):
            data = synced_records.get(stream)
            record_messages = [set(row['data']) for row in data['messages']]
            data_2 = synced_records_2.get(stream)
            record_messages_2 = [set(row['data']) for row in data_2['messages']]

            # Verify we got the same number of records as the first sync
            self.assertEqual(record_count_by_stream_2.get(stream), record_count_by_stream.get(stream),
                             msg="Syncs should replicate the samee number of records")
            self.assertEqual(record_messages_2, record_messages,
                             msg="Syncs should replicate the samee number of records")

            # Verify we got the exact same records as the first sync
            for record_message in record_messages:
                self.assertTrue(record_message in record_messages_2,
                                msg="Expected {} to be in this sync.".format(record_message))

        ##########################################################################
        ### Testing interrupted sync state_0 with date-windowing
        ##########################################################################
        # Generate a new actions object for setting expectations
        # new_actions = [utils.create_object('actions')]

        version_0 = menagerie.get_state_version(conn_id)

        # Set window_end based off current time
        window_end_0 = dt.utcnow().strftime(self.TEST_TIME_FORMAT)
        states_to_test[0]['bookmarks']['actions']['window_end'] = window_end_0

        # Set sub_window_end to today
        sub_window_end_0 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT) + timedelta(days=2)
        states_to_test[0]['bookmarks']['actions']['sub_window_end'] = sub_window_end_0.strftime(self.TEST_TIME_FORMAT)

        # Set window_start to start_date
        window_start_0 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT)
        states_to_test[0]['bookmarks']['actions']['window_start'] = window_start_0.strftime(self.TEST_TIME_FORMAT)

        print("Interjecting test state:\n{}".format(states_to_test[0]))
        menagerie.set_state(conn_id, states_to_test[0], version_0)

        # Run another sync (state_0)
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
            print("Bookmarks for {} meet expectations".format(stream))

            # Verify the newly created boards are caught in this sync
            self.assertGreater(record_count_by_stream_0.get(stream, 0),
                               record_count_by_stream.get(stream, 0),
                               msg="Expected to have more records for {}".format(stream)
            )
            self.assertEqual(record_count_by_stream_0.get(stream, 0) - record_count_by_stream.get(stream, 0),
                             len(new_boards),
                             msg="Expected to have exactly {} more record for {}".format(len(new_boards), stream)
            )

        ##########################################################################
        ### Testing interrupted sync state_1 without date-windowing
        ##########################################################################
        version_1 = menagerie.get_state_version(conn_id)

        # Set window_end based off current time
        window_end_1 = dt.utcnow().strftime(self.TEST_TIME_FORMAT)
        states_to_test[1]['bookmarks']['actions']['window_end'] = window_end_1

        # Set window_start to today
        window_start_1 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT) + timedelta(days=2)
        states_to_test[1]['bookmarks']['actions']['window_start'] = window_start_1.strftime(self.TEST_TIME_FORMAT)

        print("Interjecting test state:\n{}".format(states_to_test[1]))
        menagerie.set_state(conn_id, states_to_test[1], version_1)

        # Run another sync
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
            print("Bookmarks meet expectations")

            # Verify the smaller window replicates less data 
            self.assertLess(record_count_by_stream_1.get(stream, 0),
                            record_count_by_stream.get(stream, 0),
                            msg="Expected to have more records for {}".format(stream)
            )
            # Verify the new actions are caught in this sync
            data_1 = synced_records_1.get(stream)
            record_messages_1 = [set(row['data']) for row in data_1['messages']]

        ##########################################################################
        ### Testing standard sync state_3
        ##########################################################################
        version_3 = menagerie.get_state_version(conn_id)

        # Set window_end based off current time
        window_end_3 = state_2['bookmarks']['actions']['window_start']
        states_to_test[3]['bookmarks']['actions']['window_end'] = window_end_3

        # Set window_start to window_end
        window_start_3 = window_end_3
        states_to_test[3]['bookmarks']['actions']['window_start'] = window_start_3

        print("Interjecting test state:\n{}".format(states_to_test[3]))
        menagerie.set_state(conn_id, states_to_test[3], version_3)

        # Run another sync
        print("Running sync job 3")
        sync_job_name_3 = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status_3 = menagerie.get_exit_status(conn_id, sync_job_name_3)
        menagerie.verify_sync_exit_status(self, exit_status_3, sync_job_name_3)

        # verify data was replicated
        record_count_by_stream_3 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        replicated_row_count_3 =  reduce(lambda accum,c : accum + c, record_count_by_stream_3.values())
        self.assertGreater(replicated_row_count_3, 0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream_3))
        print("total replicated row count: {}".format(replicated_row_count_3))
        synced_records_3 = runner.get_records_from_target_output()

        # Verify bookmarks were saved as expected inc streams
        state_3 = menagerie.get_state(conn_id)

        # Test cases for state_3
        for stream in self.expected_incremental_sync_streams():
            # Verify bookmarks were saved as expected inc streams
            self.assertTrue(state_3.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
            print("Bookmarks meet expectations")

            # Verify no data was replicated for incremental streams
            self.assertEqual(
                record_count_by_stream_3.get(stream, 0), 0,
                msg="Expected not to replicate inc streams for state:\n{}".format(states_to_test[3])
            )

        ##########################################################################
        ### Testing interrupted sync state_4
        ##########################################################################
        version_4 = menagerie.get_state_version(conn_id)

        # Set window_end based off current time
        window_end_4 = dt.utcnow().strftime(self.TEST_TIME_FORMAT)
        states_to_test[4]['bookmarks']['actions']['window_end'] = window_end_4

        # Set sub_window_end to start_date
        sub_window_end_4 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT)
        states_to_test[4]['bookmarks']['actions']['sub_window_end'] = sub_window_end_4.strftime(self.TEST_TIME_FORMAT)

        # Set window_start to yesterday
        window_start_4 = dt.strptime(self.START_DATE, self.START_DATE_FORMAT) - timedelta(days=1)
        states_to_test[4]['bookmarks']['actions']['window_start'] = window_start_4.strftime(self.TEST_TIME_FORMAT)

        print("Interjecting test state:\n{}".format(states_to_test[4]))
        menagerie.set_state(conn_id, states_to_test[4], version_4)

        # Run another sync (state_4)
        print("Running sync job 4")
        sync_job_name_4 = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status_4 = menagerie.get_exit_status(conn_id, sync_job_name_4)
        menagerie.verify_sync_exit_status(self, exit_status_4, sync_job_name_4)

        # verify data was replicated
        record_count_by_stream_4 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        replicated_row_count_4 =  reduce(lambda accum,c : accum + c, record_count_by_stream_4.values())
        self.assertGreater(replicated_row_count_4, 0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream_4))
        print("total replicated row count: {}".format(replicated_row_count_4))
        synced_records_4 = runner.get_records_from_target_output()
        state_4 = menagerie.get_state(conn_id)

        # Test cases for state_4
        for stream in self.expected_incremental_sync_streams():
            # Verify bookmarks were saved as expected inc streams
            self.assertTrue(state_4.get('bookmarks', {}).get(stream, {}).get('window_start', {}))
            print("Bookmarks meet expectations")

            # Verify no data was replicated for incremental streams
            # self.assertEqual(
            #     record_count_by_stream_4.get(stream, 0), 0,
            #     msg="Expected not to replicate inc streams for state:\n{}".format(states_to_test[4])
            # )

        # CLEAN UP
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
        print("---------- TODOs still present. Not all streams are fully tested ----------")

if __name__ == '__main__':
    unittest.main()
