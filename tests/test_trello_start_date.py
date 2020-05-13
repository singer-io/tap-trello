import os
import unittest
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import trello_utils as utils

class TestTrelloStartDate(unittest.TestCase):
    """Test that we are paginating for streams when exceeding the API record limit of a single query"""

    START_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    API_LIMIT = 50
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"

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
        return "tap_tester_trello_start_date_test"

    def get_type(self):
        return "platform.trello"

    def get_credentials(self):
        return {
            'consumer_key': os.getenv('TAP_TRELLO_CONSUMER_KEY'),
            'consumer_secret': os.getenv('TAP_TRELLO_CONSUMER_SECRET'),
            'access_token': os.getenv('TAP_TRELLO_ACCESS_TOKEN'),
            'access_token_secret': os.getenv('TAP_TRELLO_ACCESS_TOKEN_SECRET'),
        }

    def testable_streams(self): # Rip this if all streams testable
        return {
            'actions',
            'boards',
            'lists',
            'users'
        }
    def expected_check_streams(self):
        return {
            'actions',
            'boards',
            'lists',
            'users'
        }

    def expected_sync_streams(self):
        return self.expected_check_streams()

    def expected_pks(self):
        return {
            "actions" : {"id"},
            "boards" : {"id"},
            "lists" : {"id"},
            "users" : {"id"}
        }

    def expected_automatic_fields(self):
        return self.expected_pks()

    def tap_name(self):
        return "tap-trello"

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date' : dt.strftime(dt.utcnow(), self.START_DATE_FORMAT),  # set to utc today 
        }
        if original:
            return return_value
        
        # Start Date test needs the new connections start date to be prior to the default
        assert self.START_DATE < return_value["start_date"]

        # Assign start date to be the default 
        return_value["start_date"] = self.START_DATE
        return return_value


    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        # Initialize start date prior to first sync
        self.START_DATE = self.get_properties().get('start_date')

        # ensure data exists for sync streams and set expectations
        expected_records = {x: [] for x in self.expected_sync_streams()} # ids by stream
        for stream in self.testable_streams():
            # for _ in range(3): # TODO improve method to check if any data exists, if not then create some
            #     new_object = utils.create_object(stream)
            #     expected_records[stream].append(new_object['id'])
            # print("Data {} records generated for stream: {}".format(len(expected_records[stream]), stream))
            print("skipping data creation")


        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        # select all catalogs
        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            for k in self.expected_automatic_fields()[cat['stream_name']]:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)
                print("Validating inclusion on {}: {}".format(cat['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')
            connections.select_catalog_and_fields_via_metadata(conn_id, cat, catalog_entry)

        #clear state
        menagerie.set_state(conn_id, {})

        # Run sync 1
        sync_job_1 = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_1)

        # read target output
        record_count_by_stream_1 = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count_1 =  reduce(lambda accum,c : accum + c, record_count_by_stream_1.values())
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        print("total replicated row count: {}".format(replicated_row_count_1))
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        start_date_1 = self.get_properties()['start_date']
        self.START_DATE = dt.strftime(dt.strptime(self.START_DATE, self.START_DATE_FORMAT) \
                                      - timedelta(days=60), self.START_DATE_FORMAT)
        start_date_2 = self.START_DATE
        print("REPLAICATION START DATE CHANGE: {} ===>>> {} ".format(start_date_1, start_date_2))


        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id = connections.ensure_connection(self, original_properties=False)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        # select all catalogs
        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            for k in self.expected_automatic_fields()[cat['stream_name']]:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)
                print("Validating inclusion on {}: {}".format(cat['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')
            connections.select_catalog_and_fields_via_metadata(conn_id, cat, catalog_entry)

        # clear state
        menagerie.set_state(conn_id, {})

        # Run sync 2
        sync_job_2 = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_2)

        # This should be validating the the PKs are written in each record
        record_count_by_stream_2 = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count_2 =  reduce(lambda accum,c : accum + c, record_count_by_stream_2.values(), 0)
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_2))
        print("total replicated row count: {}".format(replicated_row_count_2))

        synced_records_2 = runner.get_records_from_target_output()

        ##########################################################################
        ### Start Date Tests
        ##########################################################################
        syncing_incremental = False
        for stream in self.testable_streams():
            if utils.get_replication_method(stream) == self.INCREMENTAL:
                # Verify the 1st sync replicated less data than the 2nd if syncing inc streams
                self.assertGreater(replicated_row_count_2, replicated_row_count_1,
                                   msg="we replicated less data with an older start date\n" +
                                   "------------------------------\n" +
                                   "Start Date 1: {} ".format(start_date_1) +
                                   "Row Count 1: {}\n".format(replicated_row_count_1) +
                                   "------------------------------\n" +
                                   "Start Date 2: {} ".format(start_date_2) +
                                   "Row Count 2: {}\n".format(replicated_row_count_2))
                syncing_incremental = True

        if not syncing_incremental:
            # Verify the 1st and 2nd sync replicated the same number of records if not syncing inc streams
            self.assertEqual(replicated_row_count_2, replicated_row_count_1,
                             msg="we replicated less data with an older start date\n" +
                             "------------------------------\n" +
                             "Start Date 1: {} ".format(start_date_1) +
                             "Row Count 1: {}\n".format(replicated_row_count_1) +
                             "------------------------------\n" +
                             "Start Date 2: {} ".format(start_date_2) +
                             "Row Count 2: {}\n".format(replicated_row_count_2))

        # Test by each stream 
        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                if utils.get_replication_method(stream) == self.INCREMENTAL:
                    # Test that we get more records for each stream from the the 2nd sync with an older start date
                    self.assertGreater(record_count_by_stream_2.get(stream, 0),
                                       record_count_by_stream_1.get(stream,0),
                                       msg="Stream {} is {}\n".format(stream, self.INCREMENTAL) +
                                       "Expected sync with start date {} to have more records ".format(start_date_2) +
                                       "than sync with start date {}. It does not.".format(start_date_1))
                    continue

                # Test that we get the sam amount of records for each stream regardless of start date
                self.assertEqual(record_count_by_stream_2.get(stream, 0),
                                 record_count_by_stream_1.get(stream,0),
                                 msg="Stream '{}' is {}\n".format(stream, self.FULL_TABLE) +
                                 "Expected sync with start date {} to have the same amount of records".format(start_date_2) +
                                 "than sync with start date {}. It does not.".format(start_date_1))
