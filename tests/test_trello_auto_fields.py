import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import trello_utils as utils
from singer import metadata

import os
import unittest
import logging
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce



class TestTrelloAutomaticFields(unittest.TestCase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""
    START_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    TEST_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

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
        return "tap_tester_trello_auto_fields_test"

    def get_type(self):
        return "platform.trello"

    def get_credentials(self):
        return {
            'consumer_key': os.getenv('TAP_TRELLO_CONSUMER_KEY'),
            'consumer_secret': os.getenv('TAP_TRELLO_CONSUMER_SECRET'),
            'access_token': os.getenv('TAP_TRELLO_ACCESS_TOKEN'),
            'access_token_secret': os.getenv('TAP_TRELLO_ACCESS_TOKEN_SECRET'),
        }

    def testable_streams(self):
        return {
            'actions',
            'boards',
            'checklists',
            'cards',
            'lists',
            'users'
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

    def expected_full_table_streams(self):
        return {
            'boards',
            'cards',
            'checklists',
            'lists',
            'users',
        }

    def expected_incremental_streams(self):
        return {
            'actions'
        }

    def expected_sync_streams(self):
        return self.expected_check_streams()

    def expected_pks(self):
        return {
            'actions' : {"id"},
            'boards' : {"id"},
            'cards' : {'id'},
            'checklists': {'id'},
            'lists' : {"id"},
            'users' : {"id", "boardId"}
        }

    def expected_automatic_fields(self):
        return {
            'actions' : {"id", "date"},
            'boards' : {"id"},
            'cards' : {'id'},
            'checklists': {'id'},
            'lists' : {"id"},
            'users' : {"id", "boardId"}
        }

    def tap_name(self):
        return "tap-trello"

    def get_properties(self):
        return {
            'start_date' : dt.strftime(dt.utcnow(), self.START_DATE_FORMAT),  # set to utc today
        }

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {})
                # remove properties that are automatic
                for prop in self.expected_automatic_fields().get(catalog['stream_name'], []):
                    if prop in non_selected_properties:
                        del non_selected_properties[prop]
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties.keys()
            )

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Resetting tracked parent objects prior to test
        utils.reset_tracked_parent_objects()

        # ensure data exists for sync streams and set expectations
        expected_records = {x: [] for x in self.expected_sync_streams()} # ids by stream
        for stream in self.testable_streams():
            since = None
            if stream in self.expected_incremental_streams():
                since = dt.strptime(self.get_properties()['start_date'],
                                    self.START_DATE_FORMAT).strftime(self.TEST_TIME_FORMAT)
            _, existing_objects = utils.get_total_record_count_and_objects(stream, since=since)
            if existing_objects:
                logging.info("Data exists for stream: {}".format(stream))
                for obj in existing_objects:
                    expected_records[stream].append(
                        {field: obj.get(field)
                         for field in self.expected_automatic_fields().get(stream)}
                    )
                continue

            logging.info("Data does not exist for stream: {}".format(stream))

            new_object = utils.create_object(stream)
            logging.info("Data generated for stream: {}".format(stream))
            expected_records[stream].append({field: new_object.get(field)
                                             for field in self.expected_automatic_fields().get(stream)})

        conn_id = connections.ensure_connection(self)

        #run in check mode
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

        # Select all streams but only automtic fields
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=False)

        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            for k in self.expected_automatic_fields()[cat['stream_name']]:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)
                print("Validating inclusion on {}: {}".format(cat['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')

        catalogs = menagerie.get_catalogs(conn_id)

        #clear state
        menagerie.set_state(conn_id, {})

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        first_record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                         self.expected_sync_streams(),
                                                                         self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in first_record_count_by_stream.items():
            assert stream in self.expected_sync_streams()
            self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count))

        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = self.expected_automatic_fields().get(stream)

                # Verify that ONLY automatic fields are emitted
                for actual_keys in record_messages_keys:
                    self.assertEqual(
                        actual_keys.symmetric_difference(expected_keys), set(),
                        msg="Expected automatic fields and nothing else.")

                actual_records = [row['data'] for row in data['messages']]

                # Verify the number of records match expectations
                # NOTE: actions seem to be getting updated by trello's backend resulting in an action from a previous
                #       test run gettting synced again, so we will be less strict for this stream
                if stream == 'actions':
                    self.assertLessEqual(len(expected_records.get(stream)),
                                         len(actual_records),
                                         msg="Number of actual records do match expectations. " +\
                                         "We probably have duplicate records.")
                else:
                    self.assertEqual(len(expected_records.get(stream)),
                                     len(actual_records),
                                     msg="Number of actual records do match expectations. " +\
                                     "We probably have duplicate records.")


                # verify by values, that we replicated the expected records
                for actual_record in actual_records:
                    if stream != 'actions':  # see NOTE above
                        self.assertTrue(actual_record in expected_records.get(stream),
                                        msg="Actual record missing from expectations")
                for expected_record in expected_records.get(stream):
                    self.assertTrue(expected_record in actual_records,
                                    msg="Expected record missing from target.")

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


if __name__ == '__main__':
    unittest.main()
