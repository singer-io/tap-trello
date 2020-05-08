import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import trello_utils as utils

import os
import unittest
import logging
from functools import reduce


class TestTrelloAutomaticFields(unittest.TestCase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    API_LIMIT = 1000

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
        return "tap_tester_trello_no_fields_test"

    def get_type(self):
        return "platform.trello"

    def get_credentials(self):
        return {
            'consumer_key': os.getenv('TAP_TRELLO_CONSUMER_KEY'),
            'consumer_secret': os.getenv('TAP_TRELLO_CONSUMER_SECRET'),
            'access_token': os.getenv('TAP_TRELLO_ACCESS_TOKEN'),
            'access_token_secret': os.getenv('TAP_TRELLO_ACCESS_TOKEN_SECRET'),
        }

    def testable_streams(self): # TODO rip this once all streams testable
        return {
            'actions'
        }
    def expected_check_streams(self):
        return {
            'actions',
            'boards',
            'lists',
            'users'
        }

    def expected_sync_streams(self):
        return {
            'actions',
            'boards',
            'lists',
            'users'
        }

    def expected_pks(self):
        return {
            "actions": {"id"},
            "boards" : {"id"},
            "lists" : {"id"},
            "users" : {"id"}

        }

    def expected_automatic_fields(self):
        return self.expected_pks()

    def tap_name(self):
        return "tap-trello"


    def get_properties(self):
        return {
            'start_date' : '2020-03-01T00:00:00Z'
        }

    def get_total_record_count(self, parent_stream: str, child_stream: str):
        parent_objects = utils.get_objects(obj_type=parent_stream)
        count = 0

        for obj in parent_objects:
            logging.info("Getting {} via {} parent stream".format(child_stream, parent_stream))
            existing_objects = utils.get_objects(obj_type=child_stream, parent_id=obj.get('id'))
            count += len(existing_objects)

        return count

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        # Ensure tested streams have a record count which exceeds the API LIMIT
        #expected_records = {x: [] for x in self.expected_sync_streams()} # ids by stream # TODO See NOTE below
        final_count = {x: 0 for x in self.expected_sync_streams()}
        for stream in self.testable_streams(): # just actions at the moment
            parent_stream = 'boards' # TODO Set somewhere as global
            record_count = self.get_total_record_count(parent_stream, stream)
            import pdb; pdb.set_trace()
            if record_count <= self.API_LIMIT:
                logging.info("Not enough data to paginate : {} has {} records".format(stream, record_count))
                parent_id = utils.get_random_object_id(parent_stream)

                while record_count <= self.API_LIMIT:
                    new_object = utils.create_object(obj_type=stream) #, obj_id=parent_id)
                    record_count += 1
                    logging.info("Record Created: {} has {} records".format(stream, record_count))
                    # expected_records[stream].append(new_object['id'])
                    # NOTE: If we want exact records ^ will need to some work to find the action id that was just created
                final_count[stream] = self.get_total_record_count(parent_stream, stream)
                logging.info("FINAL RECORD COUNT: {} has {} records".format(stream, final_count[stream]))
                continue

        # Verify we did in fact generate enough records to exceed the API LIMIT
        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                # If we are failing here, it is most likely an issue with /tests/trello_utils.py
                self.assertGreater(final_count[stream], self.API_LIMIT,
                                   msg="Failed to create sufficient data prior to sync.")

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

        #select all catalogs
        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            for k in self.expected_automatic_fields()[cat['stream_name']]:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)
                print("Validating inclusion on {}: {}".format(cat['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')

            connections.select_catalog_and_fields_via_metadata(conn_id, cat, catalog_entry,
                                                               non_selected_fields=non_selected_fields)

        #clear state
        menagerie.set_state(conn_id, {})

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                         self.expected_sync_streams(),
                                                                         self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        for stream in self.testable_streams():
            with self.subTest(stream=stream):

                # Verify we are paginating for testable synced streams
                self.assertGreater(record_count_by_stream.get(stream, -1), self.API_LIMIT,
                                   msg="We didn't gaurantee pagination. The number of records should exceed the api limit.")

                data = synced_records.get(stream_name, [])
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]

                
                for actual_keys in record_messages_keys:

                    # Verify that the automatic fields are sent to the target for paginated streams
                    self.assertEqual(self.expected_automatic_fields().get(stream_name) - actual_keys,
                                     set(), msg="A paginated synced stream has a record that is missing automatic fields.")

                    # Verify we have more fields sent to the target than just automatic fields (this is set above)
                    # SKIP THIS ASSERTION IF ALL FIELDS ARE INTENTIONALLY AUTOMATIC FOR THIS STREAM
                    self.assertGreate(actual_keys, self.expected_automatic_fields().get(stream_name),
                                      msg="A paginated synced stream has a record that is missing non-automatic fields.")

                    # TODO Get more specific with this assertion ^ genereate an exact list of expected_keys?
                    #      This may be unnecessary? If not, see auto fields test for implementation.
