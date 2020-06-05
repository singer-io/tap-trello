import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import trello_utils as utils
from singer import metadata

import os
import unittest
import copy
import logging
import random
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce



class TestTrelloCustomFields(unittest.TestCase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""
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
        return "tap_tester_trello_custom_fields_test"

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
            'cards',
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
            'actions' : {"id"},  #, "date"},
            'boards' : {"id"},
            'cards' : {'id'},
            'checklists': {'id'},
            'lists' : {"id"},
            'users' : {"id", "boardId"}
        }

    def expected_custom_fields(self):
        return {
            "checkbox",
            "list",
            "number",
            "text",
            "date",
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
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
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
        _, existing_boards = utils.get_total_record_count_and_objects('boards')
        custom_fields_dict= {x: [] for x in self.expected_custom_fields()} # ids by stream
        custom_fields_by_board= {x.get('id'): copy.deepcopy(custom_fields_dict) for x in existing_boards} # ids by stream

        # get existing custom fields for each board
        print("Getting objects on baord with static custom field set")
        for board_id, board_cfields in custom_fields_by_board.items():
            cfields = utils.get_custom_fields('boards', board_id)
            for field in self.expected_custom_fields():
                cfields_type_field = [f for f in cfields if f['type'] == field]
                if cfields_type_field:
                    board_cfields[field] += cfields_type_field

        # get expected cards with custom fields
        expected_records_cfields = list()
        board_id = utils.NEVER_DELETE_BOARD_ID
        all_cards_on_board = utils.get_objects('cards', parent_id=board_id)
        print("Setting custom fields expectations based on static data")
        for card in all_cards_on_board:
            card_with_cfields = utils.get_objects('cards', obj_id=card.get('id'),
                                                   parent_id=board_id, custom_fields=True)

            if card_with_cfields:
                expected_records_cfields += card_with_cfields

        # veryify at least 1 record exists for each custom field type or else our assertions are invalid
        fields_exist = {x: False for x in self.expected_custom_fields()}
        for record in expected_records_cfields:
            if all(v for _, v in fields_exist.items()):
                break
            value = record.get('value')            
            if value:
                key = next(iter(value))
                if key in self.expected_custom_fields() and not fields_exist.get(key):
                    fields_exist[key] = True
                elif key == 'checked':
                    fields_exist['checkbox'] = True
                else:  # key is none b/c options on a list do not have a 'value' attribute
                    fields_exist['list'] = True
        self.assertTrue(all(v for _, v in fields_exist.items()),
                        msg="Not all custom field types have data. Data must be restored manually on Trello account" +\
                        "\nCurrent data: {}".format(fields_exist))

        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        # Select all streams and all fields
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=True)

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

        # Testing streams with custom fields
        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                
                data = synced_records.get(stream)
                record_messages = [row['data'] for row in data['messages']]
                record_ids = [message.get('id') for message in record_messages]

                record_custom_fields = [message.get('customFieldItems')
                                        for message in record_messages if message.get('customFieldItems', None)]
                record_cfield_ids = []  # [record.get('id') for record in record_custom_fields]
                for record in record_custom_fields:
                    for cfield in record:
                        record_cfield_ids.append(cfield.get('id'))


                # Verify that we replicated the records with custom_fields
                for card in all_cards_on_board:
                    if card.get('id') in expected_records_cfields:
                        self.assertIn(
                            card.get('id'), records_ids,
                            msg="Missing a record that has custom fields:\n{}".format(card.get('id'))
                        ) 

                # Verify that we replicated the expected custom fields on those records
                for expected_cfield in expected_records_cfields:
                    self.assertIn(
                        expected_cfield.get('id'), record_cfield_ids,
                        msg="Missing custom field from expected {} record id={}".format(stream, expected_cfield.get('id'))
                    )

                    # Verify the expected custom field attributes match the replicated data
                    for actual_cfields in record_custom_fields:
                        import pdb; pdb.set_trace()
                        expected_cfield_replicated = expected_cfield in actual_cfields
                        if expected_cfield_replicated:
                            break
                    self.assertTrue(expected_cfield_replicated)

        # Reset the parent objects that we have been tracking
        utils.reset_tracked_parent_objects()
        print("---------- TODOs still present. Not all streams are fully tested ----------")


if __name__ == '__main__':
    unittest.main()
