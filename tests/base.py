import os
import unittest

from datetime import datetime as dt

import tap_tester.connections as connections
import tap_tester.menagerie as menagerie


class TrelloBaseTest(unittest.TestCase):
    """Base test class with common setup and utility methods for Trello tap tests"""

    START_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    TEST_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_TRELLO_API_KEY",
            "TAP_TRELLO_API_TOKEN",
        ] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_trello_base_test"

    def get_type(self):
        return "platform.trello"

    def get_credentials(self):
        return {
            'api_key': os.getenv('TAP_TRELLO_API_KEY'),
            'api_token': os.getenv('TAP_TRELLO_API_TOKEN'),
        }

    def tap_name(self):
        return "tap-trello"

    def get_properties(self):
        return {
            'start_date': dt.strftime(dt.utcnow(), self.START_DATE_FORMAT),
        }

    def testable_streams(self):
        return self.expected_check_streams().difference(self.untestable_streams())

    def untestable_streams(self):
        return set()

    def expected_check_streams(self):
        return {
            'actions',
            'board_custom_fields',
            'board_labels',
            'board_memberships',
            'boards',
            'card_attachments',
            'card_custom_field_items',
            'cards',
            'checklists',
            'lists',
            'members',
            'organization_actions',
            'organization_members',
            'organization_memberships',
            'organizations',
            'users'
        }

    def expected_sync_streams(self):
        return self.expected_check_streams()

    def expected_full_table_streams(self):
        return {
            'boards',
            'board_custom_fields',
            'board_labels',
            'board_memberships',
            'cards',
            'card_attachments',
            'card_custom_field_items',
            'checklists',
            'lists',
            'members',
            'organizations',
            'organization_members',
            'organization_memberships',
            'users',
        }

    def expected_full_table_sync_streams(self):
        return self.expected_full_table_streams()

    def expected_incremental_streams(self):
        return {
            'actions',
            'organization_actions'
        }

    def expected_pks(self):
        return {
            'actions': {"id"},
            'boards': {"id"},
            'board_custom_fields': {"id", "boardId"},
            'board_labels': {"id", "boardId"},
            'board_memberships': {"id", "boardId"},
            'cards': {'id'},
            'card_attachments': {"id", "card_id"},
            'card_custom_field_items': {"id", "card_id"},
            'checklists': {'id'},
            'lists': {"id"},
            'members': {"id"},
            'organizations': {"id"},
            'organization_actions': {"id", "organization_id"},
            'organization_members': {"id", "organization_id"},
            'organization_memberships': {"id", "organization_id"},
            'users': {"id", "boardId"}
        }

    def expected_automatic_fields(self):
        return {
            'actions': {"id", "date"},
            'boards': {"id"},
            'board_custom_fields': {"id", "boardId"},
            'board_labels': {"id", "boardId"},
            'board_memberships': {"id", "boardId"},
            'cards': {'id'},
            'card_attachments': {"id", "card_id"},
            'card_custom_field_items': {"id", "card_id"},
            'checklists': {'id'},
            'lists': {"id"},
            'members': {"id"},
            'organizations': {"id"},
            'organization_actions': {"id", "organization_id", "date"},
            'organization_members': {"id", "organization_id"},
            'organization_memberships': {"id", "organization_id"},
            'users': {"id", "boardId"}
        }

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """
        Select all streams and optionally all fields within streams.

        Args:
            conn_id: Connection ID
            catalogs: List of catalogs to select
            select_all_fields: If False, only select automatic fields
        """
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
