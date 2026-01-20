import os

from tap_tester.base_suite_tests.base_case import BaseCase


class TrelloBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2025-12-01T00:00:00Z"
    PARENT_TAP_STREAM_ID = "parent-tap-stream-id"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-trello"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.trello"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "actions": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "date" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            },
            "boards": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "board_custom_fields": {
                cls.PRIMARY_KEYS: { "id", "boardId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            },
            "board_labels": {
                cls.PRIMARY_KEYS: { "id", "boardId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            },
            "board_memberships": {
                cls.PRIMARY_KEYS: { "id", "boardId" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            },
            "cards": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            },
            "card_attachments": {
                cls.PRIMARY_KEYS: { "id", "card_id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "cards"
            },
            "card_custom_field_items": {
                cls.PRIMARY_KEYS: { "id", "card_id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "cards"
            },
            "checklists": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            },
            "lists": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            },
            "members": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "users"
            },
            "organizations": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "organization_actions": {
                cls.PRIMARY_KEYS: { "id", "organization_id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "date" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "organizations"
            },
            "organization_members": {
                cls.PRIMARY_KEYS: { "id", "organization_id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "organizations"
            },
            "organization_memberships": {
                cls.PRIMARY_KEYS: { "id", "organization_id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "organizations"
            },
            "users": {
                cls.PRIMARY_KEYS: { "id", "boardId" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000,
                cls.PARENT_TAP_STREAM_ID: "boards"
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {'api_key': 'TAP_TRELLO_API_KEY', 'api_token': 'TAP_TRELLO_API_TOKEN'}

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2022-07-01T00:00:00Z"
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def expected_parent_tap_stream(self, stream=None):
        """return a dictionary with key of table name and value of parent stream"""
        parent_stream = {
            table: properties.get(self.PARENT_TAP_STREAM_ID, None)
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return parent_stream
        return parent_stream[stream]
