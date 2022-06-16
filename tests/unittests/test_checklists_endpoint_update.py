from unittest import mock
from tap_trello.client import TrelloClient
from tap_trello.streams import Cards
import unittest

# mock class for CatalogEntey
class MockCatalogEntry:
    def __init__(self, is_selected):
        self.selected = is_selected

    # mock class for is_selected when initializing Cards stream
    def is_selected(self):
        return self.selected

# mock class for Catalog
class MockCatalog:
    def __init__(self, is_selected):
        self.is_selected = is_selected

    # mock class for get_stream
    def get_stream(self, *args, **kwargs):
        return MockCatalogEntry(self.is_selected)

# mock 'get' and return desired data
def mock_get(*args, **kwargs):
    if "/members/me_1/boards" in args[0]:
        return [{"id": "60bfc9f04961e65051dc0711"}]
    elif "/members/me" in args[0]:
        return {"id": "me_1", "created": "2022-01-01T00:00:00Z"}
    elif "/customFields" in args[0]:
        return []
    else:
        return [
            {"id": "card_1", "customFieldItems": [], "checklists": [{"id": "checklists_1.1"}, {"id": "checklists_1.2"}]},
            {"id": "card_2", "customFieldItems": [], "checklists": [{"id": "checklists_2.1"}]}
        ]

@mock.patch("tap_trello.client.TrelloClient.get")
@mock.patch("tap_trello.streams.Checklists.write_checklists_records")
class TestCheckListsEndpointUpdate(unittest.TestCase):
    """
        Test cases for verifying we sync 'checklists' from 'cards' if we have selected 'checklists' stream
    """

    def test_checklists_not_selected(self, mocked_write_checklists_records, mocked_get):
        """
            Test case to verify we did not sync 'checklists' if we have not selected in the catalog
        """
        # mock get
        mocked_get.side_effect = mock_get
        # dummy config
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "access_token": "test_access_token",
            "access_token_secret": "test_access_token_secret",
            "consumer_key": "test_consumer_key",
            "consumer_secret": "test_consumer_secret"
        }

        # create TrelloClient
        client = TrelloClient(config)
        # create cards object
        cards = Cards(client, config, {}, MockCatalog(False))

        # function call
        list(cards.sync())

        # verify we did not call 'write_checklists_records' as we have not selected it
        self.assertEqual(mocked_write_checklists_records.call_count, 0)

    def test_checklists_selected(self, mocked_write_checklists_records, mocked_get):
        """
            Test case to verify we did not sync 'checklists' if we have not selected in the catalog
        """
        # mock get
        mocked_get.side_effect = mock_get
        # dummy config
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "access_token": "test_access_token",
            "access_token_secret": "test_access_token_secret",
            "consumer_key": "test_consumer_key",
            "consumer_secret": "test_consumer_secret"
        }

        # create TrelloClient
        client = TrelloClient(config)
        # create cards object
        cards = Cards(client, config, {}, MockCatalog(True))

        # function call
        list(cards.sync())

        # verify we called 'write_checklists_records' 2 times, as we have 2 records for cards
        self.assertEqual(mocked_write_checklists_records.call_count, 2)
        # create 2 mock calls for assertion
        expected_checklists_call = [
            mock.call([{"id": "checklists_1.1"}, {"id": "checklists_1.2"}]),
            mock.call([{"id": "checklists_2.1"}])
        ]
        # verify we called 'write_checklists_records' with expected arguments
        self.assertEqual(mocked_write_checklists_records.mock_calls, expected_checklists_call)
