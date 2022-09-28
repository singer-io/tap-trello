import unittest
from unittest import mock
from unittest.case import TestCase
from tap_trello.client import TrelloClient
from tap_trello.streams import Cards


DEFAULT_CONFIG = {
    "start_date": "dummy_st",
    "access_token": "dummy_at",
    "access_token_secret": "dummy_as",
    "consumer_key": "dummy_ck",
    "consumer_secret": "dummy_cs",
}

@mock.patch('tap_trello.client.requests.request')
class TestCardsResponseSizeValue(unittest.TestCase):
    '''
    Test that the cards_response_size param is set correctly based on different config values
    '''

    def setUp(self):
        '''Reset the response size to a value in every test'''
        # This is the default in the tap
        self.expected_response_size = 1000

    def test_param_value_from_config_for_cards(self, mock_request):
        '''
        Test that when the config param `cards_response_size` is passed, it is used
        '''
        self.expected_response_size = 200
        config = {**DEFAULT_CONFIG, "cards_response_size": self.expected_response_size}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(self.expected_response_size, card.params['limit'])

    def test_default_param_value_for_cards(self, mock_request):
        '''
        Test that when no config value is provided for the config param `cards_response_size`, then
        the default is used
        '''
        client = TrelloClient(DEFAULT_CONFIG)
        card = Cards(client, DEFAULT_CONFIG, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(self.expected_response_size, card.params['limit'])

    def test_empty_string_in_config(self, mock_request):
        '''
        Test that when empty string value is provided for the config param `cards_response_size`, 
        the default value is used
        '''
        config = {**DEFAULT_CONFIG, "cards_response_size": ""}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(self.expected_response_size, card.params['limit'])

    def test_string_value_in_config(self, mock_request):
        '''
        Test that when a string value is passed in the config param `cards_response_size`, it is 
        converted to integer and then used
        '''
        self.expected_response_size = 300
        config = {**DEFAULT_CONFIG, "cards_response_size": str(self.expected_response_size)}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(self.expected_response_size, card.params['limit'])

def mocked_get(status_code=None, json=None):
    '''
    Fake response object for the requests.request() used in the client
    '''
    fake_response = mock.Mock()
    fake_response.status_code = status_code
    fake_response.json.return_value = json
    fake_response.raise_for_status.return_value = None

    return fake_response


class TestCardPagination(unittest.TestCase):
    '''
    Test that pagination works correctly for cards
    '''
    @mock.patch('tap_trello.client.requests.request')
    def test_pagination_on_cards(self, mock_request):
        '''
        Test that the pagination works correctly setting the page size as 2, so getting a total of 3 records
        in the actual response
        '''

        mock_request.side_effect = [
            # This is a call to `GET /members/me`
            mocked_get(status_code=200, json={"id": 1}),
            # This is a call to `GET /boards/{}/customFields`
            mocked_get(status_code=200, json={}),
            # 2 records for the first API call indicating 1st page
            mocked_get(status_code=200, json=[{"id": "60ca516249f04d4221b33450", "customFieldItems": []},
                                              {"id": "61973e91b41fcf475f84b351", "customFieldItems": []}]),
            # 1 record in the second API call indicating 2nd page with one record
            mocked_get(status_code=200, json=[{"id": "60c901a838d5f63c42d22044", "customFieldItems": []}]),
        ]

        # config with the `cards_response_size` param set as 2
        config = {**DEFAULT_CONFIG, "cards_response_size": 2}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        # a total of 3 records from the first call with 2 records as the `cards_response_size` is set to 2
        # and the second API call with one record indicating the break in the while loop
        self.assertEqual(3, len(cards))
