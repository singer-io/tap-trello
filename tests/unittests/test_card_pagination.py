import unittest
from unittest import mock
from unittest.mock import Mock
import requests
from unittest.case import TestCase
from tap_trello.client import LOGGER, TrelloClient
from tap_trello.streams import Cards

@mock.patch('tap_trello.client.requests.request')
class TestMaxAPIResponseSizeValue(unittest.TestCase):
    '''
    Test that the max_api_response_size param is set correctly based on different config values
    '''
    def test_param_value_from_config_for_cards(self, mock_request):
        '''
        Test that when config parameter `max_api_response_size_card` is passed the params is set correctly from the config value
        '''
        config = {"start_date": "dummy_st","access_token": "dummy_at", "access_token_secret": "dummy_as", "consumer_key": "dummy_ck", "consumer_secret": "dummy_cs", "max_api_response_size_card": 200}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(card.params['limit'], 200)

    def test_default_param_value_for_cards(self, mock_request):
        '''
        Test that when no config value is provided for `max_api_response_size_card`, then default is takn as 5000
        '''
        config = {"start_date": "dummy_st","access_token": "dummy_at", "access_token_secret": "dummy_as", "consumer_key": "dummy_ck", "consumer_secret": "dummy_cs"}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(card.params['limit'], 5000)

    def test_empty_string_in_config(self, mock_request):
        '''
        Test that when empty string value is provided for the config param `max_api_response_size_card`, the default
        value is taken and passed as 5000
        '''
        config = {"start_date": "dummy_st","access_token": "dummy_at", "access_token_secret": "dummy_as", "consumer_key": "dummy_ck", "consumer_secret": "dummy_cs", "max_api_response_size_card": ""}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(card.params['limit'], 5000)

    def test_string_value_in_config(self, mock_request):
        '''
        Test that when a string value is passe in the config param `max_api_response_size_card`, it is converted to integer
        and then passed
        '''
        config = {"start_date": "dummy_st","access_token": "dummy_at", "access_token_secret": "dummy_as", "consumer_key": "dummy_ck", "consumer_secret": "dummy_cs", "max_api_response_size_card": "300"}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        self.assertEqual(card.params['limit'], 300)

def mocked_get(*args, **kwargs):
    '''
    Fake response object for the requests.request() used in the client
    '''
    fake_response = requests.models.Response()
    fake_response.headers.update(kwargs.get('headers', {}))
    fake_response.status_code = kwargs['status_code']

    # We can't set the content or text of the Response directly, so we mock a function
    fake_response.json = Mock()
    fake_response.json.side_effect = lambda:kwargs.get('json', {})
    fake_response.raise_for_status = Mock()
    fake_response.raise_for_status.side_effect = None

    return fake_response

def mock_build_custom_fields(**kwargs):
    return [], []

def mock_modify_rec(record, **kwargs):
    return record
    
class TestCardPagination(unittest.TestCase):
    '''
    Test that pagination works correctly for cards
    '''
    @mock.patch('tap_trello.client.requests.request', side_effect = [
        mocked_get(status_code=200, json=[{"id": "60ca516249f04d4221b33450"}, {"id":"61973e91b41fcf475f84b351"}]), # 2 records for the first API call indicating 1st page
        mocked_get(status_code=200, json=[{"id":"60c901a838d5f63c42d22044"}])]) # 1 record in the second API call indicating 2nd page with one record
    @mock.patch('tap_trello.client.TrelloClient._get_member_id')
    @mock.patch('tap_trello.streams.AddCustomFields.build_custom_fields_maps')
    @mock.patch('tap_trello.streams.AddCustomFields.modify_record')
    def test_pagination_on_cards(self, mock_modify_record, mock_custom_fields, mock_get_id, mock_request):
        '''
        Test that the pagination works correctly setting the page size as 2, so getting a total of 3 records
        in the actual response
        '''
        mock_custom_fields.side_effect = mock_build_custom_fields
        mock_modify_record.side_effect = mock_modify_rec
        # config with the `max_api_response_size_card` param set as 2
        config = {"start_date": "dummy_st","access_token": "dummy_at", "access_token_secret": "dummy_as", "consumer_key": "dummy_ck", "consumer_secret": "dummy_cs", "max_api_response_size_card": 2}
        client = TrelloClient(config)
        card = Cards(client, config, {})
        cards = list(card.get_records(['dummy']))
        # a total of 3 records from the first call with 2 records as the `max_api_response_size_card` is set to 2
        # and the second API call with one record indicating the break in the while loop
        self.assertEqual(len(cards), 3)