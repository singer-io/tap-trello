import unittest
from unittest.mock import patch

import requests
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError

from tap_trello.client import Client, _log_backoff, _log_giveup, _sanitize_url
from tap_trello.exceptions import *


default_config = {
    "base_url": "https://api.example.com",
    "request_timeout": 30,
    "api_key": "test_api_key",
    "api_token": "test_api_token",
}

DEFAULT_REQUEST_TIMEOUT = 300


class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""

    def __init__(
        self, status_code, resp = "", content=[""], headers=None, raise_error=True, text={}
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"
        self.url = "https://api.example.com/resource"

    def raise_for_status(self):
        """If an error occur, this method returns a HTTPError object.

        Raises:
            requests.HTTPError: Mock http error.

        Returns:
            int: Returns status code if not error occurred.
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        """Returns a JSON object of the result."""
        return self.text

class TestClient(unittest.TestCase):

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([
        ["empty value", "", DEFAULT_REQUEST_TIMEOUT],
        ["string value", "12", 12.0],
        ["integer value", 10, 10.0],
        ["float value", 20.0, 20.0],
        ["zero value", 0, DEFAULT_REQUEST_TIMEOUT]
    ])
    @patch("tap_trello.client.session")
    def test_client_initialization(self, test_name, input_value, expected_value, mock_session):
        default_config["request_timeout"] = input_value
        client = Client(default_config)
        assert client.request_timeout == expected_value
        assert isinstance(client._session, mock_session().__class__)


    @patch("tap_trello.client.Client._Client__make_request")
    def test_client_get(self, mock_make_request):
        mock_make_request.return_value = {"data": "ok"}
        result = self.client.get("https://api.example.com/resource")
        assert result == {"data": "ok"}
        mock_make_request.assert_called_once()


    @patch("tap_trello.client.Client._Client__make_request")
    def test_client_post(self, mock_make_request):
        mock_make_request.return_value = {"created": True}
        result = self.client.make_request('POST', "https://api.example.com/resource", body={"key": "value"})
        assert result == {"created": True}
        mock_make_request.assert_called_once()

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), TrelloBadRequestError, "A validation exception has occurred."],
        ["401 error", 401, MockResponse(401), TrelloUnauthorizedError, "The access token provided is expired, revoked, malformed or invalid for other reasons."],
        ["403 error", 403, MockResponse(403), TrelloForbiddenError, "You are missing the following required scopes: read"],
        ["404 error", 404, MockResponse(404), TrelloNotFoundError, "The resource you have specified cannot be found."],
        ["409 error", 409, MockResponse(409), TrelloConflictError, "The API request cannot be completed because the requested operation would conflict with an existing item."],
    ])
    def test_make_request_http_failure_without_retry(self, test_name, error_code, mock_response, error, error_message):

        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

        expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
        self.assertEqual(str(e.exception), expected_error_message)

    @parameterized.expand([
        ["429 error", 429, MockResponse(429), TrelloRateLimitError, "The API rate limit for your organisation/application pairing has been exceeded."],
        ["500 error", 500, MockResponse(500), TrelloInternalServerError, "The server encountered an unexpected condition which prevented it from fulfilling the request."],
        ["501 error", 501, MockResponse(501), TrelloNotImplementedError, "The server does not support the functionality required to fulfill the request."],
        ["502 error", 502, MockResponse(502), TrelloBadGatewayError, "Server received an invalid response."],
        ["503 error", 503, MockResponse(503), TrelloServiceUnavailableError, "API service is currently unavailable."],
        ["504 error", 504, MockResponse(504), TrelloGatewayTimeoutError, "The server did not receive a timely response from an upstream server."],
    ])
    @patch("time.sleep")
    def test_make_request_http_failure_with_retry(self, test_name, error_code, mock_response, error, error_message, mock_sleep):

        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
            self.assertEqual(str(e.exception), expected_error_message)
            self.assertEqual(mock_request.call_count, 5)

    @parameterized.expand([
        ["ConnectionResetError", ConnectionResetError],
        ["ConnectionError", ConnectionError],
        ["ChunkedEncodingError", ChunkedEncodingError],
        ["Timeout", Timeout],
    ])
    @patch("time.sleep")
    def test_make_request_other_failure_with_retry(self, test_name, error, mock_sleep):

        with patch.object(self.client._session, "request", side_effect=error) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            self.assertEqual(mock_request.call_count, 5)

    @patch("time.sleep")
    def test_unmapped_5xx_triggers_retry_via_catch_all(self, mock_sleep):
        """Unmapped 5xx codes (e.g., 520) should raise TrelloBackoffError and be retried."""
        mock_response = MockResponse(520)

        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(TrelloBackoffError) as ctx:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            expected_message = "HTTP-error-code: 520, Error: An unexpected server error occurred."
            self.assertEqual(str(ctx.exception), expected_message)
            self.assertEqual(mock_request.call_count, 5)

    @patch("tap_trello.client.LOGGER")
    def test_log_backoff_callback(self, mock_logger):
        """Verify _log_backoff logs a warning with retry details."""
        details = {
            "tries": 2,
            "args": [None, "GET", "https://api.trello.com/1/boards"],
            "wait": 4.0,
            "exception": TrelloInternalServerError("test error"),
        }
        _log_backoff(details)
        mock_logger.warning.assert_called_once()
        log_args = mock_logger.warning.call_args
        formatted = log_args[0][0] % log_args[0][1:]
        self.assertIn("Retry attempt 2", formatted)
        self.assertIn("https://api.trello.com/1/boards", formatted)

    @patch("tap_trello.client.LOGGER")
    def test_log_giveup_callback(self, mock_logger):
        """Verify _log_giveup logs an error with final failure details."""
        details = {
            "tries": 5,
            "args": [None, "GET", "https://api.trello.com/1/boards"],
            "exception": TrelloInternalServerError("test error"),
        }
        _log_giveup(details)
        mock_logger.error.assert_called_once()
        log_args = mock_logger.error.call_args
        formatted = log_args[0][0] % log_args[0][1:]
        self.assertIn("Giving up after 5 tries", formatted)
        self.assertIn("https://api.trello.com/1/boards", formatted)

    @patch("tap_trello.client.LOGGER")
    @patch("time.sleep")
    def test_5xx_error_logged_before_retry(self, mock_sleep, mock_logger):
        """Verify that 5xx errors are logged with sanitized endpoint context."""
        mock_response = MockResponse(503)
        mock_response.url = "https://api.trello.com/1/boards?key=secret_key&token=secret_token"

        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(TrelloServiceUnavailableError):
                self.client._Client__make_request("GET", "https://api.example.com/resource")

        # raise_for_error logs a warning for each 5xx attempt
        warning_calls = [c for c in mock_logger.warning.call_args_list
                         if len(c[0]) > 1 and c[0][1] == 503]
        self.assertGreaterEqual(len(warning_calls), 1)

        # Verify credentials are NOT in the logged URL
        for c in warning_calls:
            logged_url = c[0][2]  # 3rd positional arg is the sanitized URL
            self.assertNotIn("secret_key", logged_url)
            self.assertNotIn("secret_token", logged_url)

    def test_sanitize_url_strips_query_params(self):
        """Verify _sanitize_url removes query parameters containing credentials."""
        url = "https://api.trello.com/1/boards?key=abc123&token=xyz789"
        sanitized = _sanitize_url(url)
        self.assertEqual(sanitized, "https://api.trello.com/1/boards")
        self.assertNotIn("abc123", sanitized)
        self.assertNotIn("xyz789", sanitized)

    def test_sanitize_url_handles_no_query(self):
        """Verify _sanitize_url works correctly when there are no query params."""
        url = "https://api.trello.com/1/boards"
        self.assertEqual(_sanitize_url(url), url)

    def test_sanitize_url_handles_invalid_input(self):
        """Verify _sanitize_url returns 'unknown' for non-string input."""
        self.assertEqual(_sanitize_url(None), "unknown")
