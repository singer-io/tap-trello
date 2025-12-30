import unittest
from unittest.mock import patch, MagicMock

from tap_trello.sync import write_schema, sync, update_currently_syncing


class TestSync(unittest.TestCase):

    def test_write_schema_only_parent_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = True
        mock_stream.children = ["board_labels", "board_memberships"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, [], catalog)

        mock_stream.write_schema.assert_called_once()
        self.assertEqual(len(mock_stream.child_to_sync), 0)

    def test_write_schema_parent_child_both_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = True
        mock_stream.children = ["board_labels", "board_memberships"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, ["board_memberships"], catalog)

        mock_stream.write_schema.assert_called_once()
        self.assertEqual(len(mock_stream.child_to_sync), 1)

    def test_write_schema_child_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = False
        mock_stream.children = ["board_labels", "board_memberships"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, ["board_labels", "board_memberships"], catalog)

        self.assertEqual(mock_stream.write_schema.call_count, 0)
        self.assertEqual(len(mock_stream.child_to_sync), 2)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_trello.streams.abstracts.LegacyStream.sync")
    @patch("singer.write_record")
    def test_sync_stream1_called(self, mock_write_record, mock_sync, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema):
        mock_catalog = MagicMock()
        board_stream = MagicMock()
        board_stream.stream = "boards"
        card_stream = MagicMock()
        card_stream.stream = "cards"
        mock_catalog.get_selected_streams.return_value = [
            board_stream,
            card_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        mock_sync.return_value = iter([{}])

        sync(client, config, mock_catalog, state)

        self.assertEqual(mock_sync.call_count, 1)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_trello.streams.abstracts.LegacyStream.sync")
    @patch("singer.write_record")
    def test_sync_child_selected(self, mock_write_record, mock_sync, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema):
        mock_catalog = MagicMock()
        board_stream = MagicMock()
        board_stream.stream = "boards"
        card_stream = MagicMock()
        card_stream.stream = "cards"
        mock_catalog.get_selected_streams.return_value = [
            board_stream,
            card_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        mock_sync.return_value = iter([{}])

        sync(client, config, mock_catalog, state)

        self.assertEqual(mock_sync.call_count, 1)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_remove_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = "some_stream"
        state = {"currently_syncing": "some_stream"}

        update_currently_syncing(state, None)

        mock_get_currently_syncing.assert_called_once_with(state)
        mock_set_currently_syncing.assert_not_called()
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_set_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = None
        state = {}

        update_currently_syncing(state, "new_stream")

        mock_get_currently_syncing.assert_not_called()
        mock_set_currently_syncing.assert_called_once_with(state, "new_stream")
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state)
