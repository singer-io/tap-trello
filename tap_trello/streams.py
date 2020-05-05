import singer

class FullTable():
    """
    Mixin class to override Stream implementations for FullTable streams.
    e.g.:
        class MyStream(FullTable, Stream):
            # Specify properties, implement unique things
    """
    replication_keys = []
    replication_method = "FULL_TABLE"

    def update_bookmark(self, bookmark_value):
        """Defines the stream as non-bookmarkable"""

class Unsortable():
    """
    Mixin class to override Stream implementations for FullTable streams.
    e.g.:
        class MyStream(Unsortable, Stream):
            # Specify properties, implement unique things
    """
    def check_order(self, current_bookmark_value):
        """Defines this stream as not sortable """


class Stream:
    stream_id = None
    stream_name = None
    data_key = None
    endpoint = None
    key_properties = ["id"]
    replication_keys = []
    replication_method = "INCREMENTAL" # Default, override with "FullTable" mixin
    _last_bookmark_value = None

    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def get_params(self): # pylint: disable=no-self-use
        """ To be overriden in child. """
        return {}


    def get_format_values(self):
        return []

    def format_endpoint(self, format_values):
        return self.endpoint.format(*format_values)


    def update_bookmark(self, bookmark_value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_name, self.replication_keys[0], bookmark_value
        )
        singer.write_state(self.state)


    def check_order(self, current_bookmark_value):
        if self._last_bookmark_value is None:
            self._last_bookmark_value = current_bookmark_value

        if current_bookmark_value < self._last_bookmark_value:
            raise Exception(
                "Detected out of order data. Current bookmark value {} is less than last bookmark value {}".format(
                    current_bookmark_value, self._last_bookmark_value
                )
            )

        self._last_bookmark_value = current_bookmark_value


    def get_records(self, format_values):
        records = self.client.get(self.format_endpoint(format_values), **self.get_params())

        return records


    def should_yield(self, _): # pylint: disable=no-self-use
        """ To be overridden in child, if parent not selected. (e.g., set `emit` = False on parent obj) """
        return True


    def sync(self):
        for rec in self.get_records(self.get_format_values()):
            current_bookmark_value = rec[self.replication_keys[0]]
            self.check_order(current_bookmark_value)
            self.update_bookmark(current_bookmark_value)
            if self.should_yield(rec):
                yield rec


class ChildStream(Stream):
    parent_class = None

    def get_parent_ids(self, parent):
        # Will request for IDs of parent stream (boards currently)
        # and yield them to be used in child's sync
        # TODO: Can we filter on id for the Boards call?
        for parent_obj in parent.get_records(parent.get_format_values()):
            yield parent_obj['id']

    # TODO: If we need second-level child streams, most of sync needs pulled into get_records for this class

    def sync(self):
        parent = self.parent_class(self.client, self.config, self.state)
        for parent_id in self.get_parent_ids(parent):
            # Get users for "parent_id" (aka board_id)
            for rec in self.get_records([parent_id]):
                if self.replication_keys:
                    current_bookmark_value = rec[self.replication_keys[0]]
                    self.check_order(current_bookmark_value)
                    self.update_bookmark(current_bookmark_value)
                if self.should_yield(rec):
                    yield rec



class Boards(Unsortable, Stream):
    stream_id = "boards"
    stream_name = "boards"
    data_key = None
    endpoint = "/members/{}/boards"
    key_properties = ["id"]
    replication_keys = ["dateLastActivity"]

    def should_yield(self, rec):
        current_state_value = singer.bookmarks.get_bookmark(self.state, self.stream_name, self.replication_keys[0])
        return current_state_value <= rec[self.replication_keys[0]]

    def update_bookmark(self, bookmark_value):
        current_state_value = singer.bookmarks.get_bookmark(self.state, self.stream_name, self.replication_keys[0])
        if current_state_value is None or current_state_value < bookmark_value:
            singer.bookmarks.write_bookmark(
                self.state, self.stream_name, self.replication_keys[0], bookmark_value
            )
            singer.write_state(self.state)

    def get_format_values(self):
        return [self.client.member_id]


class Users(FullTable, Unsortable, ChildStream):
    # TODO: If a user is added to a board, does the board's dateLastActivity get updated?
    # TODO: Should this assoc the board_id to the user records? Seems pretty useless without it
    stream_id = "users"
    stream_name = "users"
    data_key = None
    endpoint = "/boards/{}/members"
    key_properties = ["id"]
    parent_class = Boards



STREAM_OBJECTS = {
    'boards': Boards,
    'users': Users
}
