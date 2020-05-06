import singer

class NonBookmarked():
    """
    Mixin class to override Stream implementations for streams that can't store a bookmark.
    e.g.:
        class MyStream(NonBookmarked, Stream):
            # Specify properties, implement unique things
    """
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
    endpoint = None
    key_properties = ["id"]
    replication_keys = []
    replication_method = None
    _last_bookmark_value = None

    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def get_params(self): # pylint: disable=no-self-use
        """ To be overriden in child. """
        return {}


    def get_format_values(self): # pylint: disable=no-self-use
        return []

    def _format_endpoint(self, format_values):
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


    def get_records(self, format_values, params=None):
        if params is None:
            params = {}
        records = self.client.get(self._format_endpoint(format_values), {**self.get_params(), **params})

        return records


    def sync(self):
        for rec in self.get_records(self.get_format_values()):
            if self.replication_keys:
                current_bookmark_value = rec[self.replication_keys[0]]
                self.check_order(current_bookmark_value)
                self.update_bookmark(current_bookmark_value)
            yield rec


class ChildStream(Stream):
    parent_class = Stream

    def get_parent_ids(self, parent): # pylint: disable=no-self-use
        # Will request for IDs of parent stream (boards currently)
        # and yield them to be used in child's sync
        for parent_obj in parent.get_records(parent.get_format_values(), params={"fields": "id"}):
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
                yield rec



class Boards(NonBookmarked, Unsortable, Stream):
    # TODO: Should boards respect the start date? i.e., not emit records from before the configured start?
    stream_id = "boards"
    stream_name = "boards"
    endpoint = "/members/{}/boards"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"


    def get_format_values(self):
        return [self.client.member_id]


class Users(NonBookmarked, Unsortable, ChildStream):
    # TODO: If a user is added to a board, does the board's dateLastActivity get updated?
    # TODO: Should this assoc the board_id to the user records? Seems pretty useless without it
    stream_id = "users"
    stream_name = "users"
    endpoint = "/boards/{}/members"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    parent_class = Boards


class Lists(NonBookmarked, Unsortable, ChildStream):
    # TODO: If a list is added to a board, does the board's dateLastActivity get updated?
    stream_id = "lists"
    stream_name = "lists"
    endpoint = "/boards/{}/lists"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    parent_class = Boards



STREAM_OBJECTS = {
    'boards': Boards,
    'users': Users,
    'lists': Lists}
