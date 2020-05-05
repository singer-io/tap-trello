import singer


class Stream:
    stream_id = None
    stream_name = None
    data_key = None
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


    def format_endpoint(self):
        """ Abstract method, to be implemented in child. """


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


    def get_records(self):
        records = self.client.get(self.format_endpoint(), **self.get_params())

        return records


    def should_yield(self, _): # pylint: disable=no-self-use
        """ To be overridden in child, if parent not selected. (e.g., set `emit` = False on parent obj) """
        return True


    def sync(self):
        for rec in self.get_records():
            current_bookmark_value = rec[self.replication_keys[0]]
            self.check_order(current_bookmark_value)
            self.update_bookmark(current_bookmark_value)
            if self.should_yield(rec):
                yield rec


class Boards(Stream):
    stream_id = "boards"
    stream_name = "boards"
    data_key = None
    endpoint = "/members/{}/boards"
    key_properties = ["id"]
    replication_keys = ["dateLastActivity"]
    replication_method = "INCREMENTAL"

    def check_order(self, current_bookmark_value):
        """Boards is not sortable"""

    def format_endpoint(self):
        return self.endpoint.format(self.client.member_id)

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



STREAM_OBJECTS = {
    'boards': Boards
}
