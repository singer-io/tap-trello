import singer
import datetime

from singer import utils

LOGGER = singer.get_logger()

class OrderChecker:
    """ Class with context manager to check ordering of values. """
    order = None
    _last_value = None

    def __init__(self, order='ASC'):
        self.order = order

    def check_order(self, current_value):
        """
        We are sub-paginating based on a sort order descending assumption for
        Actions, this ensures that this holds up.
        """
        if self.order == 'ASC':
            check_paired_order = lambda a, b: a < b
        else:
            check_paired_order = lambda a, b: a > b

        if self._last_value is None:
            self._last_value = current_value

        if check_paired_order(current_value, self._last_value):
            asc_desc = "ascending" if self.order == 'ASC' else "descending"
            gt_lt = "less than" if self.order == 'ASC' else "greater than"
            raise Exception(
                "Detected out of order data. In {} sorted stream, current sorted " +
                "value {} is {} last sorted value {}".format(
                    asc_desc,
                    current_value,
                    gt_lt,
                    self._last_value)
            )

        self._last_value = current_value

    def __enter__(self):
        """
        Reset last known value for an `OrderChecker` object used across
        multiple `with` blocks to scope within the current block context.
        """
        self._last_value = None
        return self

    def __exit__(self, *args):
        """ Required for context manager usage. """

class Mixin:
    """ Empty class to mark mixin classes as such. """

class Unsortable(Mixin):
    """
    Mixin class to mark a Stream subclass as Unsortable
    NB: No current functionality, but we thought it was useful for higher-order declarative behavior.
    e.g.:
        class MyStream(Unsortable, Stream):
            # Specify properties, implement unique things
    """


# NB: We've observed that Trello will only return 50 actions, this is to sub-paginate
MAX_API_RESPONSE_SIZE = 50

class DateWindowPaginated(Mixin):
    """
    Mixin class to provide date windowing on the `get_records` requests
    """
    # Default, can be overridden in implementation, the API support fractional days
    date_window = 30
    bookmark_key = 'next_window_start'

    def _get_initial_bookmark(self):
        bookmark = singer.get_bookmark(self.state, self.stream_id, self.bookmark_key)
        if not bookmark:
            return utils.strptime_to_utc(self.config['start_date'])

        return utils.strptime_to_utc(bookmark)

    def update_bookmark(self, bookmark_value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_id, self.bookmark_key, bookmark_value
        )
        singer.write_state(self.state)

    def get_records(self, format_values, params=None):
        if params is None:
            params = {}

        # NB: Both request parameters are exclusive, so this makes the windowing inclusive
        # - The Trello API doesn't appear to go to microsecond precision
        window_start = self._get_initial_bookmark() - datetime.timedelta(milliseconds=1)
        end = utils.now() + datetime.timedelta(milliseconds=1)

        window_offset = datetime.timedelta(days=self.date_window)
        window_end = window_start + window_offset + datetime.timedelta(milliseconds=1)
        initial_end = window_end # Initial end used for resuming post-sub-paginating
        while window_start < end:
            records = self.client.get(self._format_endpoint(format_values), params={"since": utils.strftime(window_start),
                                                                                    "before": utils.strftime(window_end),
                                                                                    **params})
            with OrderChecker("DESC") as oc:
                for rec in records:
                    oc.check_order(rec["date"])
                    yield rec

            if len(records) >= MAX_API_RESPONSE_SIZE:
                LOGGER.info("%s - Paginating within date_window %s to %s, due to max records being received.",
                            self.stream_id,
                            utils.strftime(window_start), utils.strftime(window_end))
                # NB: Actions are sorted backwards, so if we get the
                # max_response_size, set the window_end to the last
                # record's timestamp (inclusive) and try again.
                window_end = utils.strptime_to_utc(records[-1]["date"]) + datetime.timedelta(milliseconds=1)
            else:
                if window_end < end:
                    self.update_bookmark(utils.strftime(window_end))
                elif window_end >= end:
                    self.update_bookmark(utils.strftime(end))

                window_start = initial_end - datetime.timedelta(milliseconds=1)
                window_end = window_start + window_offset + datetime.timedelta(milliseconds=1)
                initial_end = window_end

            # TODO: Store both start/end bookmarks here?


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


    def get_format_values(self): # pylint: disable=no-self-use
        return []

    def _format_endpoint(self, format_values):
        return self.endpoint.format(*format_values)


    def get_records(self, format_values, params=None):
        if params is None:
            params = {}
        records = self.client.get(self._format_endpoint(format_values), params=params)

        return records


    def sync(self):
        for rec in self.get_records(self.get_format_values()):
            yield rec


class ChildStream(Stream):
    parent_class = Stream

    def get_parent_ids(self, parent): # pylint: disable=no-self-use
        # Will request for IDs of parent stream (boards currently)
        # and yield them to be used in child's sync
        LOGGER.info("%s - Retrieving IDs of parent stream: %s",
                    self.stream_id,
                    self.parent_class.stream_id)
        for parent_obj in parent.get_records(parent.get_format_values(), params={"fields": "id"}):
            yield parent_obj['id']

    # TODO: If we need second-level child streams, most of sync needs pulled into get_records for this class

    def sync(self):
        parent = self.parent_class(self.client, self.config, self.state)
        for parent_id in self.get_parent_ids(parent):
            # Get users for "parent_id" (aka board_id)
            for rec in self.get_records([parent_id]):
                yield rec



class Boards(Unsortable, Stream):
    # TODO: Should boards respect the start date? i.e., not emit records from before the configured start?
    stream_id = "boards"
    stream_name = "boards"
    endpoint = "/members/{}/boards"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"


    def get_format_values(self):
        return [self.client.member_id]


class Users(Unsortable, ChildStream):
    # TODO: If a user is added to a board, does the board's dateLastActivity get updated?
    # TODO: Should this assoc the board_id to the user records? Seems pretty useless without it
    stream_id = "users"
    stream_name = "users"
    endpoint = "/boards/{}/members"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent_class = Boards


class Lists(Unsortable, ChildStream):
    # TODO: If a list is added to a board, does the board's dateLastActivity get updated?
    stream_id = "lists"
    stream_name = "lists"
    endpoint = "/boards/{}/lists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent_class = Boards


class Actions(DateWindowPaginated, ChildStream):
    # TODO: If an action is completed on a board, does the board's dateLastActivity get updated?
    stream_id = "actions"
    stream_name = "actions"
    endpoint = "/boards/{}/actions"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    parent_class = Boards


STREAM_OBJECTS = {
    'boards': Boards,
    'users': Users,
    'lists': Lists,
    'actions': Actions,
}
