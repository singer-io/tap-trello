from datetime import datetime, timedelta
from itertools import dropwhile
import singer

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
                ("Detected out of order data. In {} sorted stream, current sorted " +
                 "value {} is {} last sorted value {}").format(
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
    stream_id = None
    stream_name = None
    endpoint = None
    key_properties = None
    replication_keys = []
    replication_method = None
    _last_bookmark_value = None
    config = None
    state = None
    client = None
    MAX_API_RESPONSE_SIZE = None
    params = {}

    def _get_window_state(self):
        window_start = singer.get_bookmark(self.state, self.stream_id, 'window_start')
        sub_window_end = singer.get_bookmark(self.state, self.stream_id, 'sub_window_end')
        window_end = singer.get_bookmark(self.state, self.stream_id, 'window_end')

        start_date = self.config.get('start_date')
        end_date = self.config.get('end_date', window_end)

        window_start = utils.strptime_to_utc(max(window_start, start_date))
        sub_window_end = sub_window_end and  utils.strptime_to_utc(min(sub_window_end, end_date))
        window_end = utils.strptime_to_utc(min(window_end, end_date))

        return window_start, sub_window_end, window_end

    def on_window_started(self):
        if singer.get_bookmark(self.state, self.stream_id, 'sub_window_end') is None:
            if singer.get_bookmark(self.state, self.stream_id, 'window_start') is None:
                singer.write_bookmark(self.state, self.stream_id, "window_start", self.config.get('start_date'))
            if singer.get_bookmark(self.state, self.stream_id, 'window_end') is None:
                now = utils.strftime(utils.now())
                singer.write_bookmark(self.state, self.stream_id, "window_end", min(self.config.get('end_date', now), now))
        singer.write_state(self.state)

    def on_window_finished(self):
        # Set window_start to current window_end
        window_start = singer.get_bookmark(self.state, self.stream_id, "window_end")
        singer.write_bookmark(self.state, self.stream_id, "window_start", window_start)
        singer.clear_bookmark(self.state, self.stream_id, "window_end")
        singer.write_state(self.state)

    def get_records(self, format_values):
        """ Overrides the default get_records to provide date_window pagination and bookmarking. """
        window_start, sub_window_end, window_end = self._get_window_state()
        window_start -= timedelta(milliseconds=1) # To make start inclusive

        if sub_window_end is not None:
            for rec in self._paginate_window(window_start, sub_window_end, format_values):
                yield rec
        else:
            for rec in self._paginate_window(window_start, window_end, format_values):
                yield rec


    def _update_bookmark(self, key, value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_id, key, utils.strftime(value)
        )

    def _paginate_window(self, window_start, window_end, format_values):
        sub_window_end = window_end
        while True:
            records = self.client.get(self._format_endpoint(format_values), params={"since": utils.strftime(window_start), # pylint: disable=no-member
                                                                                    "before": utils.strftime(sub_window_end),
                                                                                    **self.params})
            with OrderChecker("DESC") as oc:
                for rec in records:
                    oc.check_order(rec["date"])
                    yield rec

            if len(records) >= self.MAX_API_RESPONSE_SIZE:
                LOGGER.info("%s - Paginating within date_window %s to %s, due to max records being received.",
                            self.stream_id,
                            utils.strftime(window_start), utils.strftime(sub_window_end))
                # NB: Actions are sorted backwards, so if we get the
                # max_response_size, set the window_end to the last
                # record's timestamp (inclusive) and try again.
                sub_window_end = utils.strptime_to_utc(records[-1]["date"]) + timedelta(milliseconds=1)
                self._update_bookmark("sub_window_end", sub_window_end)
                singer.write_state(self.state)
            else:
                LOGGER.info("%s - Finished syncing between %s and %s",
                            self.stream_id,
                            utils.strftime(window_start),
                            window_end)
                singer.bookmarks.clear_bookmark(self.state, self.stream_id, "sub_window_end")
                break



class Stream:
    stream_id = None
    stream_name = None
    endpoint = None
    key_properties = ["id"]
    replication_keys = []
    replication_method = None
    _last_bookmark_value = None
    MAX_API_RESPONSE_SIZE = None
    params = {}

    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def get_format_values(self): # pylint: disable=no-self-use
        return []

    def _format_endpoint(self, format_values):
        return self.endpoint.format(*format_values)

    def modify_record(self, record, **kwargs): # pylint: disable=no-self-use,unused-argument
        return record

    def get_records(self, format_values, additional_params=None):
        if additional_params is None:
            additional_params = {}

        # Boards, Users, and Lists don't handle an api limit key
        # Passing in None doesn't change the response (no 400 returned)
        records = self.client.get(
            self._format_endpoint(format_values),
            params={
                "limit": self.MAX_API_RESPONSE_SIZE,
                **self.params,
                **additional_params
            })

        if self.MAX_API_RESPONSE_SIZE and len(records) >= self.MAX_API_RESPONSE_SIZE:
            raise Exception(
                ("{}: Number of records returned is greater than max API response size of {}.").format(
                    self.stream_id,
                    self.MAX_API_RESPONSE_SIZE)
            )

        for rec in records:
            yield self.modify_record(rec, parent_id_list = format_values)


    def sync(self):
        for rec in self.get_records(self.get_format_values()):
            yield rec

class AddBoardId(Mixin):
    def modify_record(self, record, **kwargs): # pylint: disable=no-self-use
        boardIdList = kwargs['parent_id_list']
        assert len(boardIdList) == 1
        record["boardId"] = boardIdList[0]
        return record


class ChildStream(Stream):
    parent_class = Stream

    def get_parent_ids(self, parent):
        # Will request for IDs of parent stream (boards currently)
        # and yield them to be used in child's sync
        LOGGER.info("%s - Retrieving IDs of parent stream: %s",
                    self.stream_id,
                    self.parent_class.stream_id)
        for parent_obj in parent.get_records(parent.get_format_values(), additional_params={"fields": "id"}):
            yield parent_obj['id']

    def _sort_parent_ids_by_created(self, parent_ids): # pylint: disable=no-self-use
        # NB This is documented here. Yes it's hacky
        # - https://help.trello.com/article/759-getting-the-time-a-card-or-board-was-created
        parents = [{"id": x, "created": datetime.utcfromtimestamp(int(x[:8], 16))}
                   for x in parent_ids]
        return sorted(parents, key=lambda x: x["created"])

    # TODO: If we need second-level child streams, most of sync needs pulled into get_records for this class

    def on_window_started(self):
        pass

    def on_window_finished(self):
        singer.write_state(self.state)

    def sync(self):
        self.on_window_started()
        parent = self.parent_class(self.client, self.config, self.state)

        # Get the most recent parent ID and resume from there, if necessary
        bookmarked_parent = singer.get_bookmark(self.state, self.stream_id, 'parent_id')
        parent_ids = [p['id'] for p in self._sort_parent_ids_by_created(self.get_parent_ids(parent))]

        if bookmarked_parent and bookmarked_parent in parent_ids:
            # NB: This will cause some rework, but it will guarantee the tap doesn't miss records if interrupted.
            # - If there's too much data to sync all parents in a single run, this API is not appropriate for that data set.
            parent_ids = dropwhile(lambda p: p != bookmarked_parent, parent_ids)
        for parent_id in parent_ids:
            singer.write_bookmark(self.state, self.stream_id, "parent_id", parent_id)
            singer.write_state(self.state)
            for rec in self.get_records([parent_id]):
                yield rec
        singer.clear_bookmark(self.state, self.stream_id, "parent_id")
        self.on_window_finished()


class Boards(Unsortable, Stream):
    # TODO: Should boards respect the start date? i.e., not emit records from before the configured start?
    # TODO: Is this also limited to 50 records per response? If so, might need paginated...
    stream_id = "boards"
    stream_name = "boards"
    endpoint = "/members/{}/boards"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"


    def get_format_values(self):
        return [self.client.member_id]


class Users(Unsortable, AddBoardId, ChildStream):
    # TODO: If a user is added to a board, does the board's dateLastActivity get updated?
    # TODO: Should this assoc the board_id to the user records? Seems pretty useless without it
    stream_id = "users"
    stream_name = "users"
    endpoint = "/boards/{}/members"
    key_properties = ["id", "boardId"]
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
    MAX_API_RESPONSE_SIZE = 1000
    params = {'limit': 1000}

class Cards(ChildStream):
    stream_id = "cards"
    stream_name = "cards"
    endpoint = "/boards/{}/cards/all"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent_class = Boards
    MAX_API_RESPONSE_SIZE = 20000
    params = {'limit': 20000}

class Checklists(ChildStream):
    stream_id = "checklists"
    stream_name = "checklists"
    endpoint = "/boards/{}/checklists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent_class = Boards
    params = {'fields': 'all', 'checkItem_fields': 'all'}


STREAM_OBJECTS = {
    'boards': Boards,
    'users': Users,
    'lists': Lists,
    'actions': Actions,
    'cards': Cards,
    'checklists': Checklists
}
