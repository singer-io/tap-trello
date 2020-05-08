import singer
from datetime import datetime, timedelta
from itertools import dropwhile

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
    sync_start = None
    def _get_window_state(self):
        window_state = {}

        start_date = self.config['start_date']
        window_start_bookmark = singer.get_bookmark(self.state, self.stream_id, 'window_start') or start_date
        window_start = utils.strptime_to_utc(max(window_start_bookmark, start_date))

        window_sub_end = singer.get_bookmark(self.state, self.stream_id, 'window_sub_end')
        window_sub_end = window_sub_end and utils.strptime_to_utc(window_sub_end)

        # NB: window_end identifies either the first window's end_date, or the next window's start_date
        # - If it's not present, this indicates that we're on our first sync.
        next_window_start = singer.get_bookmark(self.state, self.stream_id, 'next_window_start')
        next_window_start = next_window_start and utils.strptime_to_utc(next_window_start)

        if window_sub_end is not None and next_window_start is None:
            raise Exception("Invalid State: window_sub_end provided without a next_window_start.")

        return window_start, window_sub_end, next_window_start

    def sync_started(self):
        # Create a sync_start value here, store in state
    
    def sync_finished(self):
        # Upgrade sync_start to a bookmark here to be used next time through to override `start_date`.

    
    def get_records(self, format_values, params=None):
        """ Overrides the default get_records to provide date_window pagination and bookmarking. """
        # TODO: Pre-task to check if there is a "now" written already and use the min?
        if params is None:
            params = {}

        self.sync_start = utils.strptime(singer.get_bookmark(self.state, self.stream_id, 'sync_start', utils.strftime(utils.now())))
        window_start, window_sub_end, next_window_start = self._get_window_state()
        window_start -= timedelta(milliseconds=1) # To make start inclusive

        if window_sub_end is not None:
            # Indicates we need to resume, next_window_start should be present
            next_window_start -= timedelta(milliseconds=1) # To make next start inclusive
            for rec in self._paginate_window(window_start, window_sub_end, format_values, params):
                yield rec

        # Sync records since previous window
        # next_window_start indicates we resumed a sync, if not present, then we use window_start
        next_window_start = next_window_start or window_start

        now = utils.strftime(self.sync_start)
        next_window_end = utils.strptime_to_utc(min(self.config.get('end_date', now), now))

        self._update_bookmark("next_window_start", next_window_end)

        for rec in self._paginate_window(next_window_start, next_window_end, format_values, params):
            yield rec

        # Set the final end to the next start
        self._update_bookmark("window_start", next_window_end)
        singer.bookmarks.clear_bookmark(self.state, self.stream_id, "next_window_start")
        singer.write_state(self.state)


    def _update_bookmark(self, key, value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_id, key, utils.strftime(value)
        )

    def _paginate_window(self, window_start, window_end, format_values, params):
        self._update_bookmark("window_start", window_start)
        self._update_bookmark("window_sub_end", window_end)
        singer.write_state(self.state)
        
        while True:
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
                window_end = utils.strptime_to_utc(records[-1]["date"]) + timedelta(milliseconds=1)
                self._update_bookmark("window_sub_end", window_end)
                singer.write_state(self.state)
            else:
                singer.bookmarks.clear_bookmark(self.state, self.stream_id, "window_start")
                singer.bookmarks.clear_bookmark(self.state, self.stream_id, "window_sub_end")
                singer.write_state(self.state)
                break


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

    def _sort_parent_ids_by_created(self, parent_ids):
        # NB This is documented here. Yes it's hacky
        # - https://help.trello.com/article/759-getting-the-time-a-card-or-board-was-created
        parents = [{"id": x, "created": datetime.utcfromtimestamp(int(x[:8], 16))}
                   for x in parent_ids]
        return sorted(parents, key=lamba x: x["created"])
        
    # TODO: If we need second-level child streams, most of sync needs pulled into get_records for this class

    def sync(self):
        parent = self.parent_class(self.client, self.config, self.state)

        # Ensure that start time of this stream's sync is bookmarked
        sync_start = utils.strptime(singer.get_bookmark(self.state, self.stream_id, 'sync_start', utils.strftime(utils.now())))
        singer.write_bookmark(self.state, self.stream_id, 'sync_start', utils.strftime(sync_start))
        singer.write_state(self.state)

        # Get the most recent parent ID and resume from there, if necessary
        bookmarked_parent = singer.get_bookmark(self.state, self.stream_id, 'parent_id')
        parent_ids = [p['id'] for p in self._sort_parent_ids_by_created(self.get_parent_ids(parent))]

        if bookmarked_parent and bookmarked_parent in parent_ids:
            # NB: This will cause some rework, but it will guarantee the tap doesn't miss records if interrupted.
            # - If there's too much data to sync all parents in a single run, this API is not appropriate for that data set.
            parent_ids = dropwhile(lambda p: parent_id != bookmarked_parent,
                                  parent_ids)
        for parent_id in parent_ids:
            # --- Clear State
            # --- Add parent_id for next time
            # Clear all but sync_start and parent_id
            singer.write_bookmark(self.state, self.stream_id, "parent_id", parent_id)
            for rec in self.get_records([parent_id]):
                yield rec
        singer.clear_bookmark(self.state, self.stream_id, "sync_start")
        singer.clear_bookmark(self.state, self.stream_id, "parent_id")
        self.sync_finished()
        # -- Write child's bookmark to "now", maybe ask self what that means?
        # - e.g., self.write_bookmark_to_now() or something similar where we write the "earliest now that was written"
        # -- And remove the parent_id bookmark (we're done)



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
