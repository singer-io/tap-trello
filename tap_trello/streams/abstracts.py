from abc import ABC, abstractmethod
import json
import operator
from datetime import datetime, timedelta
from itertools import dropwhile
from typing import Any, Dict, Tuple, List, Iterator, Optional

import singer
from singer import (Transformer, get_bookmark, get_logger, metadata, metrics,
                    write_bookmark, write_record, write_schema, utils)

LOGGER = get_logger()

# NB: We've observed that Trello will only return 50 actions, this is to sub-paginate
MAX_API_RESPONSE_SIZE = 50


class OrderChecker:
    """ Class with context manager to check ordering of values. """
    order = None
    _last_value = None
    check_paired_order = None

    def __init__(self, order='ASC'):
        self.order = order
        self.check_paired_order = operator.lt if order == 'ASC' else operator.gt

    def check_order(self, current_value):
        """
        We are sub-paginating based on a sort order descending assumption for
        Actions, this ensures that this holds up.
        """
        if self._last_value is None:
            self._last_value = current_value

        if self.check_paired_order(current_value, self._last_value):
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


class Unsortable:
    """
    Marker class to identify Stream subclasses that are unsortable.
    NB: No current functionality, but we thought it was useful for higher-order declarative behavior.
    """


class DateWindowPaginated:
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

    def get_window_state(self):
        window_start = get_bookmark(self.state, self.stream_id, 'window_start')
        sub_window_end = get_bookmark(self.state, self.stream_id, 'sub_window_end')
        window_end = get_bookmark(self.state, self.stream_id, 'window_end')

        # adjusting window to lookback 1 day
        adjusted_window_start = utils.strftime(utils.strptime_to_utc(window_start)-timedelta(days=1))

        start_date = self.config.get('start_date')
        end_date = self.config.get('end_date', window_end)

        window_start = utils.strptime_to_utc(max(adjusted_window_start, start_date))
        sub_window_end = sub_window_end and utils.strptime_to_utc(min(sub_window_end, end_date))
        window_end = utils.strptime_to_utc(min(window_end, end_date))

        return window_start, sub_window_end, window_end

    def on_window_started(self):
        if get_bookmark(self.state, self.stream_id, 'sub_window_end') is None:
            if get_bookmark(self.state, self.stream_id, 'window_start') is None:
                write_bookmark(self.state, self.stream_id, "window_start", self.config.get('start_date'))
            if get_bookmark(self.state, self.stream_id, 'window_end') is None:
                now = utils.strftime(utils.now())
                write_bookmark(self.state, self.stream_id, "window_end", min(self.config.get('end_date', now), now))
        singer.write_state(self.state)

    def on_window_finished(self):
        # Set window_start to current window_end
        window_start = get_bookmark(self.state, self.stream_id, "window_end")
        write_bookmark(self.state, self.stream_id, "window_start", window_start)
        singer.clear_bookmark(self.state, self.stream_id, "window_end")
        singer.write_state(self.state)

    def get_records(self, format_values):
        """ Overrides the default get_records to provide date_window pagination and bookmarking. """
        window_start, sub_window_end, window_end = self.get_window_state()
        window_start -= timedelta(milliseconds=1) # To make start inclusive

        if sub_window_end is not None:
            for rec in self.paginate_window(window_start, sub_window_end, format_values):
                yield rec
        else:
            for rec in self.paginate_window(window_start, window_end, format_values):
                yield rec


    def update_bookmark(self, key, value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_id, key, utils.strftime(value)
        )

    def paginate_window(self, window_start, window_end, format_values):
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
                self.update_bookmark("sub_window_end", sub_window_end)
                singer.write_state(self.state)
            else:
                LOGGER.info("%s - Finished syncing between %s and %s",
                            self.stream_id,
                            utils.strftime(window_start),
                            window_end)
                singer.bookmarks.clear_bookmark(self.state, self.stream_id, "sub_window_end")
                break


class LegacyStream:
    """
    Legacy base class for Trello streams (pre-refactor).

    This class provides the original sync pattern used by Boards and parent streams.

    For new streams, prefer using BaseStream, IncrementalStream, or FullTableStream.
    This class is maintained for backward compatibility with existing streams.
    """
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


    def get_format_values(self):
        return []

    def _format_endpoint(self, format_values):
        return self.endpoint.format(*format_values)

    def modify_record(self, record, **kwargs): # pylint: disable=unused-argument
        return record

    def build_custom_fields_maps(self, **kwargs): # pylint: disable=unused-argument
        return {}, {}

    def get_records(self, format_values, additional_params=None):
        if additional_params is None:
            additional_params = {}

        custom_fields_map, dropdown_options_map = self.build_custom_fields_maps(parent_id_list=format_values)

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
            yield self.modify_record(rec, parent_id_list = format_values, custom_fields_map = custom_fields_map, dropdown_options_map = dropdown_options_map)


    def sync(self):
        for rec in self.get_records(self.get_format_values()):
            yield rec


class LegacyChildStream(LegacyStream):
    """
    Legacy base class for child streams (pre-refactor).

    This class extends LegacyStream to handle parent-child relationships where
    child records are fetched for each parent ID (e.g., Lists/Cards for each Board).
    It manages parent_id bookmarking for resumable syncs.

    For new child streams, prefer using ChildBaseStream.
    This class is maintained for backward compatibility with existing child streams.
    """
    parent = LegacyStream

    def get_parent_ids(self, parent):
        # Will request for IDs of parent stream (boards currently)
        # and yield them to be used in child's sync
        LOGGER.info("%s - Retrieving IDs of parent stream: %s",
                    self.stream_id,
                    self.parent.stream_id)
        for parent_obj in parent.get_records(parent.get_format_values(), additional_params={"fields": "id"}):
            yield parent_obj['id']

    def _sort_parent_ids_by_created(self, parent_ids):
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
        parent = self.parent(self.client, self.config, self.state)

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


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    page_size = 1000
    next_page_key = "page"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    children = []
    parent = ""
    data_key = ""
    parent_bookmark_key = ""
    http_method = "GET"
    bookmark_value = None

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict() if catalog else {}
        self.metadata = metadata.to_map(catalog.metadata) if catalog else {}
        self.child_to_sync = []
        self.params = {}
        self.data_payload = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> List:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def get_records(self) -> Iterator:
        """Interacts with api client interaction and pagination."""
        self.params["page"] = self.page_size
        next_page = 1

        while next_page:
            response = self.client.make_request(
                self.http_method,
                self.url_endpoint,
                self.params,
                self.headers,
                body=json.dumps(self.data_payload),
                path=self.path
            )
            raw_records, next_page = self._normalize_response(response, self.url_endpoint)

            if next_page:
                self.params[self.next_page_key] = next_page

            yield from raw_records

    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs) -> None:
        """
        Update JSON body for the stream
        """
        self.data_payload.update(kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict: # pylint: disable=unused-argument
        """
        Modify the record before writing to the stream
        """
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str: # pylint: disable=unused-argument
        """
        Get the URL endpoint for the stream
        """
        if not parent_obj:
            return self.url_endpoint or f"{self.client.base_url}/{self.path}"

        parent_id = None
        if isinstance(parent_obj, dict):
            parent_id = parent_obj.get('id')

            if not parent_id:
                for key in ['idOrganization', 'idBoard', 'boardId', 'organization_id', 'organizationId']:
                    parent_id = parent_obj.get(key)
                    if parent_id:
                        break

        if not parent_id:
            LOGGER.warning(
                "Could not extract parent id for %s from parent_obj keys: %s",
                getattr(self, 'tap_stream_id', 'unknown'),
                list(parent_obj.keys()) if isinstance(parent_obj, dict) else type(parent_obj)
            )
            return f"{self.client.base_url}/{self.path}"

        try:
            return f"{self.client.base_url}/{self.path.format(id=parent_id)}"
        except (KeyError, ValueError):
            try:
                return f"{self.client.base_url}/{self.path.format(parent_id)}"
            except Exception as e:
                LOGGER.error(
                    "Failed to format URL for %s with path %s and parent_id %s: %s",
                    getattr(self, 'tap_stream_id', 'unknown'), self.path, parent_id, e
                )
                return f"{self.client.base_url}/{self.path}"

    def _normalize_response(self, response: Any, url: str) -> Tuple[list, Optional[Any]]:
        """
        Normalize different Trello response shapes into (raw_records, next_page).

        Returns a tuple where raw_records is a list of record dicts and next_page or None.
        """
        if isinstance(response, dict):
            # Check if this is a paginated response with a data_key
            if self.data_key and self.data_key in response:
                raw_records = response.get(self.data_key, [])
                next_page = response.get(self.next_page_key)
            else:
                # Handle single record response '/members/{id}' returns one member dict
                raw_records = [response]
                next_page = None
        elif isinstance(response, list):
            raw_records = response
            next_page = None
        else:
            LOGGER.warning(
                "%s - Unexpected response type %s from endpoint %s",
                getattr(self, 'tap_stream_id', str(self.__class__)),
                type(response),
                url,
            )
            raw_records = []
            next_page = None

        return raw_records, next_page


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""


    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(state, stream, key or self.replication_keys[0], self.client.config["start_date"])
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )


    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        self.update_params(updated_since=bookmark_date)
        self.update_data_payload(parent_obj=parent_obj)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                record_bookmark = transformed_record[self.replication_keys[0]]
                if record_bookmark >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_bookmark
                    )

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_keys = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self.update_data_payload(parent_obj=parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value


class ParentBaseStream(IncrementalStream):
    """Base Class for Parent Stream."""

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = (
            super().get_bookmark(state, stream) if self.is_selected() else None
        )
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            child_bookmark = super().get_bookmark(
                state, child.tap_stream_id, key=bookmark_key
            )
            min_parent_bookmark = (
                min(min_parent_bookmark, child_bookmark)
                if min_parent_bookmark
                else child_bookmark
            )

        return min_parent_bookmark

    def write_bookmark(
        self, state: Dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            super().write_bookmark(
                state, child.tap_stream_id, key=bookmark_key, value=value
            )

        return state


class ChildBaseStream(IncrementalStream):
    """Base Class for Child Stream."""

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """Singleton bookmark value for child streams."""
        # Disable pylint access-member-before-definition since bookmark_value is defined at runtime
        if not self.bookmark_value: # pylint: disable=access-member-before-definition
            self.bookmark_value = super().get_bookmark(state, stream)

        return self.bookmark_value


# Backward-compatible aliases for legacy class names
Stream = LegacyStream
ChildStream = LegacyChildStream
