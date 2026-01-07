from tap_trello.streams.abstracts import Unsortable, ChildStream
from tap_trello.streams.boards import Boards


class Users(Unsortable, ChildStream):
    stream_id = "users"
    stream_name = "users"
    endpoint = "/boards/{}/members"
    key_properties = ["id", "boardId"]
    replication_method = "FULL_TABLE"
    parent = Boards

    def modify_record(self, record, **kwargs):
        """Add boardId to user records."""
        board_id_list = kwargs['parent_id_list']
        if len(board_id_list) != 1:
            raise ValueError(f"Expected exactly one board ID, got {len(board_id_list)}")
        record["boardId"] = board_id_list[0]
        return record
