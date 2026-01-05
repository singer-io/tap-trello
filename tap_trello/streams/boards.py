from tap_trello.streams.abstracts import Unsortable, Stream


class Boards(Unsortable, Stream):
    stream_id = "boards"
    stream_name = "boards"
    endpoint = "/members/{}/boards"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"

    def get_format_values(self):
        return [self.client.member_id]
