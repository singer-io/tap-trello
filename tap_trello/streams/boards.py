from tap_trello.streams.abstracts import Unsortable, Stream


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
