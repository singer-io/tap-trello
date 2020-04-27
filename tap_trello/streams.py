class Stream:
    stream_name = None
    data_key = None
    endpoint = None
    key_properties = ["id"]
    replication_keys = []
    replication_method = None
    

class Foo(Stream):
    """
    Dummy class for discovery
    """


