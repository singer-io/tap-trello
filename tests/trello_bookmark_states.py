
ACTIONS = {
    "state_0": { "sync_end": 0, "parent_id": 0, "window_start": 0, "window_sub_end": 0, "next_window_start": 0 },
    "state_1": { "last_sync_end": 0,  "sync_end": 0, "parent_id": 0, "window_start": 0, "window_sub_end": 0, "next_window_start": 0 },
    "state_2": { "last_sync_end": 0, "sync_end": 0,"parent_id": 0 },
    "state_3": { "sync_end": 0, "parent_id": 0}
}

def get_states_formatted(index: int):

    state_index = "state_{}".format(index)
    return { "bookmarks": { "actions": ACTIONS[state_index], "boards": dict(), "cards": dict(), "lists": dict(), "users": dict() } }

if __name__ == "__main__":
    print("Testing states")
    for i in range(4):
        print("\n")
        print(get_states_formatted(i))

