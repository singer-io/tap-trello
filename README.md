# tap-trello

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [Trello API].
- Extracts the following resources:
    - [BoardMemberships](https://developer.atlassian.com/cloud/trello/rest/api-group-boards/#api-boards-id-memberships-get)

    - [BoardCustomFields](https://developer.atlassian.com/cloud/trello/rest/api-group-boards/#api-boards-id-customfields-get)

    - [BoardLabels](https://developer.atlassian.com/cloud/trello/rest/api-group-boards/#api-boards-id-labels-get)

    - [CardAttachments](https://developer.atlassian.com/cloud/trello/rest/api-group-cards/#api-cards-id-attachments-get)

    - [CardCustomFieldItems](https://developer.atlassian.com/cloud/trello/rest/api-group-cards/#api-cards-id-customfielditems-get)

    - [Members](https://developer.atlassian.com/cloud/trello/rest/api-group-members/#api-members-id-get)

    - [Organizations](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-get)

    - [OrganizationActions](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-actions-get)

    - [OrganizationMembers](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-members-get)

    - [OrganizationMemberships](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-memberships-get)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


**[board_memberships](https://developer.atlassian.com/cloud/trello/rest/api-group-boards/#api-boards-id-memberships-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[board_custom_fields](https://developer.atlassian.com/cloud/trello/rest/api-group-boards/#api-boards-id-customfields-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[board_labels](https://developer.atlassian.com/cloud/trello/rest/api-group-boards/#api-boards-id-labels-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[card_attachments](https://developer.atlassian.com/cloud/trello/rest/api-group-cards/#api-cards-id-attachments-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[card_custom_field_items](https://developer.atlassian.com/cloud/trello/rest/api-group-cards/#api-cards-id-customfielditems-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[members](https://developer.atlassian.com/cloud/trello/rest/api-group-members/#api-members-id-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[organizations](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[organization_actions](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-actions-get)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[organization_members](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-members-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[organization_memberships](https://developer.atlassian.com/cloud/trello/rest/api-group-organizations/#api-organizations-id-memberships-get)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE



## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-trello
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json

    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-trello <api_user_email@your_company.com>`
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.

    ```json
    {
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-trello <api_user_email@your_company.com>",
        "request_timeout": 300
    }
    ```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
            "export": "2019-09-27T22:34:39.000000Z",
            "funnels": "2019-09-28T15:30:26.000000Z",
            "revenue": "2019-09-28T18:23:53Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-trello --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-trello --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-trello --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-trello --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    While developing the trello tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_trello -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap_trello --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2020–2025 Stitch
