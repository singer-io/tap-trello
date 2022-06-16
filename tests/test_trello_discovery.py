import re
from tap_tester import connections, menagerie, runner
import os
import unittest
import trello_utils as utils

class TestTrelloDiscovery(unittest.TestCase):
    def setUp(self):
        missing_envs = [x for x in [
            "TAP_TRELLO_CONSUMER_KEY",
            "TAP_TRELLO_CONSUMER_SECRET",
            "TAP_TRELLO_ACCESS_TOKEN",
            "TAP_TRELLO_ACCESS_TOKEN_SECRET",
        ] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_trello_discovery"

    def get_type(self):
        return "platform.trello"

    def get_credentials(self):
        return {
            'consumer_key': os.getenv('TAP_TRELLO_CONSUMER_KEY'),
            'consumer_secret': os.getenv('TAP_TRELLO_CONSUMER_SECRET'),
            'access_token': os.getenv('TAP_TRELLO_ACCESS_TOKEN'),
            'access_token_secret': os.getenv('TAP_TRELLO_ACCESS_TOKEN_SECRET'),
        }

    def expected_check_streams(self):
        return {
            'boards',
            'users',
            'lists',
            'actions',
            'cards',
            'checklists'
        }

    def tap_name(self):
        return "tap-trello"


    def get_properties(self):
        return {
            'start_date' : '2020-03-01T00:00:00Z'
        }

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.
        • Verify number of actual streams discovered matches expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow the naming convention
          streams should only have lowercase alphas and underscore
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys are given the inclusion of automatic.
        • verify that unsupported fields are marked as unsupported in metadata.
        • verify that all other fields have the inclusion of available metadata.
        """
        streams_to_test = self.expected_check_streams()

        conn_id = connections.ensure_connection(self)

        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify ensure the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = utils.get_primary_keys(stream)
                expected_replication_keys = utils.get_replication_keys(stream)
                expected_automatic_fields = expected_primary_keys | expected_replication_keys
                expected_replication_method = utils.get_replication_method(stream)
                expected_unsupported_fields = utils.get_unsupported_fields(stream)

                # collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(
                    stream_properties[0].get(
                        "metadata", {"table-key-properties": []}).get("table-key-properties", [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {"valid-replication-keys": []}).get("valid-replication-keys", [])
                )

                actual_replication_method = stream_properties[0].get(
                    "metadata", {"forced-replication-method": None}).get("forced-replication-method")

                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )

                actual_unsupported_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "unsupported"
                )

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                # verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg = "duplicates in the metadata entries retrieved")

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify replication key(s) match expectations
                self.assertSetEqual(
                    expected_replication_keys, actual_replication_keys
                )

                # verify primary key(s) match expectations
                self.assertSetEqual(
                    expected_primary_keys, actual_primary_keys,
                )

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if actual_replication_keys:
                    self.assertEqual("INCREMENTAL", actual_replication_method)
                else:
                    self.assertEqual("FULL_TABLE", actual_replication_method)

                # verify the replication method matches our expectations
                self.assertEqual(
                    expected_replication_method, actual_replication_method
                )

                # verify that primary keys and replication keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # verify that unsupported fields are marked as unsupported in metadata
                self.assertEqual(expected_unsupported_fields, actual_unsupported_fields)

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in (actual_automatic_fields | actual_unsupported_fields)}),
                    msg="Not all non key properties are set to available in metadata")


if __name__ == '__main__':
    unittest.main()
