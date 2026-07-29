from unittest.mock import patch, MagicMock

from amiadapters.configuration import secrets
from test.base_test_case import BaseTestCase


class TestSecrets(BaseTestCase):

    @patch("amiadapters.configuration.secrets._create_aws_secrets_manager_client")
    def test_get_secrets_returns_nested_dict(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        mock_client.batch_get_secret_value.return_value = {
            "SecretValues": [
                {
                    "Name": "ami-connect/sinks/my-snowflake",
                    "SecretString": '{"user": "x", "password": "y"}',
                },
                {
                    "Name": "ami-connect/sources/my-source",
                    "SecretString": '{"token": "abc"}',
                },
            ]
        }
        result = secrets.get_secrets()
        self.assertIn("sinks", result)
        self.assertIn("my-snowflake", result["sinks"])
        self.assertEqual(result["sinks"]["my-snowflake"]["user"], "x")
        self.assertIn("sources", result)
        self.assertIn("my-source", result["sources"])
        self.assertEqual(result["sources"]["my-source"]["token"], "abc")

    @patch("amiadapters.configuration.secrets._create_aws_secrets_manager_client")
    def test_get_secrets_follows_pagination(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        # First page returns a NextToken, so get_secrets() must request a second
        # page to retrieve the remaining secret.
        mock_client.batch_get_secret_value.side_effect = [
            {
                "SecretValues": [
                    {
                        "Name": "ami-connect/sinks/my-snowflake",
                        "SecretString": '{"user": "x"}',
                    },
                ],
                "NextToken": "page-2-token",
            },
            {
                "SecretValues": [
                    {
                        "Name": "ami-connect/sources/my-source",
                        "SecretString": '{"token": "abc"}',
                    },
                ],
            },
        ]

        result = secrets.get_secrets()

        # Secrets from both pages are present.
        self.assertEqual(result["sinks"]["my-snowflake"]["user"], "x")
        self.assertEqual(result["sources"]["my-source"]["token"], "abc")

        # Two calls: the second forwards the NextToken from the first.
        self.assertEqual(mock_client.batch_get_secret_value.call_count, 2)
        first_call, second_call = mock_client.batch_get_secret_value.call_args_list
        self.assertNotIn("NextToken", first_call.kwargs)
        self.assertEqual(second_call.kwargs["NextToken"], "page-2-token")
        self.assertEqual(first_call.kwargs["MaxResults"], 20)

    @patch("amiadapters.configuration.secrets._create_aws_secrets_manager_client")
    def test_update_secret_configuration_updates_existing(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        secret = MagicMock()
        secret.to_json.return_value = '{"user": "x"}'
        secrets.update_secret_configuration("sinks", "my-snowflake", secret)
        mock_client.put_secret_value.assert_called_once()
        mock_client.create_secret.assert_not_called()

    @patch("amiadapters.configuration.secrets._create_aws_secrets_manager_client")
    def test_update_secret_configuration_creates_if_not_found(
        self, mock_client_factory
    ):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        secret = MagicMock()
        secret.to_json.return_value = '{"user": "x"}'
        error = MagicMock()
        error.response = {"Error": {"Code": "ResourceNotFoundException"}}
        mock_client.put_secret_value.side_effect = secrets.ClientError(
            error.response, "put_secret_value"
        )
        secrets.update_secret_configuration("sinks", "my-snowflake", secret)
        mock_client.create_secret.assert_called_once()

    @patch("amiadapters.configuration.secrets._create_aws_secrets_manager_client")
    def test_update_secret_configuration_raises_other_errors(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        secret = MagicMock()
        secret.to_json.return_value = '{"user": "x"}'
        error = MagicMock()
        error.response = {"Error": {"Code": "OtherError"}}
        mock_client.put_secret_value.side_effect = secrets.ClientError(
            error.response, "put_secret_value"
        )
        with self.assertRaises(secrets.ClientError):
            secrets.update_secret_configuration("sinks", "my-snowflake", secret)

    @patch("amiadapters.configuration.secrets._create_aws_secrets_manager_client")
    def test_remove_secret_configuration(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        secrets.remove_secret_configuration("sinks", "my-snowflake")
        mock_client.delete_secret.assert_called_once_with(
            SecretId="ami-connect/sinks/my-snowflake",
            ForceDeleteWithoutRecovery=True,
        )

    @patch("amiadapters.configuration.secrets.get_global_aws_profile")
    @patch("amiadapters.configuration.secrets.get_global_aws_region")
    @patch("amiadapters.configuration.secrets.boto3")
    def test_create_aws_secrets_manager_client_with_profile(
        self, mock_boto3, mock_region, mock_profile
    ):
        mock_profile.return_value = "test-profile"
        mock_region.return_value = "us-west-2"
        mock_session = MagicMock()
        mock_boto3.Session.return_value = mock_session
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        client = secrets._create_aws_secrets_manager_client()
        mock_boto3.Session.assert_called_once_with(profile_name="test-profile")
        mock_session.client.assert_called_once_with(
            "secretsmanager", region_name="us-west-2"
        )
        self.assertEqual(client, mock_client)

    @patch("amiadapters.configuration.secrets.get_global_aws_profile")
    @patch("amiadapters.configuration.secrets.get_global_aws_region")
    @patch("amiadapters.configuration.secrets.boto3")
    def test_create_aws_secrets_manager_client_without_profile(
        self, mock_boto3, mock_region, mock_profile
    ):
        mock_profile.return_value = None
        mock_region.return_value = "us-west-2"
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        client = secrets._create_aws_secrets_manager_client()
        mock_boto3.client.assert_called_once_with(
            "secretsmanager", region_name="us-west-2"
        )
        self.assertEqual(client, mock_client)
