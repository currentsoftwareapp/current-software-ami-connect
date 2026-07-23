from unittest.mock import patch

from amiadapters.configuration import base
from test.base_test_case import BaseTestCase


class TestCreateUtilityBillingSettingsConnectionFromSecrets(BaseTestCase):

    @patch("amiadapters.configuration.base.create_utility_billing_settings_connection")
    def test_returns_connection_when_url_present(self, mock_connect):
        secrets = {
            "pipeline": {
                "utility_billing": {
                    "connection_url": "postgresql://user:pass@host:5432/db"
                }
            }
        }

        result = base.create_utility_billing_settings_connection_from_secrets(secrets)

        mock_connect.assert_called_once_with(
            connection_url="postgresql://user:pass@host:5432/db"
        )
        self.assertEqual(mock_connect.return_value, result)

    @patch("amiadapters.configuration.base.create_utility_billing_settings_connection")
    def test_returns_none_when_secret_absent(self, mock_connect):
        for secrets in ({}, {"pipeline": {}}, {"pipeline": {"utility_billing": {}}}):
            with self.subTest(secrets=secrets):
                result = base.create_utility_billing_settings_connection_from_secrets(
                    secrets
                )
                self.assertIsNone(result)
        mock_connect.assert_not_called()
