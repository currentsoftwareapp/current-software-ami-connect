from amiadapters.configuration.models import (
    PipelineSecretsBase,
    UtilityBillingSecrets,
)
from test.base_test_case import BaseTestCase


class TestPipelineSecretsBase(BaseTestCase):

    def test_from_dict_builds_utility_billing_secrets(self):
        secret = PipelineSecretsBase.from_dict(
            "utility_billing",
            {"connection_url": "postgresql://user:pass@host:5432/db"},
        )
        self.assertIsInstance(secret, UtilityBillingSecrets)
        self.assertEqual("postgresql://user:pass@host:5432/db", secret.connection_url)

    def test_from_dict_raises_on_unknown_name(self):
        with self.assertRaises(ValueError):
            PipelineSecretsBase.from_dict("nope", {"connection_url": "x"})

    def test_from_dict_raises_on_empty_config(self):
        with self.assertRaises(ValueError):
            PipelineSecretsBase.from_dict("utility_billing", {})

    def test_validate_requires_connection_url(self):
        with self.assertRaises(ValueError):
            UtilityBillingSecrets(connection_url=None).validate()
