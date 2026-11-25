from amiadapters.configuration.models import (
    NeptuneSourceConfig,
    PipelineSecretsBase,
    SourceConfigBase,
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


class TestNeptuneSourceConfig(BaseTestCase):
    def _raw_config(self, **overrides):
        return {
            "org_id": "my_org",
            "type": "neptune",
            "timezone": "America/Los_Angeles",
            "secrets": None,
            "sinks": [],
            "task_output_controller": None,
            "metrics": None,
            "meter_alerts": None,
            **overrides,
        }

    def test_from_dict_builds_config_without_external_adapter_location(self):
        config = SourceConfigBase.from_dict(self._raw_config())
        self.assertIsInstance(config, NeptuneSourceConfig)

    def test_from_dict_accepts_legacy_external_adapter_location(self):
        config = SourceConfigBase.from_dict(
            self._raw_config(external_adapter_location="./neptune")
        )
        self.assertIsInstance(config, NeptuneSourceConfig)
