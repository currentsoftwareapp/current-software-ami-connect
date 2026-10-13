from amiadapters.configuration.models import (
    PipelineSecretsBase,
    UtilityBillingSecrets,
    base_org_id,
    is_new_style_subeca_org_id,
)
from test.base_test_case import BaseTestCase


class TestNewStyleSubecaOrgId(BaseTestCase):

    def test_is_new_style_subeca_org_id_true_for_suffixed_org(self):
        self.assertTrue(is_new_style_subeca_org_id("my_subeca_utility_2"))

    def test_is_new_style_subeca_org_id_false_for_plain_org(self):
        self.assertFalse(is_new_style_subeca_org_id("my_subeca_utility"))

    def test_base_org_id_strips_suffix(self):
        self.assertEqual("my_subeca_utility", base_org_id("my_subeca_utility_2"))

    def test_base_org_id_returns_unchanged_when_no_suffix(self):
        self.assertEqual("my_subeca_utility", base_org_id("my_subeca_utility"))


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
