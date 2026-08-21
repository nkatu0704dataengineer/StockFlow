"""
Unit tests for ProviderAdapter.
"""

import unittest
from unittest.mock import MagicMock

from include.api.adapter import ProviderAdapter
from include.api.providers.base import BaseProvider
from include.core.exceptions import ProviderAdapterException


class TestProviderAdapter(unittest.TestCase):
    """Regression tests for the single-provider adapter contract."""

    def test_init_requires_provider(self):
        """The adapter must reject a missing provider instance."""
        with self.assertRaises(ProviderAdapterException):
            ProviderAdapter(provider=None)

    def test_fetch_retries_and_returns_success(self):
        """Fetch must retry the injected provider and return successful raw JSON."""
        provider = MagicMock(spec=BaseProvider)
        provider.provider_name = "Primary"
        provider.fetch.side_effect = [Exception("temporary failure"), {"ok": True}]

        adapter = ProviderAdapter(provider=provider, max_retry=2)
        result = adapter.fetch("NVDA")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(provider.fetch.call_count, 2)

    def test_fetch_raises_after_retry_budget_is_exhausted(self):
        """Fetch must raise a ProviderAdapterException after exhausting retries."""
        provider = MagicMock(spec=BaseProvider)
        provider.provider_name = "Primary"
        provider.fetch.side_effect = Exception("all attempts failed")

        adapter = ProviderAdapter(provider=provider, max_retry=3)

        with self.assertRaises(ProviderAdapterException) as context:
            adapter.fetch("AAPL")

        self.assertIn("Primary failed after 3 attempts", str(context.exception))

    def test_health_check_delegates_to_single_provider(self):
        """Health check must delegate to the injected provider and return a bool."""
        provider = MagicMock(spec=BaseProvider)
        provider.provider_name = "Primary"
        provider.health_check.return_value = True

        adapter = ProviderAdapter(provider=provider)
        result = adapter.health_check()

        self.assertIsInstance(result, bool)
        self.assertTrue(result)
        provider.health_check.assert_called_once()


if __name__ == "__main__":
    unittest.main()
