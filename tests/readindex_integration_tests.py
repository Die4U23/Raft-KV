"""Portable checks for the integration test's verdicts, not Raft correctness."""
import contextlib
import io
import unittest
from unittest import SkipTest
from unittest.mock import MagicMock, patch

import test_readindex_integration as integration
from cluster_smoke import RespError


class IntegrationVerdictTests(unittest.TestCase):
    def test_runner_propagates_failures_and_incomplete_runs(self):
        def passed(_):
            return

        def failed(_):
            raise AssertionError('injected incorrect result')

        def skipped(_):
            raise SkipTest('injected missing prerequisite')

        for cases, status, summary in (
            ([passed], 0, '1 passed, 0 failed, 0 skipped'),
            ([failed], 1, '0 passed, 1 failed, 0 skipped'),
            ([passed, skipped], 2, '1 passed, 0 failed, 1 skipped'),
            ([failed, skipped], 1, '0 passed, 1 failed, 1 skipped'),
            ([], 2, '0 passed, 0 failed, 0 skipped'),
        ):
            with self.subTest(status=status, summary=summary):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(integration.run_tests(cases), status)
                self.assertIn(summary, output.getvalue())
                if status:
                    self.assertNotIn('All integration tests passed', output.getvalue())

    def test_missing_binary_is_not_a_successful_run(self):
        output = io.StringIO()
        with patch.object(integration.Path, 'exists', return_value=False), \
             patch.object(integration.Path, 'is_file', return_value=False), \
             contextlib.redirect_stdout(output):
            result = integration.main([])
        self.assertEqual(result, 2)
        self.assertIn('0 passed, 0 failed, 4 skipped', output.getvalue())
        self.assertNotIn('All integration tests passed', output.getvalue())

    def run_restarted_read(self, reply=None, error=None):
        cluster = MagicMock()
        cluster.nodes = [MagicMock() for _ in range(3)]
        cluster.reservations = [MagicMock() for _ in range(6)]
        cluster.wait_for.side_effect = [0, 1, 1]
        cluster.leader.return_value = 1
        client = MagicMock()
        client.command.side_effect = ['OK', 'OK', 'OK', error or reply]
        connection = MagicMock()
        connection.__enter__.return_value = client
        with patch.object(integration.Path, 'exists', return_value=True), \
             patch.object(integration.Path, 'is_file', return_value=True), \
             patch.object(integration, 'Cluster', return_value=cluster), \
             patch.object(integration.RespClient, 'connect', return_value=connection), \
             contextlib.redirect_stdout(io.StringIO()):
            integration.test_restarted_follower_rejects_reads()
        cluster.close.assert_called_once()

    def test_successful_get_cannot_pass_as_rejection(self):
        for reply in (b'value1', b'other-value', None, 'OK'):
            with self.subTest(reply=reply):
                with self.assertRaises(AssertionError):
                    self.run_restarted_read(reply)

    def test_only_expected_protocol_rejection_passes(self):
        self.run_restarted_read(RespError('ERR MOVED 1'))
        with self.assertRaises(AssertionError):
            self.run_restarted_read(RespError('ERR storage failure'))

    def test_transport_and_assertion_errors_are_not_swallowed(self):
        for error in (TimeoutError('timeout'), AssertionError('broken invariant')):
            with self.subTest(error=type(error).__name__):
                with self.assertRaises(type(error)):
                    self.run_restarted_read(error=error)


if __name__ == '__main__':
    unittest.main()
