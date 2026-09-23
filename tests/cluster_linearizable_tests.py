"""Helpers for linearizable-read isolation. Does not start a server."""
import unittest

from cluster_linearizable import isolated_get_ok
from cluster_smoke import RespError


class IsolatedGetOracleTests(unittest.TestCase):
    def test_errors_are_safe(self):
        self.assertTrue(isolated_get_ok(RespError('ERR read index timeout')))
        self.assertTrue(isolated_get_ok(RespError('ERR MOVED 1')))
        self.assertTrue(isolated_get_ok(RespError('timeout-or-disconnect: timed out')))

    def test_successful_values_are_unsafe(self):
        self.assertFalse(isolated_get_ok(b'old'))
        self.assertFalse(isolated_get_ok(b'new'))
        self.assertFalse(isolated_get_ok('old'))
        self.assertFalse(isolated_get_ok(None))


if __name__ == '__main__':
    unittest.main(verbosity=2)
