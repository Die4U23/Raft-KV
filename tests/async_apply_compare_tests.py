"""Ordering and INFO-summary checks. These do not measure the server."""
import unittest

from async_apply_compare import cell_sequence, longest_mount, summarize_info_samples


class PlanTests(unittest.TestCase):
    def test_pipeline_blocks_alternate_which_mode_starts(self):
        cells = cell_sequence(3)
        self.assertEqual([cell['pipeline'] for cell in cells], [1] * 6 + [16] * 6)
        self.assertEqual([cell['async_apply'] for cell in cells if cell['pipeline'] == 1],
                         [False, True, True, False, False, True])
        self.assertEqual([cell['async_apply'] for cell in cells if cell['pipeline'] == 16],
                         [False, True, True, False, False, True])
        for mode in (False, True):
            self.assertEqual(sum(cell['async_apply'] is mode and cell['pipeline'] == 1 for cell in cells), 3)

    def test_rejects_zero_repeats(self):
        with self.assertRaises(ValueError):
            cell_sequence(0)


class InfoSummaryTests(unittest.TestCase):
    def test_term_increase_and_lag_ignore_disagreement(self):
        def sample(term, lag, duration, agree=True):
            leader = {'state': 'leader', 'leader_id': 1, 'term': term, 'apply_lag': lag}
            follower = {'state': 'follower', 'leader_id': 1 if agree else 0, 'term': term, 'apply_lag': 0}
            return {'duration_s': duration, 'nodes': {'1': leader, '0': follower}}

        summary = summarize_info_samples([
            sample(1, 2, 0.01),
            sample(1, 4, 0.02),
            sample(1, 0, 0.01, agree=False),
            sample(2, 1, 0.03),
        ])
        self.assertEqual(summary['term_increases'], 1)
        self.assertEqual(summary['terms_seen'], [1, 2])
        self.assertEqual(summary['max_apply_lag'], 4)
        self.assertEqual(summary['leadership_disagreements'], 1)
        self.assertAlmostEqual(summary['sampling_overhead_seconds'], 0.07)
        self.assertEqual(summarize_info_samples([])['max_apply_lag'], None)


class MountTests(unittest.TestCase):
    def test_longest_matching_mount(self):
        lines = ['/dev/sda1 / ext4 rw 0 0', '/dev/sda2 /tmp ext4 rw 0 0',
                 '/dev/sdb1 /var ext4 rw 0 0']
        chosen = longest_mount('/tmp/raft-kv-apply-cmp', lines)
        self.assertEqual(chosen['mount'], '/tmp')
        self.assertEqual(chosen['fstype'], 'ext4')
