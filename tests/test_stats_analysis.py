"""Tests for ``src/stats_analysis.py``.

Run with ``python -m unittest discover -s tests`` (pytest also works).
The data tests read ``results/raw/experiment_results.csv`` and the processed tables,
so they check that the Phase 10 statistics reproduce the Phase 8 and Phase 9 numbers.
"""

import math
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import stats_analysis as sa  # noqa: E402
from src.cost_model import BREAKEVEN_OUTPUT, break_even_runs  # noqa: E402


class HelperTests(unittest.TestCase):
    def test_cliffs_delta_extremes(self):
        self.assertEqual(sa.cliffs_delta([5, 6, 7], [1, 2, 3]), 1.0)
        self.assertEqual(sa.cliffs_delta([1, 2, 3], [5, 6, 7]), -1.0)
        self.assertEqual(sa.cliffs_delta([1, 2], [1, 2]), 0.0)

    def test_cliffs_labels(self):
        self.assertEqual(sa.cliffs_label(0.1), "negligible")
        self.assertEqual(sa.cliffs_label(-0.2), "small")
        self.assertEqual(sa.cliffs_label(0.4), "medium")
        self.assertEqual(sa.cliffs_label(1.0), "large")

    def test_wilcoxon_floor_at_five_pairs(self):
        # With five pairs the smallest possible two-sided exact p-value is 2/32.
        p = sa.wilcoxon_exact_p([10, 11, 12, 13, 14], [1, 2, 3, 4, 5])
        self.assertAlmostEqual(p, 0.0625)

    def test_wilcoxon_all_ties(self):
        self.assertEqual(sa.wilcoxon_exact_p([1, 2], [1, 2]), 1.0)

    def test_vector_break_even_matches_cost_model(self):
        cases = [(3.0, 2.0, 1.0), (2.0, 2.0, 1.0), (0.1, 5.0, 1.0), (1.0, 1.0, 1.0),
                 (1.0, 1.0, 2.0), (2.4399, 1.0, 0.1258), (0.3, 0.1, 0.0)]
        copt, static, action = (np.array(c) for c in zip(*cases))
        got = sa._vector_break_even(copt, static, action)
        for value, case in zip(got, cases):
            self.assertEqual(value, break_even_runs(*case))

    def test_block_resamples_shape_and_seed(self):
        a = sa.block_resamples(5, resamples=100)
        self.assertEqual(a.shape, (100, 5))
        self.assertTrue(((a >= 0) & (a < 5)).all())
        np.testing.assert_array_equal(a, sa.block_resamples(5, resamples=100))

    def test_percentile_ci_order(self):
        low, high = sa.percentile_ci(np.arange(1001))
        self.assertAlmostEqual(low, 25.0)
        self.assertAlmostEqual(high, 975.0)


class DataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runs = sa.load_runs()
        cls.idx = sa.block_resamples(5, resamples=500)

    def test_all_data_checks_pass(self):
        checks = sa.data_checks()
        self.assertTrue(checks["passed"].all(), checks[~checks["passed"]])

    def test_a1_point_speedups(self):
        effects = sa.a1_effects(self.runs, self.idx)
        wall = effects[effects["metric"] == "wall"].set_index("workload_id")["speedup"]
        expected = {"A1_large_high": 7.95, "A1_large_low": 8.47, "A1_small_high": 11.31, "A1_small_low": 10.66}
        for workload, value in expected.items():
            self.assertAlmostEqual(wall[workload], value, places=2)

    def test_a1_plateaus(self):
        effects = sa.a1_effects(self.runs, self.idx)
        wall = effects[effects["metric"] == "wall"].set_index("workload_id")["plateau_partitions"]
        self.assertEqual(wall["A1_small_high"], "1")
        self.assertEqual(wall["A1_large_high"], "4")
        self.assertEqual(wall["A1_large_low"], "1 2 4")
        self.assertEqual(wall["A1_small_low"], "1 2")

    def test_a2_benefit_matches_phase7(self):
        summary = pd.read_csv(sa.RESULTS_PROCESSED_DIR / "a2_compaction_summary.csv").set_index("workload_id")
        effects = sa.a2_effects(self.runs, self.idx)
        wall = effects[effects["metric"] == "wall"].set_index("workload_id")["benefit_pct"]
        for workload, value in wall.items():
            self.assertAlmostEqual(value, summary.loc[workload, "median_benefit_pct"], places=4)

    def test_break_even_points_match_phase8(self):
        table = pd.read_csv(BREAKEVEN_OUTPUT)
        table = table[table["action"].isin(["A1_search", "A2"])].set_index(["workload_id", "action", "proxy"])
        ours = sa.breakeven_uncertainty(self.runs, self.idx).set_index(["workload_id", "action", "proxy"])
        for key, row in ours.iterrows():
            expected = float(table.loc[key, "n_star"])
            self.assertTrue(row["n_star"] == expected or (math.isinf(expected) and math.isinf(row["n_star"])), key)

    def test_h2_matches_phase9(self):
        tests = sa.h2_tests(sa.load_decisions()).set_index("proxy")
        self.assertEqual((tests.loc["P1", "wins"], tests.loc["P1", "ties"], tests.loc["P1", "losses"]), (13, 1, 2))
        self.assertEqual((tests.loc["P2", "wins"], tests.loc["P2", "ties"], tests.loc["P2", "losses"]), (15, 1, 0))
        self.assertAlmostEqual(tests.loc["P1", "mean_reduction"], 0.473, places=3)
        self.assertAlmostEqual(tests.loc["P1", "mean_ci_case_low"], 0.305, places=3)
        self.assertAlmostEqual(tests.loc["P1", "mean_ci_case_high"], 0.623, places=3)


if __name__ == "__main__":
    unittest.main()
