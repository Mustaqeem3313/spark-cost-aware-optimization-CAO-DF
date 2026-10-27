"""Tests for the decision rule and the interactive mode of ``src/decision_framework.py``.

Run with ``python -m unittest discover -s tests`` (pytest also works).
"""

import math
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import decision_framework as df  # noqa: E402
from src.cost_model import PRICE_PER_VCPU_HOUR_USD, STUDY_MACHINE_VCPUS  # noqa: E402


def scripted(answers):
    """A fake ``input`` that returns the given answers in order."""
    answers = iter(answers)
    return lambda prompt: next(answers)


def features(orders, files, shuffle):
    return pd.Series({"log_orders": math.log10(orders), "log_files": math.log10(files),
                      "shuffle_fraction": shuffle})


class DecideTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profiles = df.training_profiles(PRICE_PER_VCPU_HOUR_USD, STUDY_MACHINE_VCPUS)

    def test_one_run_prefers_cheap_compaction(self):
        decision = df.decide(self.profiles, features(500_000, 25, 0.5), 1)
        self.assertEqual(decision.choice, "Compact")
        self.assertAlmostEqual(decision.predicted_tc["Nothing"], 1.0)

    def test_ten_runs_prefers_tuning(self):
        decision = df.decide(self.profiles, features(500_000, 25, 0.5), 10)
        self.assertEqual(decision.choice, "Tune")

    def test_single_file_never_compacts(self):
        decision = df.decide(self.profiles, features(500_000, 1, 0.5), 50)
        self.assertNotIn("Compact", decision.predicted_tc)

    def test_predicted_cost_formula(self):
        decision = df.decide(self.profiles, features(500_000, 25, 0.5), 10)
        est = decision.estimates["A1"]
        self.assertAlmostEqual(decision.predicted_tc["Tune"], est.copt_ratio + 10 * (1 - est.saving_pct))


class InteractiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profiles = df.training_profiles(PRICE_PER_VCPU_HOUR_USD, STUDY_MACHINE_VCPUS)

    def test_answers_are_parsed(self):
        out = []
        answers = df.ask_workload(self.profiles, scripted(["500,000", "25", "0.5", "10", ""]), out.append)
        self.assertEqual(answers, {"orders": 500_000, "files": 25, "shuffle": 0.5, "runs": 10, "proxy": "P1"})

    def test_invalid_answers_are_asked_again(self):
        out = []
        answers = df.ask_workload(
            self.profiles, scripted(["abc", "-5", "500000", "0", "25", "1.5", "0.5", "10", "p3", "p2"]), out.append)
        self.assertEqual(answers["orders"], 500_000)
        self.assertEqual(answers["proxy"], "P2")
        self.assertEqual(sum(line.startswith("  enter") for line in out), 5)

    def test_out_of_range_input_is_flagged(self):
        out = []
        df.ask_workload(self.profiles, scripted(["50000000", "25", "0.5", "10", "P1"]), out.append)
        self.assertTrue(any("outside the measured study range" in line for line in out))


if __name__ == "__main__":
    unittest.main()
