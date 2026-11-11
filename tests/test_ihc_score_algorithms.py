import unittest
from datetime import date, timedelta

from scripts.ihc_score_algorithms import (
    calculate_conversion_bonus_awarded,
    calculate_conversion_momentum,
    validate_algorithm_version,
)


class ConversionBonusTests(unittest.TestCase):
    def test_first_positive_impulse_earns_no_bonus(self):
        self.assertEqual(calculate_conversion_bonus_awarded(1, 0), 0)

    def test_each_later_positive_impulse_earns_five_points(self):
        self.assertEqual(calculate_conversion_bonus_awarded(1, 1), 5)
        self.assertEqual(calculate_conversion_bonus_awarded(1, 2), 5)

    def test_plus_two_counts_as_two_positive_units(self):
        self.assertEqual(calculate_conversion_bonus_awarded(2, 0), 5)
        self.assertEqual(calculate_conversion_bonus_awarded(2, 1), 10)

    def test_non_positive_delta_earns_no_bonus(self):
        self.assertEqual(calculate_conversion_bonus_awarded(0, 3), 0)
        self.assertEqual(calculate_conversion_bonus_awarded(-1, 3), 0)


class ConversionMomentumTests(unittest.TestCase):
    def test_seven_day_old_and_new_bonus_match_discussed_example(self):
        today = date(2026, 7, 19)
        events = [(today - timedelta(days=7), 5), (today, 5)]
        self.assertAlmostEqual(
            calculate_conversion_momentum(events, today), 7.3914845
        )

    def test_each_event_decays_from_its_own_award_date(self):
        today = date(2026, 7, 19)
        events = [(today - timedelta(days=10), 5), (today - timedelta(days=3), 5)]
        self.assertAlmostEqual(
            calculate_conversion_momentum(events, today),
            5 * (0.9**10) + 5 * (0.9**3),
        )

    def test_event_expires_at_age_28(self):
        today = date(2026, 7, 29)
        events = [
            (today - timedelta(days=27), 5),
            (today - timedelta(days=28), 5),
        ]
        self.assertAlmostEqual(
            calculate_conversion_momentum(events, today), 5 * (0.9**27)
        )


class AlgorithmVersionTests(unittest.TestCase):
    def test_defaults_to_v1_and_accepts_v2(self):
        self.assertEqual(validate_algorithm_version(None), "v1")
        self.assertEqual(validate_algorithm_version("V2"), "v2")

    def test_rejects_unknown_version(self):
        with self.assertRaises(ValueError):
            validate_algorithm_version("v3")


if __name__ == "__main__":
    unittest.main()
