import unittest

import pandas as pd

from scripts.ihc_identity import find_baseline_reset_group_ids


class BaselineResetTests(unittest.TestCase):
    def test_resets_new_groups_and_changed_spotify_ids(self):
        current = pd.DataFrame(
            {
                "group_id": ["unchanged", "changed", "new"],
                "spotify_id": ["artist-a", "artist-b-new", "artist-c"],
            }
        ).set_index("group_id")
        previous = pd.DataFrame(
            {
                "group_id": ["unchanged", "changed"],
                "spotify_id": ["artist-a", "artist-b-old"],
            }
        ).set_index("group_id")

        result = find_baseline_reset_group_ids(current, previous)

        self.assertEqual(set(result), {"changed", "new"})

    def test_resets_when_only_one_side_has_a_spotify_id(self):
        current = pd.DataFrame(
            {
                "group_id": ["added", "removed", "still-missing"],
                "spotify_id": ["artist-a", None, None],
            }
        ).set_index("group_id")
        previous = pd.DataFrame(
            {
                "group_id": ["added", "removed", "still-missing"],
                "spotify_id": [None, "artist-b", None],
            }
        ).set_index("group_id")

        result = find_baseline_reset_group_ids(current, previous)

        self.assertEqual(set(result), {"added", "removed"})

    def test_all_current_groups_reset_when_previous_snapshot_is_empty(self):
        current = pd.DataFrame(
            {
                "group_id": ["one", "two"],
                "spotify_id": ["artist-a", "artist-b"],
            }
        ).set_index("group_id")
        previous = pd.DataFrame(columns=["spotify_id"])
        previous.index.name = "group_id"

        result = find_baseline_reset_group_ids(current, previous)

        self.assertEqual(set(result), {"one", "two"})


if __name__ == "__main__":
    unittest.main()
