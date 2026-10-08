import pandas as pd


def find_baseline_reset_group_ids(
    current_snapshots: pd.DataFrame, previous_snapshots: pd.DataFrame
) -> pd.Index:
    """Return groups whose day-over-day metrics must start from a new baseline.

    A group needs a reset when it is newly observed or when its Spotify artist ID
    differs from the preceding snapshot. In both cases, comparing absolute Spotify
    metrics would measure an identity change rather than organic growth.
    """

    new_group_ids = current_snapshots.index.difference(previous_snapshots.index)
    if previous_snapshots.empty:
        return new_group_ids

    if "spotify_id" not in current_snapshots or "spotify_id" not in previous_snapshots:
        return new_group_ids

    existing_group_ids = current_snapshots.index.intersection(previous_snapshots.index)
    current_spotify_ids = (
        current_snapshots.loc[existing_group_ids, "spotify_id"]
        .astype("string")
        .fillna("")
    )
    previous_spotify_ids = (
        previous_snapshots.loc[existing_group_ids, "spotify_id"]
        .astype("string")
        .fillna("")
    )
    changed_group_ids = existing_group_ids[
        current_spotify_ids.ne(previous_spotify_ids).to_numpy()
    ]

    return new_group_ids.union(changed_group_ids, sort=False)
