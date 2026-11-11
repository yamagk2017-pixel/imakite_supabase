import base64
import math
import os
import random
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from supabase import Client, create_client

from ihc_score_algorithms import (
    calculate_conversion_bonus_awarded,
    calculate_conversion_momentum,
    validate_algorithm_version,
)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def get_supabase_client() -> Client:
    supabase_url = require_env("SUPABASE_URL")
    supabase_key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(supabase_url, supabase_key)


class TokenManager:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expiry_time = datetime.now()

    def _get_new_token(self) -> None:
        auth = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        headers = {"Authorization": f"Basic {auth}"}
        data = {"grant_type": "client_credentials"}
        try:
            response = requests.post(
                "https://accounts.spotify.com/api/token", headers=headers, data=data
            )
            response.raise_for_status()
            token_data = response.json()
            self.token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.token_expiry_time = datetime.now() + timedelta(
                seconds=expires_in - 300
            )
            print("Spotify access token refreshed.")
        except requests.exceptions.RequestException as exc:
            print(f"Failed to get token: {exc}")
            self.token = None

    def get_token(self) -> str | None:
        if self.token is None or datetime.now() >= self.token_expiry_time:
            print("Refreshing Spotify token...")
            self._get_new_token()
        return self.token

    def force_refresh_token(self) -> None:
        print("401 error detected. Forcing token refresh.")
        self._get_new_token()


def make_spotify_request(url: str, token_manager: TokenManager, max_retries: int = 5):
    for attempt in range(max_retries):
        try:
            token = token_manager.get_token()
            if not token:
                raise Exception("Token not available")

            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                return response.json()
            if response.status_code == 401:
                print(f"401 for {url}")
                token_manager.force_refresh_token()
                continue
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 5))
                print(f"Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
            elif response.status_code >= 500:
                wait_time = (2**attempt) + random.uniform(0, 1)
                print(
                    f"Server error {response.status_code}. Retry {attempt + 1} after {wait_time:.2f}s..."
                )
                time.sleep(wait_time)
            else:
                return None
        except requests.exceptions.RequestException as exc:
            print(f"Request exception: {exc} retrying...")
            wait_time = (2**attempt) + random.uniform(0, 1)
            time.sleep(wait_time)
    return None


def get_latest_track_info(artist_id: str, token_manager: TokenManager):
    url = (
        "https://api.spotify.com/v1/artists/"
        f"{artist_id}/albums?include_groups=album,single&market=JP&limit=10"
    )
    data = make_spotify_request(url, token_manager)
    if not data or not data.get("items"):
        return "N/A", "N/A"

    latest_album = sorted(data["items"], key=lambda x: x["release_date"], reverse=True)[
        0
    ]
    album_id = latest_album["id"]

    tracks_url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=1"
    tracks_data = make_spotify_request(tracks_url, token_manager)

    if tracks_data and tracks_data.get("items"):
        track = tracks_data["items"][0]
        track_name = track["name"]
        track_id = track["id"]
        embed_link = f"https://open.spotify.com/embed/track/{track_id}"
        return track_name, embed_link

    return "N/A", "N/A"


def get_fallback_track_info(artist_id: str, token_manager: TokenManager):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=JP"
    data = make_spotify_request(url, token_manager)
    if data and data.get("tracks"):
        track = data["tracks"][0]
        track_name = track["name"]
        track_id = track["id"]
        embed_link = f"https://open.spotify.com/embed/track/{track_id}"
        return track_name, embed_link
    return "N/A", "N/A"


def get_artist_image_url(artist_id: str, token_manager: TokenManager) -> str:
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    data = make_spotify_request(url, token_manager)
    if data and data.get("images"):
        return data["images"][0]["url"]
    return "N/A"


def fetch_snapshot(supabase: Client, date_str: str) -> pd.DataFrame:
    response = (
        supabase.schema("ihc")
        .table("artist_snapshots")
        .select(
            "snapshot_date, group_id, spotify_id, name, artist_popularity, followers, track_popularity_sum, new_release_count"
        )
        .eq("snapshot_date", date_str)
        .execute()
    )
    return pd.json_normalize(response.data or [])


def get_latest_snapshot_date(supabase: Client) -> str:
    response = (
        supabase.schema("ihc")
        .table("artist_snapshots")
        .select("snapshot_date")
        .order("snapshot_date", desc=True)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError("No snapshot data found in ihc.artist_snapshots")
    return rows[0]["snapshot_date"]


def fetch_conversion_history(
    supabase: Client, start_date: str, end_date: str
) -> pd.DataFrame:
    """Fetch prior daily signals and awarded conversion bonus events."""

    page_size = int(os.getenv("PAGE_SIZE", "1000"))
    offset = 0
    rows = []
    while True:
        response = (
            supabase.schema("ihc")
            .table("daily_rankings")
            .select(
                "snapshot_date,group_id,artist_popularity,popularity_delta,"
                "conversion_bonus_awarded"
            )
            .gte("snapshot_date", start_date)
            .lte("snapshot_date", end_date)
            .order("snapshot_date")
            .order("group_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    history = pd.json_normalize(rows)
    if history.empty:
        return pd.DataFrame(
            columns=[
                "snapshot_date",
                "group_id",
                "artist_popularity",
                "popularity_delta",
                "conversion_bonus_awarded",
            ]
        )
    history["snapshot_date"] = pd.to_datetime(history["snapshot_date"]).dt.date
    history["popularity_delta"] = pd.to_numeric(
        history["popularity_delta"], errors="coerce"
    ).fillna(0)
    history["artist_popularity"] = pd.to_numeric(
        history["artist_popularity"], errors="coerce"
    )
    history["conversion_bonus_awarded"] = pd.to_numeric(
        history["conversion_bonus_awarded"], errors="coerce"
    ).fillna(0)
    return history


def apply_score_algorithm(
    ranking_points: pd.DataFrame,
    history: pd.DataFrame,
    snapshot_date: str,
    algorithm_version: str,
    *,
    conversion_window_days: int,
    conversion_decay_rate: float,
    conversion_bonus_per_repeat: float,
) -> pd.DataFrame:
    """Attach v1/v2 score components and return rows ranked by active score."""

    target_date = datetime.fromisoformat(snapshot_date).date()
    history_by_group = {
        group_id: rows for group_id, rows in history.groupby("group_id")
    }

    awarded_values = []
    momentum_values = []
    for _, row in ranking_points.iterrows():
        group_id = row["group_id"]
        group_history = history_by_group.get(group_id)
        if group_history is None:
            prior_positive_impulses = 0.0
            events = []
        else:
            eligible_history = group_history[
                group_history["artist_popularity"].notna()
                & (group_history["artist_popularity"] > 2)
            ]
            prior_positive_impulses = eligible_history[
                "popularity_delta"
            ].clip(lower=0).sum()
            events = [
                (event_row["snapshot_date"], event_row["conversion_bonus_awarded"])
                for _, event_row in group_history.iterrows()
                if event_row["conversion_bonus_awarded"] > 0
            ]

        eligible = bool(row.get("conversion_eligible", True))
        awarded = 0.0
        if algorithm_version == "v2" and eligible:
            awarded = calculate_conversion_bonus_awarded(
                row.get("popularity_delta"),
                prior_positive_impulses,
                bonus_per_repeat=conversion_bonus_per_repeat,
            )
        events.append((target_date, awarded))
        momentum = (
            calculate_conversion_momentum(
                events,
                target_date,
                decay_rate=conversion_decay_rate,
                window_days=conversion_window_days,
            )
            if algorithm_version == "v2" and eligible
            else 0.0
        )
        awarded_values.append(awarded)
        momentum_values.append(momentum)

    ranking_points = ranking_points.copy()
    ranking_points["conversion_bonus_awarded"] = awarded_values
    ranking_points["conversion_momentum"] = momentum_values
    ranking_points["algorithm_version"] = algorithm_version
    if algorithm_version == "v2":
        ranking_points["score"] = (
            ranking_points["base_score"] + ranking_points["conversion_momentum"]
        )
    else:
        ranking_points["score"] = ranking_points["base_score"]

    if algorithm_version == "v1":
        # Preserve the legacy stable ordering for tied scores.
        ranking_points = ranking_points.sort_values(
            by="score", ascending=False
        ).reset_index(drop=True)
    else:
        ranking_points = ranking_points.sort_values(
            by=["score", "base_score", "group_id"], ascending=[False, False, True]
        ).reset_index(drop=True)
    ranking_points.insert(0, "rank", ranking_points.index + 1)
    return ranking_points


def fetch_group_names(supabase: Client, group_ids: list[str]) -> dict:
    if not group_ids:
        return {}

    unique_group_ids = list(dict.fromkeys(group_ids))
    chunk_size_raw = os.getenv("GROUP_NAMES_CHUNK_SIZE", "50")
    try:
        chunk_size = int(chunk_size_raw)
    except ValueError as exc:
        raise ValueError("GROUP_NAMES_CHUNK_SIZE must be an integer.") from exc
    if chunk_size <= 0:
        raise ValueError("GROUP_NAMES_CHUNK_SIZE must be greater than 0.")

    total_chunks = math.ceil(len(unique_group_ids) / chunk_size)
    print(
        f"Resolving group names: ids={len(unique_group_ids)} chunk_size={chunk_size} chunks={total_chunks}"
    )

    all_rows = []
    for index in range(total_chunks):
        start = index * chunk_size
        end = start + chunk_size
        id_chunk = unique_group_ids[start:end]
        response = (
            supabase.schema("imd")
            .table("groups")
            .select("id, name_ja")
            .in_("id", id_chunk)
            .execute()
        )
        batch_rows = response.data or []
        all_rows.extend(batch_rows)
        print(
            f"Fetched group names chunk {index + 1}/{total_chunks}: requested={len(id_chunk)} returned={len(batch_rows)}"
        )

    df_names = pd.json_normalize(all_rows)
    if df_names.empty:
        return {}
    df_names = df_names.drop_duplicates(subset=["id"])
    return dict(zip(df_names["id"], df_names["name_ja"]))


def to_int(value):
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value):
    if value is None or pd.isna(value):
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    return casted if math.isfinite(casted) else None


def parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def calculate_change_stats(today_val, yesterday_val):
    diff, ratio_str = pd.NA, "N/A"
    if pd.notna(today_val) and pd.notna(yesterday_val):
        diff = today_val - yesterday_val
        if yesterday_val != 0:
            ratio_str = f"{(diff / yesterday_val) * 100:.2f}%"
        elif today_val == 0:
            ratio_str = "0.00%"
        else:
            ratio_str = "N/A (prev 0)"
    elif pd.notna(today_val):
        diff = today_val
        ratio_str = "N/A (no prev)"
    elif pd.notna(yesterday_val):
        diff = -yesterday_val
        ratio_str = "N/A (no today)"
    return diff, ratio_str


def main() -> None:
    supabase = get_supabase_client()

    algorithm_version = validate_algorithm_version(
        os.getenv("IHC_RANKING_ALGORITHM_VERSION"), default="v1"
    )
    conversion_window_days = int(os.getenv("CONVERSION_WINDOW_DAYS", "28"))
    conversion_decay_rate = float(os.getenv("CONVERSION_DECAY_RATE", "0.9"))
    conversion_bonus_per_repeat = float(
        os.getenv("CONVERSION_BONUS_PER_REPEAT", "5")
    )
    if conversion_window_days <= 0:
        raise ValueError("CONVERSION_WINDOW_DAYS must be greater than 0.")
    if not 0 < conversion_decay_rate <= 1:
        raise ValueError(
            "CONVERSION_DECAY_RATE must be greater than 0 and at most 1."
        )
    if conversion_bonus_per_repeat < 0:
        raise ValueError("CONVERSION_BONUS_PER_REPEAT must not be negative.")
    print(
        "Ranking algorithm:",
        algorithm_version,
        f"conversion_window_days={conversion_window_days}",
        f"conversion_decay_rate={conversion_decay_rate}",
        f"conversion_bonus_per_repeat={conversion_bonus_per_repeat}",
    )

    snapshot_date = os.getenv("SNAPSHOT_DATE")
    if not snapshot_date:
        # Use the latest available snapshot so workflow timing delay does not
        # cause date mismatches between snapshot and ranking jobs.
        snapshot_date = get_latest_snapshot_date(supabase)
    prev_date = (datetime.fromisoformat(snapshot_date) - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    print(f"Snapshot date: {snapshot_date} / Prev date: {prev_date}")

    df_now = fetch_snapshot(supabase, snapshot_date)
    if df_now.empty:
        raise ValueError(f"No snapshot data for {snapshot_date}")

    df_prev = fetch_snapshot(supabase, prev_date)
    if df_prev.empty and not parse_bool_env("ALLOW_MISSING_PREV_SNAPSHOT"):
        raise ValueError(
            f"No previous snapshot data for {prev_date}. "
            "Refusing to save rankings because every artist would be treated as new."
        )

    group_ids = df_now["group_id"].dropna().unique().tolist()
    name_map = fetch_group_names(supabase, group_ids)
    df_now["name"] = df_now["group_id"].map(name_map).fillna(df_now["name"])

    df_now = df_now.set_index("group_id")
    if not df_prev.empty:
        df_prev = df_prev.set_index("group_id")

    from ihc_identity import find_baseline_reset_group_ids

    df_diff = df_now.copy()
    baseline_reset_group_ids = find_baseline_reset_group_ids(df_now, df_prev)
    if len(baseline_reset_group_ids) > 0:
        print(
            "Resetting day-over-day baselines for new or Spotify-ID-changed groups:",
            len(baseline_reset_group_ids),
        )
    df_diff["name"] = df_now["name"]

    prev_popularity = df_prev.get("artist_popularity", pd.Series(dtype="float64")).reindex(
        df_now.index
    )
    df_diff["popularity_delta"] = df_now["artist_popularity"] - prev_popularity
    df_diff.loc[baseline_reset_group_ids, "popularity_delta"] = 0
    df_diff["popularity_delta"] = df_diff["popularity_delta"].fillna(0)

    current_followers = df_now["followers"]
    prev_followers_series = df_prev.get("followers", pd.Series(dtype="float64")).reindex(
        df_now.index
    )
    prev_followers_filled = prev_followers_series.fillna(0)
    denominator_followers = prev_followers_filled.replace(0, 1)
    df_diff["followers_ratio"] = (current_followers - prev_followers_filled) / denominator_followers
    df_diff.loc[baseline_reset_group_ids, "followers_ratio"] = 0.0

    current_track_pop_sum = df_now["track_popularity_sum"]
    prev_track_pop_sum_series = df_prev.get(
        "track_popularity_sum", pd.Series(dtype="float64")
    ).reindex(df_now.index)
    prev_track_pop_sum_filled = prev_track_pop_sum_series.fillna(0)
    denominator_track_pop_sum = prev_track_pop_sum_filled.replace(0, 1)
    df_diff["track_popularity_sum_ratio"] = (
        current_track_pop_sum - prev_track_pop_sum_filled
    ) / denominator_track_pop_sum
    df_diff.loc[baseline_reset_group_ids, "track_popularity_sum_ratio"] = 0.0

    df_diff["new_release_count"] = df_now["new_release_count"]
    df_diff.loc[baseline_reset_group_ids, "new_release_count"] = 0

    df_diff["score"] = (
        df_diff["popularity_delta"] * 3.0
        + df_diff["followers_ratio"] * 2.0
        + df_diff["track_popularity_sum_ratio"] * 1.0
        + df_diff["new_release_count"] * 0.3
    )

    df_diff["artist_popularity"] = pd.to_numeric(
        df_diff["artist_popularity"], errors="coerce"
    )
    condition_low_popularity = df_diff["artist_popularity"].isin([0, 1, 2]) | df_diff[
        "artist_popularity"
    ].isna()
    df_diff.loc[condition_low_popularity, "score"] = 0
    df_diff["conversion_eligible"] = ~condition_low_popularity

    # Persist all score components in the existing point scale (legacy score x10).
    ranking_points = df_diff.reset_index()
    ranking_points["base_score"] = ranking_points["score"] * 10

    if algorithm_version == "v2":
        history_start = (
            datetime.fromisoformat(snapshot_date)
            - timedelta(days=conversion_window_days)
        ).strftime("%Y-%m-%d")
        conversion_history = fetch_conversion_history(
            supabase, history_start, prev_date
        )
        print(
            "Fetched conversion history:",
            f"rows={len(conversion_history)}",
            f"start={history_start}",
            f"end={prev_date}",
        )
    else:
        conversion_history = pd.DataFrame(
            columns=[
                "snapshot_date",
                "group_id",
                "artist_popularity",
                "popularity_delta",
                "conversion_bonus_awarded",
            ]
        )

    ranking_points = apply_score_algorithm(
        ranking_points,
        conversion_history,
        snapshot_date,
        algorithm_version,
        conversion_window_days=conversion_window_days,
        conversion_decay_rate=conversion_decay_rate,
        conversion_bonus_per_repeat=conversion_bonus_per_repeat,
    )

    ranking_points = ranking_points[
        [
            "rank",
            "score",
            "base_score",
            "conversion_bonus_awarded",
            "conversion_momentum",
            "algorithm_version",
            "name",
            "group_id",
            "spotify_id",
            "artist_popularity",
            "popularity_delta",
            "followers",
            "followers_ratio",
            "new_release_count",
            "track_popularity_sum_ratio",
            "track_popularity_sum",
        ]
    ]

    response = (
        supabase.schema("ihc")
        .table("daily_rankings")
        .select("group_id, rank, score, artist_popularity, track_popularity_sum_ratio")
        .eq("snapshot_date", prev_date)
        .execute()
    )
    df_prev_rankings = pd.json_normalize(response.data or [])
    if df_prev_rankings.empty:
        df_prev_score = pd.DataFrame(columns=["prev_rank", "score_prev"])
    else:
        df_prev_score = df_prev_rankings.set_index("group_id")[["rank", "score"]].rename(
            columns={"rank": "prev_rank", "score": "score_prev"}
        )

    ranking_points = ranking_points.merge(
        df_prev_score, how="left", left_on="group_id", right_index=True
    )
    ranking_points["score_delta"] = ranking_points["score"] - ranking_points["score_prev"]
    threshold = 50.0
    ranking_points["rising_flag"] = ranking_points["score_delta"] > threshold
    ranking_points["rising_icon"] = ranking_points["rising_flag"]

    ranking_points = ranking_points[
        [
            "rank",
            "prev_rank",
            "score",
            "base_score",
            "conversion_bonus_awarded",
            "conversion_momentum",
            "algorithm_version",
            "score_prev",
            "score_delta",
            "rising_icon",
            "name",
            "group_id",
            "spotify_id",
            "artist_popularity",
            "popularity_delta",
            "followers",
            "followers_ratio",
            "new_release_count",
            "track_popularity_sum_ratio",
            "track_popularity_sum",
        ]
    ]

    records = []
    for _, row in ranking_points.iterrows():
        records.append(
            {
                "snapshot_date": snapshot_date,
                "group_id": row["group_id"],
                "rank": to_int(row["rank"]),
                "prev_rank": to_int(row.get("prev_rank")),
                "score": to_float(row.get("score")),
                "base_score": to_float(row.get("base_score")),
                "conversion_bonus_awarded": to_float(
                    row.get("conversion_bonus_awarded")
                ),
                "conversion_momentum": to_float(row.get("conversion_momentum")),
                "algorithm_version": row.get("algorithm_version"),
                "score_prev": to_float(row.get("score_prev")),
                "score_delta": to_float(row.get("score_delta")),
                "rising_icon": bool(row.get("rising_icon")),
                "artist_name": row.get("name"),
                "artist_popularity": to_int(row.get("artist_popularity")),
                "popularity_delta": to_int(row.get("popularity_delta")),
                "followers": to_int(row.get("followers")),
                "followers_ratio": to_float(row.get("followers_ratio")),
                "new_release_count": to_int(row.get("new_release_count")),
                "track_popularity_sum_ratio": to_float(
                    row.get("track_popularity_sum_ratio")
                ),
                "track_popularity_sum": to_int(row.get("track_popularity_sum")),
            }
        )

    batch_size = int(os.getenv("BATCH_SIZE", "500"))
    for start in range(0, len(records), batch_size):
        batch = records[start : start + batch_size]
        (
            supabase.schema("ihc")
            .table("daily_rankings")
            .upsert(batch, on_conflict="snapshot_date,group_id")
            .execute()
        )
        print(f"Upserted {start + len(batch)} / {len(records)}")

    df_today_stats_source = ranking_points.copy()
    df_today_stats_source["artist_popularity"] = pd.to_numeric(
        df_today_stats_source["artist_popularity"], errors="coerce"
    )
    df_today_stats_source["track_popularity_sum_ratio"] = pd.to_numeric(
        df_today_stats_source["track_popularity_sum_ratio"], errors="coerce"
    )
    df_today_stats_source["score"] = pd.to_numeric(
        df_today_stats_source["score"], errors="coerce"
    )

    if df_prev_rankings.empty:
        df_yesterday_stats_source = pd.DataFrame()
    else:
        df_yesterday_stats_source = df_prev_rankings.copy()
        df_yesterday_stats_source["artist_popularity"] = pd.to_numeric(
            df_yesterday_stats_source["artist_popularity"], errors="coerce"
        )
        df_yesterday_stats_source["track_popularity_sum_ratio"] = pd.to_numeric(
            df_yesterday_stats_source["track_popularity_sum_ratio"], errors="coerce"
        )
        df_yesterday_stats_source["score"] = pd.to_numeric(
            df_yesterday_stats_source["score"], errors="coerce"
        )

    count_pop_zero_today = (
        df_today_stats_source[df_today_stats_source["artist_popularity"] == 0].shape[0]
        if not df_today_stats_source.empty
        else 0
    )
    count_tpsr_zero_today = (
        df_today_stats_source[df_today_stats_source["track_popularity_sum_ratio"] == 0].shape[0]
        if not df_today_stats_source.empty
        else 0
    )
    count_both_zero_today = (
        df_today_stats_source[
            (df_today_stats_source["artist_popularity"] == 0)
            & (df_today_stats_source["track_popularity_sum_ratio"] == 0)
        ].shape[0]
        if not df_today_stats_source.empty
        else 0
    )
    avg_score_today = (
        df_today_stats_source["score"].mean()
        if not df_today_stats_source.empty
        and not df_today_stats_source["score"].dropna().empty
        else pd.NA
    )

    count_pop_zero_yesterday = pd.NA
    count_tpsr_zero_yesterday = pd.NA
    count_both_zero_yesterday = pd.NA
    avg_score_yesterday = pd.NA
    if not df_yesterday_stats_source.empty:
        count_pop_zero_yesterday = df_yesterday_stats_source[
            df_yesterday_stats_source["artist_popularity"] == 0
        ].shape[0]
        count_tpsr_zero_yesterday = df_yesterday_stats_source[
            df_yesterday_stats_source["track_popularity_sum_ratio"] == 0
        ].shape[0]
        count_both_zero_yesterday = df_yesterday_stats_source[
            (df_yesterday_stats_source["artist_popularity"] == 0)
            & (df_yesterday_stats_source["track_popularity_sum_ratio"] == 0)
        ].shape[0]
        if not df_yesterday_stats_source["score"].dropna().empty:
            avg_score_yesterday = df_yesterday_stats_source["score"].mean()

    pop_zero_diff, pop_zero_ratio_str = calculate_change_stats(
        count_pop_zero_today, count_pop_zero_yesterday
    )
    tpsr_zero_diff, tpsr_zero_ratio_str = calculate_change_stats(
        count_tpsr_zero_today, count_tpsr_zero_yesterday
    )
    both_zero_diff, both_zero_ratio_str = calculate_change_stats(
        count_both_zero_today, count_both_zero_yesterday
    )
    avg_score_diff, avg_score_ratio_str = calculate_change_stats(
        avg_score_today, avg_score_yesterday
    )

    stats_record = {
        "snapshot_date": snapshot_date,
        "avg_score": to_float(avg_score_today),
        "count_pop_zero": to_int(count_pop_zero_today),
        "count_tpsr_zero": to_int(count_tpsr_zero_today),
        "count_both_zero": to_int(count_both_zero_today),
        "avg_score_prev": to_float(avg_score_yesterday),
        "count_pop_zero_prev": to_int(count_pop_zero_yesterday),
        "count_tpsr_zero_prev": to_int(count_tpsr_zero_yesterday),
        "count_both_zero_prev": to_int(count_both_zero_yesterday),
        "avg_score_diff": to_float(avg_score_diff),
        "count_pop_zero_diff": to_int(pop_zero_diff),
        "count_tpsr_zero_diff": to_int(tpsr_zero_diff),
        "count_both_zero_diff": to_int(both_zero_diff),
        "avg_score_ratio": avg_score_ratio_str,
        "count_pop_zero_ratio": pop_zero_ratio_str,
        "count_tpsr_zero_ratio": tpsr_zero_ratio_str,
        "count_both_zero_ratio": both_zero_ratio_str,
    }

    (
        supabase.schema("ihc")
        .table("daily_stats")
        .upsert(stats_record, on_conflict="snapshot_date")
        .execute()
    )

    prev_cumulative = (
        supabase.schema("ihc")
        .table("cumulative_rankings")
        .select("group_id, cumulative_score")
        .eq("snapshot_date", prev_date)
        .execute()
    )
    df_prev_cumulative = pd.json_normalize(prev_cumulative.data or [])
    if df_prev_cumulative.empty:
        df_prev_cumulative = pd.DataFrame(columns=["group_id", "cumulative_score"])

    # Preserve the legacy cumulative-ranking behavior; v2 only changes daily rank.
    score_points = ranking_points.copy()
    score_points["score_points"] = score_points["base_score"]

    merged = score_points.merge(df_prev_cumulative, on="group_id", how="left")
    merged["cumulative_score"] = merged["cumulative_score"].fillna(0) + merged[
        "score_points"
    ].fillna(0)

    merged = merged.sort_values(by="cumulative_score", ascending=False).reset_index(
        drop=True
    )
    if "rank" in merged.columns:
        merged = merged.drop(columns=["rank"])
    merged.insert(0, "rank", merged.index + 1)

    cumulative_records = []
    for _, row in merged.iterrows():
        cumulative_records.append(
            {
                "snapshot_date": snapshot_date,
                "group_id": row["group_id"],
                "rank": int(row["rank"]),
                "artist_name": row["name"],
                "cumulative_score": float(row["cumulative_score"]),
                "score": float(row["score_points"]),
                "artist_popularity": to_int(row.get("artist_popularity")),
            }
        )

    for start in range(0, len(cumulative_records), batch_size):
        batch = cumulative_records[start : start + batch_size]
        (
            supabase.schema("ihc")
            .table("cumulative_rankings")
            .upsert(batch, on_conflict="snapshot_date,group_id")
            .execute()
        )
        print(f"Cumulative upserted {start + len(batch)} / {len(cumulative_records)}")

    token_manager = TokenManager(
        require_env("SPOTIFY_CLIENT_ID"), require_env("SPOTIFY_CLIENT_SECRET")
    )
    top20_df = ranking_points.head(20).copy()

    extra_info = []
    for artist_id in top20_df["spotify_id"]:
        print(f"Fetching Spotify info for {artist_id}...")
        track_name, embed_link = get_latest_track_info(artist_id, token_manager)
        if track_name == "N/A" or embed_link == "N/A":
            track_name, embed_link = get_fallback_track_info(artist_id, token_manager)
        image_url = get_artist_image_url(artist_id, token_manager)
        extra_info.append(
            {
                "spotify_id": artist_id,
                "latest_track_name": track_name,
                "latest_track_embed_link": embed_link,
                "artist_image_url": image_url,
            }
        )
        time.sleep(0.5)

    df_extra_info = pd.DataFrame(extra_info)
    top20_df = pd.merge(top20_df, df_extra_info, on="spotify_id")

    top20_records = []
    for _, row in top20_df.iterrows():
        top20_records.append(
            {
                "snapshot_date": snapshot_date,
                "group_id": row["group_id"],
                "rank": int(row["rank"]),
                "artist_name": row["name"],
                "score": float(row["score"]),
                "latest_track_name": row.get("latest_track_name"),
                "latest_track_embed_link": row.get("latest_track_embed_link"),
                "artist_image_url": row.get("artist_image_url"),
            }
        )

    if parse_bool_env("RESET_DAILY_TOP20"):
        (
            supabase.schema("ihc")
            .table("daily_top20")
            .delete()
            .eq("snapshot_date", snapshot_date)
            .execute()
        )
        print(f"Reset daily_top20 rows for snapshot_date={snapshot_date}")

    (
        supabase.schema("ihc")
        .table("daily_top20")
        .upsert(top20_records, on_conflict="snapshot_date,group_id")
        .execute()
    )

    print("✅ Ranking, stats, cumulative, and top20 saved.")


if __name__ == "__main__":
    main()
