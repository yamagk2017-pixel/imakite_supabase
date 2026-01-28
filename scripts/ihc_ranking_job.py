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


def fetch_group_names(supabase: Client, group_ids: list[str]) -> dict:
    if not group_ids:
        return {}
    response = (
        supabase.schema("imd")
        .table("groups")
        .select("id, name_ja")
        .in_("id", group_ids)
        .execute()
    )
    df_names = pd.json_normalize(response.data or [])
    if df_names.empty:
        return {}
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

    snapshot_date = os.getenv("SNAPSHOT_DATE")
    if not snapshot_date:
        snapshot_date = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d")
    prev_date = (datetime.fromisoformat(snapshot_date) - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    print(f"Snapshot date: {snapshot_date} / Prev date: {prev_date}")

    df_now = fetch_snapshot(supabase, snapshot_date)
    if df_now.empty:
        raise ValueError(f"No snapshot data for {snapshot_date}")

    df_prev = fetch_snapshot(supabase, prev_date)

    group_ids = df_now["group_id"].dropna().unique().tolist()
    name_map = fetch_group_names(supabase, group_ids)
    df_now["name"] = df_now["group_id"].map(name_map).fillna(df_now["name"])

    df_now = df_now.set_index("group_id")
    if not df_prev.empty:
        df_prev = df_prev.set_index("group_id")

    df_diff = df_now.copy()
    new_artist_ids = df_now.index.difference(df_prev.index)
    df_diff["name"] = df_now["name"]

    prev_popularity = df_prev.get("artist_popularity", pd.Series(dtype="float64")).reindex(
        df_now.index
    )
    df_diff["popularity_delta"] = df_now["artist_popularity"] - prev_popularity
    df_diff.loc[new_artist_ids, "popularity_delta"] = 0
    df_diff["popularity_delta"] = df_diff["popularity_delta"].fillna(0)

    current_followers = df_now["followers"]
    prev_followers_series = df_prev.get("followers", pd.Series(dtype="float64")).reindex(
        df_now.index
    )
    prev_followers_filled = prev_followers_series.fillna(0)
    denominator_followers = prev_followers_filled.replace(0, 1)
    df_diff["followers_ratio"] = (current_followers - prev_followers_filled) / denominator_followers
    df_diff.loc[new_artist_ids, "followers_ratio"] = 0.0

    current_track_pop_sum = df_now["track_popularity_sum"]
    prev_track_pop_sum_series = df_prev.get(
        "track_popularity_sum", pd.Series(dtype="float64")
    ).reindex(df_now.index)
    prev_track_pop_sum_filled = prev_track_pop_sum_series.fillna(0)
    denominator_track_pop_sum = prev_track_pop_sum_filled.replace(0, 1)
    df_diff["track_popularity_sum_ratio"] = (
        current_track_pop_sum - prev_track_pop_sum_filled
    ) / denominator_track_pop_sum
    df_diff.loc[new_artist_ids, "track_popularity_sum_ratio"] = 0.0

    df_diff["new_release_count"] = df_now["new_release_count"]
    df_diff.loc[new_artist_ids, "new_release_count"] = 0

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

    ranking_raw = df_diff.sort_values(by="score", ascending=False).reset_index()
    ranking_raw.insert(0, "rank", ranking_raw.index + 1)

    ranking_raw = ranking_raw[
        [
            "rank",
            "score",
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

    # daily_rankings はポイント表記 (score * 10) を保存する
    ranking_points = ranking_raw.copy()
    ranking_points["score"] = ranking_points["score"] * 10

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

    score_points = ranking_raw.copy()
    score_points["score_points"] = score_points["score"] * 10

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

    (
        supabase.schema("ihc")
        .table("daily_top20")
        .upsert(top20_records, on_conflict="snapshot_date,group_id")
        .execute()
    )

    print("✅ Ranking, stats, cumulative, and top20 saved.")


if __name__ == "__main__":
    main()
