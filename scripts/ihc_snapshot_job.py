import base64
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


def fetch_spotify_ids(supabase: Client) -> pd.DataFrame:
    print("Loading Spotify IDs from Supabase (imd.external_ids)...")
    response = (
        supabase.schema("imd")
        .table("external_ids")
        .select("external_id, group_id")
        .eq("service", "spotify")
        .execute()
    )

    if not response.data:
        raise Exception("No data found in imd.external_ids for service=spotify.")

    df_ids = pd.json_normalize(response.data)
    df_ids = df_ids.rename(columns={"external_id": "spotify_id"})
    df_ids = df_ids[["spotify_id", "group_id"]]

    duplicate_ids = df_ids[df_ids.duplicated(subset=["spotify_id"], keep=False)]
    if not duplicate_ids.empty:
        print(
            f"Warning: {len(duplicate_ids['spotify_id'].unique())} duplicated spotify_id found."
        )
        df_ids = df_ids.drop_duplicates(subset="spotify_id")

    print(df_ids.head())
    return df_ids


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
                print(f"401 for {url}, retrying...")
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


def get_artist_info(artist_id: str, token_manager: TokenManager):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    return make_spotify_request(url, token_manager)


def get_top_track_popularities(artist_id: str, token_manager: TokenManager, top_n: int = 5):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=JP"
    data = make_spotify_request(url, token_manager)
    if data and "tracks" in data:
        tracks = data["tracks"][:top_n]
        return tracks, [track["popularity"] for track in tracks]
    return [], []


def get_artist_image_url_from_info(info: dict | None) -> str | None:
    if not info:
        return None
    images = info.get("images", [])
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            url = first.get("url")
            if isinstance(url, str) and url:
                return url
    return None


def count_recent_releases(tracks, days: int = 7) -> int:
    count = 0
    now = datetime.now().date()
    for track in tracks:
        try:
            release_date_str = track["album"]["release_date"]
            if len(release_date_str) == 10:
                release_date = datetime.strptime(release_date_str, "%Y-%m-%d").date()
            elif len(release_date_str) == 7:
                release_date = datetime.strptime(release_date_str, "%Y-%m").date()
            elif len(release_date_str) == 4:
                release_date = datetime.strptime(release_date_str, "%Y").date()
            else:
                continue
            if now - release_date <= timedelta(days=days):
                count += 1
        except (ValueError, KeyError):
            continue
    return count


def fetch_snapshot(df_ids: pd.DataFrame, token_manager: TokenManager) -> pd.DataFrame:
    snapshot = []
    total = len(df_ids)
    for index, row in df_ids.iterrows():
        spotify_id = row["spotify_id"]
        print(f"[{index + 1}/{total}] Fetching {spotify_id}...")
        info = get_artist_info(spotify_id, token_manager)
        tracks, pops = get_top_track_popularities(spotify_id, token_manager)
        new_count = count_recent_releases(tracks, days=7)

        snapshot.append(
            {
                "spotify_id": spotify_id,
                "name": info.get("name", "N/A") if info else "N/A",
                "artist_image_url": get_artist_image_url_from_info(info),
                "artist_popularity": info.get("popularity", 0) if info else 0,
                "followers": info.get("followers", {}).get("total", 0) if info else 0,
                "track_popularity_sum": sum(pops) if pops else 0,
                "new_release_count": new_count,
            }
        )
    return pd.DataFrame(snapshot)


def to_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def to_nullable_text(value) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def upsert_snapshots(
    supabase: Client,
    df_snapshot: pd.DataFrame,
    df_ids: pd.DataFrame,
    snapshot_date: str,
    batch_size: int,
) -> None:
    df_snapshot = df_snapshot.merge(df_ids, on="spotify_id", how="left")

    missing = df_snapshot[df_snapshot["group_id"].isna()]
    if not missing.empty:
        print(
            f"Warning: {len(missing)} rows missing group_id. They will be skipped."
        )
        print(missing["spotify_id"].head())

    df_snapshot = df_snapshot.dropna(subset=["group_id"])

    records = []
    for _, row in df_snapshot.iterrows():
        records.append(
            {
                "snapshot_date": snapshot_date,
                "group_id": row["group_id"],
                "spotify_id": row["spotify_id"],
                "name": row["name"] if isinstance(row["name"], str) else "N/A",
                "artist_popularity": to_int(row["artist_popularity"]),
                "followers": to_int(row["followers"]),
                "track_popularity_sum": to_int(row["track_popularity_sum"]),
                "new_release_count": to_int(row["new_release_count"]),
            }
        )

    if not records:
        raise Exception("No records to upsert.")

    for start in range(0, len(records), batch_size):
        batch = records[start : start + batch_size]
        supabase.schema("ihc").table("artist_snapshots").upsert(
            batch,
            on_conflict="snapshot_date,group_id",
        ).execute()
        print(f"Upserted {start + len(batch)} / {len(records)}")


def update_group_images(
    supabase: Client,
    df_snapshot: pd.DataFrame,
    df_ids: pd.DataFrame,
    batch_size: int,
) -> None:
    df_image = df_snapshot[["spotify_id", "artist_image_url"]].merge(
        df_ids, on="spotify_id", how="left"
    )
    df_image = df_image.dropna(subset=["group_id"])
    df_image = df_image.drop_duplicates(subset=["group_id"], keep="last")

    image_rows = []
    for _, row in df_image.iterrows():
        group_id = to_nullable_text(row.get("group_id"))
        if not group_id:
            continue
        image_rows.append(
            {
                "group_id": group_id,
                "artist_image_url": to_nullable_text(row.get("artist_image_url")),
            }
        )

    if not image_rows:
        print("No group image rows to update.")
        return

    updated = 0
    now_iso = datetime.now(ZoneInfo("Asia/Tokyo")).isoformat()
    for start in range(0, len(image_rows), batch_size):
        batch = image_rows[start : start + batch_size]
        for row in batch:
            payload = {
                "artist_image_url": to_nullable_text(row["artist_image_url"]),
                "artist_image_source": "spotify",
                "artist_image_updated_at": to_nullable_text(now_iso),
            }
            supabase.schema("imd").table("groups").update(payload).eq(
                "id", to_nullable_text(row["group_id"])
            ).execute()
            updated += 1
        print(f"Updated group images {updated} / {len(image_rows)}")


def main() -> None:
    supabase = get_supabase_client()
    df_ids = fetch_spotify_ids(supabase)

    client_id = require_env("SPOTIFY_CLIENT_ID")
    client_secret = require_env("SPOTIFY_CLIENT_SECRET")
    token_manager = TokenManager(client_id, client_secret)

    df_snapshot = fetch_snapshot(df_ids, token_manager)

    for round_num in range(3):
        na_ids = df_snapshot[
            (df_snapshot["name"] == "N/A")
            | ((df_snapshot["followers"] == 0) & (df_snapshot["artist_popularity"] > 0))
        ]["spotify_id"]

        if len(na_ids) == 0:
            print("All data retrieved.")
            break

        print(f"Retry round {round_num + 1}: {len(na_ids)} items")
        time.sleep(5)
        retry_df = fetch_snapshot(df_ids[df_ids["spotify_id"].isin(na_ids)], token_manager)
        df_snapshot = df_snapshot.set_index("spotify_id")
        df_snapshot.update(retry_df.set_index("spotify_id"))
        df_snapshot = df_snapshot.reset_index()

    df_snapshot = df_snapshot.drop_duplicates(subset="spotify_id", keep="last")

    snapshot_date = os.getenv("SNAPSHOT_DATE")
    if not snapshot_date:
        snapshot_date = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d")

    batch_size = int(os.getenv("BATCH_SIZE", "500"))
    print(f"Snapshot date: {snapshot_date}")
    upsert_snapshots(supabase, df_snapshot, df_ids, snapshot_date, batch_size)
    update_group_images(supabase, df_snapshot, df_ids, batch_size)


if __name__ == "__main__":
    main()
