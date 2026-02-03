import os
from datetime import date, datetime, timedelta

import requests
from supabase import Client, create_client


PLAYLIST_BASE_NAME = "イマキテ週間ランキングTOP20"
SPOTIFY_MARKET = "JP"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def get_supabase_client() -> Client:
    supabase_url = require_env("SUPABASE_URL")
    supabase_key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(supabase_url, supabase_key)


def get_latest_week_end_date(supabase: Client) -> date:
    resp = (
        supabase.schema("ihc")
        .table("weekly_rankings")
        .select("week_end_date")
        .order("week_end_date", desc=True)
        .limit(1)
        .execute()
    )
    data = resp.data or []
    if not data:
        raise ValueError("No weekly_rankings data found.")
    return datetime.fromisoformat(data[0]["week_end_date"]).date()


def parse_week_end_date(supabase: Client) -> date:
    raw = os.getenv("WEEK_END_DATE")
    if raw:
        return datetime.fromisoformat(raw).date()
    return get_latest_week_end_date(supabase)


def week_label(week_end: date) -> str:
    week_start = week_end - timedelta(days=6)
    return f"({week_start.year}/{week_start.month}/{week_start.day}-{week_end.year}/{week_end.month}/{week_end.day})"


def get_spotify_access_token() -> str:
    client_id = require_env("SPOTIFY_CLIENT_ID")
    client_secret = require_env("SPOTIFY_CLIENT_SECRET")
    refresh_token = require_env("SPOTIFY_REFRESH_TOKEN")
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    response = requests.post("https://accounts.spotify.com/api/token", data=data, timeout=30)
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise ValueError("No access_token returned from Spotify.")
    return token


def spotify_get(url: str, access_token: str, params: dict | None = None) -> dict:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_latest_track_uri(artist_id: str, access_token: str) -> str | None:
    albums = spotify_get(
        f"https://api.spotify.com/v1/artists/{artist_id}/albums",
        access_token,
        params={"include_groups": "single,album", "market": SPOTIFY_MARKET, "limit": 20},
    ).get("items", [])

    latest_album = None
    latest_date = ""
    for album in albums:
        release_date = album.get("release_date", "")
        if release_date > latest_date:
            latest_date = release_date
            latest_album = album

    if latest_album:
        album_id = latest_album["id"]
        tracks = spotify_get(
            f"https://api.spotify.com/v1/albums/{album_id}/tracks",
            access_token,
        ).get("items", [])
        if tracks:
            return tracks[0]["uri"]

    return None


def get_top_track_uri(artist_id: str, access_token: str) -> str | None:
    tracks = spotify_get(
        f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks",
        access_token,
        params={"market": SPOTIFY_MARKET},
    ).get("tracks", [])
    if tracks:
        return tracks[0]["uri"]
    return None


def get_or_create_playlist(access_token: str, user_id: str) -> str:
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.spotify.com/v1/users/{user_id}/playlists"
    next_url = url

    while next_url:
        res = requests.get(next_url, headers=headers, timeout=30)
        res.raise_for_status()
        payload = res.json()
        for playlist in payload.get("items", []):
            if playlist.get("name", "").startswith(PLAYLIST_BASE_NAME):
                return playlist["id"]
        next_url = payload.get("next")

    body = {
        "name": PLAYLIST_BASE_NAME,
        "description": "イマキテ週間ランキングTOP20 自動生成プレイリスト",
        "public": False,
    }
    create_res = requests.post(url, headers=headers, json=body, timeout=30)
    create_res.raise_for_status()
    return create_res.json()["id"]


def replace_playlist_tracks(access_token: str, playlist_id: str, uris: list[str]) -> None:
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    requests.put(url, headers=headers, json={"uris": uris[:100]}, timeout=30).raise_for_status()

    for start in range(100, len(uris), 100):
        chunk = uris[start : start + 100]
        requests.post(url, headers=headers, json={"uris": chunk}, timeout=30).raise_for_status()


def update_playlist(access_token: str, playlist_id: str, new_name: str, description: str) -> None:
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    requests.put(url, headers=headers, json={"name": new_name, "description": description}, timeout=30).raise_for_status()


def main() -> None:
    supabase = get_supabase_client()
    week_end = parse_week_end_date(supabase)
    label = week_label(week_end)

    # Fetch top 20 group_ids
    resp = (
        supabase.schema("ihc")
        .table("weekly_rankings")
        .select("group_id, rank")
        .eq("week_end_date", week_end.isoformat())
        .order("rank")
        .limit(20)
        .execute()
    )
    rankings = resp.data or []
    if not rankings:
        raise ValueError(f"No weekly_rankings for {week_end.isoformat()}")

    group_ids = [row["group_id"] for row in rankings if row.get("group_id")]

    # Map group_id -> spotify artist id
    ids_resp = (
        supabase.schema("imd")
        .table("external_ids")
        .select("group_id, external_id")
        .eq("service", "spotify")
        .in_("group_id", group_ids)
        .execute()
    )
    id_rows = ids_resp.data or []
    id_map = {row["group_id"]: row["external_id"] for row in id_rows}
    artist_ids = [id_map.get(gid) for gid in group_ids if id_map.get(gid)]

    if not artist_ids:
        raise ValueError("No spotify artist ids found for weekly top20.")

    access_token = get_spotify_access_token()
    user_id = require_env("SPOTIFY_USER_ID")

    track_uris = []
    for artist_id in artist_ids:
        uri = get_latest_track_uri(artist_id, access_token)
        if not uri:
            uri = get_top_track_uri(artist_id, access_token)
        if uri:
            track_uris.append(uri)

    if not track_uris:
        raise ValueError("No track URIs resolved from Spotify.")

    playlist_id = get_or_create_playlist(access_token, user_id)
    replace_playlist_tracks(access_token, playlist_id, track_uris)
    new_name = f"{PLAYLIST_BASE_NAME} {label}"
    update_playlist(access_token, playlist_id, new_name, f"イマキテランキング集計期間：{label}")
    print(f"✅ プレイリスト作成・更新完了: https://open.spotify.com/playlist/{playlist_id}")


if __name__ == "__main__":
    main()
