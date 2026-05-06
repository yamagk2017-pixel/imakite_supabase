import os
import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
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


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_week_end_date() -> date:
    raw = os.getenv("WEEK_END_DATE")
    if raw:
        return datetime.fromisoformat(raw).date()
    # Default to yesterday (JST) so Tue 00:00 run aggregates Tue-Mon.
    return (datetime.now(ZoneInfo("Asia/Tokyo")).date() - timedelta(days=1))


def fetch_group_names(supabase: Client, group_ids: list[str]) -> dict[str, str]:
    if not group_ids:
        return {}

    unique_group_ids = list(dict.fromkeys(group_ids))
    chunk_size = int(os.getenv("GROUP_NAMES_CHUNK_SIZE", "200"))
    if chunk_size <= 0:
        raise ValueError("GROUP_NAMES_CHUNK_SIZE must be greater than 0.")

    total_chunks = math.ceil(len(unique_group_ids) / chunk_size)
    print(
        f"Resolving group names: ids={len(unique_group_ids)} chunk_size={chunk_size} chunks={total_chunks}"
    )

    rows = []
    for index in range(total_chunks):
        start = index * chunk_size
        end = start + chunk_size
        id_chunk = unique_group_ids[start:end]
        resp_names = (
            supabase.schema("imd")
            .table("groups")
            .select("id, name_ja")
            .in_("id", id_chunk)
            .execute()
        )
        batch = resp_names.data or []
        rows.extend(batch)
        print(
            f"Fetched group names chunk {index + 1}/{total_chunks}: requested={len(id_chunk)} returned={len(batch)}"
        )

    df_names = pd.json_normalize(rows)
    if df_names.empty:
        return {}
    df_names = df_names.drop_duplicates(subset=["id"])
    return dict(zip(df_names["id"], df_names["name_ja"]))


def main() -> None:
    supabase = get_supabase_client()

    week_end_date = parse_week_end_date()
    week_start_date = week_end_date - timedelta(days=6)
    print(f"Weekly range: {week_start_date} - {week_end_date}")

    # Pull daily rankings for the last 7 days (score is already x10)
    # Fetch all rows (PostgREST default limit is 1000)
    page_size = int(os.getenv("PAGE_SIZE", "1000"))
    offset = 0
    rows = []
    while True:
        response = (
            supabase.schema("ihc")
            .table("daily_rankings")
            .select("snapshot_date, group_id, score, artist_popularity")
            .gte("snapshot_date", week_start_date.isoformat())
            .lte("snapshot_date", week_end_date.isoformat())
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
    df = pd.json_normalize(rows)
    if df.empty:
        raise ValueError("No daily_rankings data for the specified week.")

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    print(
        "Fetched daily_rankings:",
        f"rows={len(df)}",
        f"min_date={df['snapshot_date'].min()}",
        f"max_date={df['snapshot_date'].max()}",
    )

    # Aggregate total score per artist (score is already x10)
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)

    totals = df.groupby("group_id", as_index=False)["score"].sum()
    totals = totals.rename(columns={"score": "total_score"})

    # Use latest day's artist_popularity within the week
    latest = (
        df.sort_values(by=["snapshot_date"])\
          .groupby("group_id", as_index=False)\
          .tail(1)[["group_id", "artist_popularity"]]
    )

    # Attach name_ja
    group_ids = totals["group_id"].dropna().unique().tolist()
    name_map = fetch_group_names(supabase, group_ids)

    ranking = totals.merge(latest, on="group_id", how="left")
    ranking["artist_name"] = ranking["group_id"].map(name_map)
    ranking = ranking.sort_values(by="total_score", ascending=False).reset_index(drop=True)
    ranking.insert(0, "rank", ranking.index + 1)

    # Previous week ranks
    prev_week_end = week_end_date - timedelta(days=7)
    resp_prev = (
        supabase.schema("ihc")
        .table("weekly_rankings")
        .select("group_id, rank")
        .eq("week_end_date", prev_week_end.isoformat())
        .execute()
    )
    df_prev = pd.json_normalize(resp_prev.data or [])
    if df_prev.empty:
        ranking["prev_rank"] = None
    else:
        prev_map = dict(zip(df_prev["group_id"], df_prev["rank"]))
        ranking["prev_rank"] = ranking["group_id"].map(prev_map)

    records = []
    for _, row in ranking.iterrows():
        records.append(
            {
                "week_end_date": week_end_date.isoformat(),
                "rank": to_int(row["rank"]),
                "prev_rank": to_int(row.get("prev_rank")),
                "group_id": row["group_id"],
                "artist_name": row.get("artist_name"),
                "total_score": to_float(row.get("total_score")),
                "artist_popularity": to_int(row.get("artist_popularity")),
            }
        )

    batch_size = int(os.getenv("BATCH_SIZE", "500"))
    for start in range(0, len(records), batch_size):
        batch = records[start : start + batch_size]
        (
            supabase.schema("ihc")
            .table("weekly_rankings")
            .upsert(batch, on_conflict="week_end_date,group_id")
            .execute()
        )
        print(f"Upserted {start + len(batch)} / {len(records)}")

    print("Weekly rankings saved.")


if __name__ == "__main__":
    main()
