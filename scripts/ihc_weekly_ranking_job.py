import os
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
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


def main() -> None:
    supabase = get_supabase_client()

    week_end_date = parse_week_end_date()
    week_start_date = week_end_date - timedelta(days=6)
    print(f"Weekly range: {week_start_date} - {week_end_date}")

    # Pull daily rankings for the last 7 days (score is already x10)
    response = (
        supabase.schema("ihc")
        .table("daily_rankings")
        .select("snapshot_date, group_id, score, artist_popularity")
        .gte("snapshot_date", week_start_date.isoformat())
        .lte("snapshot_date", week_end_date.isoformat())
        .execute()
    )
    df = pd.json_normalize(response.data or [])
    if df.empty:
        raise ValueError("No cumulative_rankings data for the specified week.")

    # Aggregate total score per artist (score is already x10)
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)

    debug_group_id = os.getenv("WEEKLY_DEBUG_GROUP_ID")
    if debug_group_id:
        debug_rows = df[df["group_id"] == debug_group_id][["snapshot_date", "score"]]
        print("Debug group rows:")
        print(debug_rows.to_string(index=False))
        print(f"Debug group sum: {debug_rows['score'].sum()}")

    totals = df.groupby("group_id", as_index=False)["score"].sum()
    totals = totals.rename(columns={"score": "total_score"})

    # Use latest day's artist_popularity within the week
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    latest = (
        df.sort_values(by=["snapshot_date"])\
          .groupby("group_id", as_index=False)\
          .tail(1)[["group_id", "artist_popularity"]]
    )

    # Attach name_ja
    group_ids = totals["group_id"].dropna().unique().tolist()
    name_map = {}
    if group_ids:
        resp_names = (
            supabase.schema("imd")
            .table("groups")
            .select("id, name_ja")
            .in_("id", group_ids)
            .execute()
        )
        df_names = pd.json_normalize(resp_names.data or [])
        if not df_names.empty:
            name_map = dict(zip(df_names["id"], df_names["name_ja"]))

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
