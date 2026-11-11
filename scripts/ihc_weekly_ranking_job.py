import os
import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from supabase import Client, create_client

from ihc_score_algorithms import validate_algorithm_version


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

    algorithm_version = validate_algorithm_version(
        os.getenv("IHC_WEEKLY_ALGORITHM_VERSION"), default="v1"
    )
    print("Weekly ranking algorithm:", algorithm_version)

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
            .select(
                "snapshot_date,group_id,score,base_score,conversion_momentum,"
                "algorithm_version,artist_popularity"
            )
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

    # v1 always aggregates the legacy base score. v2 adds conversion momentum
    # once at the week end instead of summing the same residual seven times.
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
    df["base_score"] = pd.to_numeric(df["base_score"], errors="coerce")
    df["base_score"] = df["base_score"].fillna(df["score"])
    df["conversion_momentum"] = pd.to_numeric(
        df["conversion_momentum"], errors="coerce"
    ).fillna(0)

    base_totals = (
        df.groupby("group_id", as_index=False)["base_score"]
        .sum()
        .rename(columns={"base_score": "base_total_score"})
    )

    # Use latest day's artist_popularity within the week
    latest = (
        df.sort_values(by=["snapshot_date"])\
          .groupby("group_id", as_index=False)\
          .tail(1)[["group_id", "artist_popularity", "conversion_momentum"]]
    )

    if algorithm_version == "v2":
        observed_dates = set(df["snapshot_date"].unique())
        expected_dates = {
            week_start_date + timedelta(days=offset) for offset in range(7)
        }
        missing_dates = sorted(expected_dates - observed_dates)
        if missing_dates:
            raise ValueError(
                "Weekly v2 requires seven complete daily dates; missing: "
                + ", ".join(day.isoformat() for day in missing_dates)
            )
        daily_versions = set(df["algorithm_version"].dropna().astype(str))
        if daily_versions != {"v2"}:
            raise ValueError(
                "Weekly v2 requires all source rows to use daily algorithm v2; "
                f"found: {sorted(daily_versions)}"
            )

    # Attach name_ja
    group_ids = base_totals["group_id"].dropna().unique().tolist()
    name_map = fetch_group_names(supabase, group_ids)

    ranking = base_totals.merge(latest, on="group_id", how="left")
    ranking["v2_candidate_total_score"] = (
        ranking["base_total_score"] + ranking["conversion_momentum"]
    )
    ranking["v1_candidate_rank"] = (
        ranking["base_total_score"].rank(method="first", ascending=False).astype(int)
    )
    ranking["v2_candidate_rank"] = (
        ranking["v2_candidate_total_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    if algorithm_version == "v2":
        ranking["total_score"] = ranking["v2_candidate_total_score"]
    else:
        ranking["total_score"] = ranking["base_total_score"]
    ranking["artist_name"] = ranking["group_id"].map(name_map)
    if algorithm_version == "v1":
        # Preserve the legacy stable ordering for tied weekly scores.
        ranking = ranking.sort_values(
            by="total_score", ascending=False
        ).reset_index(drop=True)
    else:
        ranking = ranking.sort_values(
            by=["total_score", "group_id"], ascending=[False, True]
        ).reset_index(drop=True)
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
                "base_total_score": to_float(row.get("base_total_score")),
                "conversion_momentum": to_float(row.get("conversion_momentum")),
                "v2_candidate_total_score": to_float(
                    row.get("v2_candidate_total_score")
                ),
                "v1_candidate_rank": to_int(row.get("v1_candidate_rank")),
                "v2_candidate_rank": to_int(row.get("v2_candidate_rank")),
                "algorithm_version": algorithm_version,
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
