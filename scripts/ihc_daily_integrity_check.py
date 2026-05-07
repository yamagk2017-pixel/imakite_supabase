import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from supabase import Client, create_client


GITHUB_API_VERSION = "2022-11-28"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def parse_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def resolve_target_date() -> str:
    raw = os.getenv("TARGET_DATE", "").strip()
    if raw:
        try:
            return datetime.fromisoformat(raw).date().isoformat()
        except ValueError as exc:
            raise ValueError("TARGET_DATE must be a valid date (YYYY-MM-DD).") from exc

    now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    return (now_jst.date() - timedelta(days=1)).isoformat()


def get_supabase_client() -> Client:
    supabase_url = require_env("SUPABASE_URL")
    supabase_key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(supabase_url, supabase_key)


def count_rows_for_date(
    supabase: Client,
    schema: str,
    table: str,
    date_column: str,
    target_date: str,
) -> int:
    response = (
        supabase.schema(schema)
        .table(table)
        .select("group_id", count="exact", head=True)
        .eq(date_column, target_date)
        .execute()
    )
    count = getattr(response, "count", None)
    if count is None:
        data = getattr(response, "data", None) or []
        return len(data)
    return int(count)


def dispatch_workflow(
    workflow_file: str,
    target_date: str,
    ref: str,
    token: str,
    repo: str,
) -> None:
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    payload = {
        "ref": ref,
        "inputs": {"snapshot_date": target_date},
        "return_run_details": True,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    if response.status_code not in (200, 204):
        raise RuntimeError(
            f"Failed to dispatch {workflow_file}: "
            f"status={response.status_code} body={response.text}"
        )

    if response.status_code == 200:
        data = response.json()
        run_id = data.get("workflow_run_id")
        html_url = data.get("html_url")
        print(
            f"Dispatched {workflow_file}: run_id={run_id} "
            f"url={html_url if html_url else 'n/a'}"
        )
    else:
        print(f"Dispatched {workflow_file} (204 No Content)")


def main() -> None:
    target_date = resolve_target_date()
    dry_run = parse_bool(os.getenv("DRY_RUN"))
    print(f"Target date: {target_date}")
    print(f"Dry run: {dry_run}")

    supabase = get_supabase_client()
    snapshot_count = count_rows_for_date(
        supabase,
        schema="ihc",
        table="artist_snapshots",
        date_column="snapshot_date",
        target_date=target_date,
    )
    ranking_count = count_rows_for_date(
        supabase,
        schema="ihc",
        table="daily_rankings",
        date_column="snapshot_date",
        target_date=target_date,
    )

    print(f"artist_snapshots count for {target_date}: {snapshot_count}")
    print(f"daily_rankings count for {target_date}: {ranking_count}")

    snapshot_missing = snapshot_count == 0
    ranking_missing = ranking_count == 0

    if not snapshot_missing and not ranking_missing:
        print("Integrity check passed. No backfill required.")
        return

    if dry_run:
        if snapshot_missing:
            print(
                "Would dispatch ihc_snapshot.yml. "
                "Ranking backfill is chained by ihc_ranking.yml workflow_run."
            )
            return
        if ranking_missing:
            print("Would dispatch ihc_ranking.yml.")
            return

    github_token = require_env("GITHUB_TOKEN")
    repo = require_env("GITHUB_REPOSITORY")
    dispatch_ref = os.getenv("DISPATCH_REF", "").strip() or "main"

    if snapshot_missing:
        dispatch_workflow(
            workflow_file="ihc_snapshot.yml",
            target_date=target_date,
            ref=dispatch_ref,
            token=github_token,
            repo=repo,
        )
        print(
            "Snapshot was missing. Ranking is expected to run automatically "
            "via ihc_ranking.yml (workflow_run on snapshot success)."
        )
        return

    if ranking_missing:
        dispatch_workflow(
            workflow_file="ihc_ranking.yml",
            target_date=target_date,
            ref=dispatch_ref,
            token=github_token,
            repo=repo,
        )
        print("Ranking was missing and has been dispatched.")


if __name__ == "__main__":
    main()
