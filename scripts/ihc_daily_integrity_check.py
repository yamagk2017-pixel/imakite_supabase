import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from supabase import Client, create_client


GITHUB_API_VERSION = "2022-11-28"
RUN_LOOKBACK_SECONDS = 60
RUN_POLL_INTERVAL_SECONDS = 15
RUN_WAIT_TIMEOUT_SECONDS = 60 * 60


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


def github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }


def parse_github_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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
) -> datetime:
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"
    dispatched_at = datetime.now(timezone.utc)
    payload = {
        "ref": ref,
        "inputs": {"snapshot_date": target_date},
        "return_run_details": True,
    }
    if workflow_file == "ihc_ranking.yml":
        payload["inputs"]["reset_daily_top20"] = "true"

    response = requests.post(
        url, headers=github_headers(token), json=payload, timeout=30
    )
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

    return dispatched_at


def find_dispatched_run(
    workflow_file: str,
    ref: str,
    dispatched_at: datetime,
    token: str,
    repo: str,
) -> dict | None:
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/runs"
    response = requests.get(
        url,
        headers=github_headers(token),
        params={"event": "workflow_dispatch", "branch": ref, "per_page": 20},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to list runs for {workflow_file}: "
            f"status={response.status_code} body={response.text}"
        )

    threshold = dispatched_at - timedelta(seconds=RUN_LOOKBACK_SECONDS)
    candidates = []
    for run in response.json().get("workflow_runs", []):
        created_at = parse_github_datetime(run["created_at"])
        if created_at >= threshold:
            candidates.append((created_at, run))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def get_workflow_run(run_id: int, token: str, repo: str) -> dict:
    url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}"
    response = requests.get(url, headers=github_headers(token), timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to get workflow run {run_id}: "
            f"status={response.status_code} body={response.text}"
        )
    return response.json()


def wait_for_dispatched_workflow(
    workflow_file: str,
    ref: str,
    dispatched_at: datetime,
    token: str,
    repo: str,
) -> dict:
    deadline = time.monotonic() + RUN_WAIT_TIMEOUT_SECONDS
    run_id = None

    while time.monotonic() < deadline:
        if run_id is None:
            run = find_dispatched_run(workflow_file, ref, dispatched_at, token, repo)
            if run is None:
                print(f"Waiting for dispatched {workflow_file} run to appear...")
                time.sleep(RUN_POLL_INTERVAL_SECONDS)
                continue
            run_id = run["id"]
            print(
                f"Found dispatched {workflow_file}: "
                f"run_id={run_id} url={run.get('html_url', 'n/a')}"
            )
        else:
            run = get_workflow_run(run_id, token, repo)

        status = run.get("status")
        conclusion = run.get("conclusion")
        if status == "completed":
            if conclusion != "success":
                raise RuntimeError(
                    f"{workflow_file} run_id={run_id} completed with "
                    f"conclusion={conclusion}"
                )
            print(f"{workflow_file} run_id={run_id} completed successfully.")
            return run

        print(f"Waiting for {workflow_file} run_id={run_id}: status={status}")
        time.sleep(RUN_POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"Timed out waiting for {workflow_file} workflow_dispatch run to complete."
    )


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
                "Would dispatch ihc_snapshot.yml, wait for it to complete, "
                "then dispatch ihc_ranking.yml."
            )
            return
        if ranking_missing:
            print("Would dispatch ihc_ranking.yml.")
            return

    github_token = require_env("GITHUB_TOKEN")
    repo = require_env("GITHUB_REPOSITORY")
    dispatch_ref = os.getenv("DISPATCH_REF", "").strip() or "main"

    if snapshot_missing:
        snapshot_dispatched_at = dispatch_workflow(
            workflow_file="ihc_snapshot.yml",
            target_date=target_date,
            ref=dispatch_ref,
            token=github_token,
            repo=repo,
        )
        wait_for_dispatched_workflow(
            workflow_file="ihc_snapshot.yml",
            ref=dispatch_ref,
            dispatched_at=snapshot_dispatched_at,
            token=github_token,
            repo=repo,
        )
        ranking_dispatched_at = dispatch_workflow(
            workflow_file="ihc_ranking.yml",
            target_date=target_date,
            ref=dispatch_ref,
            token=github_token,
            repo=repo,
        )
        wait_for_dispatched_workflow(
            workflow_file="ihc_ranking.yml",
            ref=dispatch_ref,
            dispatched_at=ranking_dispatched_at,
            token=github_token,
            repo=repo,
        )
        print("Snapshot and ranking backfill completed.")
        return

    if ranking_missing:
        ranking_dispatched_at = dispatch_workflow(
            workflow_file="ihc_ranking.yml",
            target_date=target_date,
            ref=dispatch_ref,
            token=github_token,
            repo=repo,
        )
        wait_for_dispatched_workflow(
            workflow_file="ihc_ranking.yml",
            ref=dispatch_ref,
            dispatched_at=ranking_dispatched_at,
            token=github_token,
            repo=repo,
        )
        print("Ranking was missing and has been backfilled.")


if __name__ == "__main__":
    main()
