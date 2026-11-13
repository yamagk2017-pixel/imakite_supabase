"""Regression tests for delayed daily inputs and automatic publication retries."""
import copy
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import ihc_release_playlist_job as job
from ihc_release_core import JST


def daily_rows(start='2026-11-07', days=7):
    return [{'snapshot_date': (date.fromisoformat(start)+timedelta(days=i)).isoformat(),
             'group_id': group} for i in range(days) for group in ('a', 'b')]


class RetryDB:
    def __init__(self, cutoff='2026-11-10', snapshots=None, rankings=None):
        self.state={'last_cutoff': cutoff, 'revision': 1, 'locked_until': None}
        self.snapshots=daily_rows() if snapshots is None else snapshots
        self.rankings=copy.deepcopy(self.snapshots) if rankings is None else rankings
        self.pending=[]
        self.reads=[]
        self.writes=[]

    def project(self, slug):
        return self.state

    def rows(self, schema, table, params):
        self.reads.append(table)
        if table=='release_playlist_runs':
            return self.pending
        rows=self.snapshots if table=='artist_snapshots' else self.rankings
        start=params['snapshot_date'].removeprefix('gte.')
        end=params['and'].removeprefix('(snapshot_date.lte.').removesuffix(')')
        return [r for r in rows if start<=r['snapshot_date']<=end]

    def rpc(self, name, data):
        self.writes.append(name)
        if name=='prepare_release_playlist_run':
            self.pending=[{'id': 'run', 'cutoff': data['p_cutoff'], 'mode': data['p_mode'],
                           'status': 'prepared', 'target_tracks': data['p_target'],
                           'added_tracks': data['p_added'], 'evidence': data['p_evidence']}]
            return self.pending[0]


class Readiness(unittest.TestCase):
    def check(self, db, requested='2026-11-14', now=None):
        return job.automatic_readiness(db, db.state, db.pending, requested, now)

    def test_committed_week_skips_without_reading_daily_inputs(self):
        db=RetryDB(cutoff='2026-11-14')
        self.assertEqual(self.check(db)['status'], 'current')
        self.assertEqual(db.reads, [])

    def test_missing_friday_waits_then_becomes_ready_when_data_arrives(self):
        db=RetryDB(rankings=daily_rows(days=6))
        result=self.check(db)
        self.assertEqual(result['status'], 'waiting')
        self.assertEqual(result['missing_dates'], ['2026-11-13'])
        db.rankings=daily_rows()
        self.assertEqual(self.check(db)['status'], 'ready')

    def test_partial_daily_write_does_not_count_as_complete(self):
        db=RetryDB(rankings=daily_rows()[:-1])
        result=self.check(db)
        self.assertEqual(result['status'], 'waiting')
        self.assertEqual(result['missing_dates'], [])
        self.assertEqual(result['incomplete_dates'], ['2026-11-13'])

    def test_missing_snapshot_also_waits(self):
        db=RetryDB(snapshots=daily_rows(days=6), rankings=daily_rows())
        self.assertEqual(self.check(db)['missing_dates'], ['2026-11-13'])

    def test_newly_registered_group_does_not_require_seven_days_for_that_group(self):
        rows=daily_rows()+[{'snapshot_date': '2026-11-13', 'group_id': 'new'}]
        self.assertEqual(self.check(RetryDB(snapshots=rows))['status'], 'ready')

    def test_oldest_uncommitted_week_is_checked_before_later_weeks(self):
        result=self.check(RetryDB(rankings=[]), requested='2026-11-21')
        self.assertEqual(result['cutoff'], '2026-11-14')
        self.assertEqual(result['status'], 'waiting')

    def test_frozen_run_resumes_without_rechecking_daily_sources(self):
        db=RetryDB(rankings=[])
        db.pending=[{'cutoff': '2026-11-14'}]
        self.assertEqual(self.check(db)['reason'], 'resume_prepared_run')
        self.assertEqual(db.reads, [])

    def test_unexpired_lease_waits_and_expired_lease_can_resume(self):
        db=RetryDB()
        db.pending=[{'cutoff': '2026-11-14'}]
        db.state['locked_until']='2026-11-14T01:00:00+09:00'
        self.assertEqual(self.check(db, now=datetime(2026,11,14,0,45,tzinfo=JST))['status'], 'waiting')
        self.assertEqual(self.check(db, now=datetime(2026,11,14,1,1,tzinfo=JST))['status'], 'ready')


class AutomaticJob(unittest.TestCase):
    def run_job(self, db, requested='2026-11-14'):
        def committed(_db, spotify, run, token):
            db.state['last_cutoff']=run['cutoff']
            db.state['revision']+=1
            db.pending=[]
            return {'tracks': 50, 'url': 'https://example.invalid/playlist',
                    'cutoff': run['cutoff'], 'added': 1, 'cover_artist': 'Group'}

        with tempfile.TemporaryDirectory() as temp, \
             patch.object(job, 'ROOT', Path(temp)), \
             patch.object(job, 'Database', return_value=db), \
             patch.object(job, 'datetime') as clock, \
             patch.object(job, 'Spotify') as spotify, \
             patch.object(job, 'weekly_plan', return_value=(['target'], ['added'], {})) as plan, \
             patch.object(job, 'publish', side_effect=committed) as publish, \
             patch.dict(job.os.environ, {'GITHUB_STEP_SUMMARY': ''}), \
             patch('builtins.print'), \
             patch.object(sys, 'argv', ['job', '--mode', 'weekly', '--cutoff', requested,
                                        '--automatic', '--apply']):
            clock.now.return_value=datetime(2026,11,21,1,tzinfo=JST)
            job.main()
            return spotify, plan, publish

    def test_current_week_has_no_spotify_access_or_database_writes(self):
        db=RetryDB(cutoff='2026-11-14')
        spotify, plan, publish=self.run_job(db)
        spotify.assert_not_called()
        plan.assert_not_called()
        publish.assert_not_called()
        self.assertEqual(db.writes, [])

    def test_missing_data_has_no_spotify_access_or_database_writes(self):
        db=RetryDB(rankings=[])
        spotify, plan, publish=self.run_job(db)
        spotify.assert_not_called()
        plan.assert_not_called()
        publish.assert_not_called()
        self.assertEqual(db.writes, [])

    def test_retry_after_data_arrives_publishes_once_and_later_retry_is_noop(self):
        db=RetryDB(rankings=daily_rows(days=6))
        self.assertEqual(self.run_job(db)[2].call_count, 0)
        db.rankings=daily_rows()
        self.assertEqual(self.run_job(db)[2].call_count, 1)
        self.assertEqual(db.state['last_cutoff'], '2026-11-14')
        self.assertEqual(self.run_job(db)[2].call_count, 0)
        self.assertEqual(db.writes, ['prepare_release_playlist_run', 'unlock_release_playlist'])

    def test_catch_up_commits_ready_week_then_waits_for_next_missing_week(self):
        db=RetryDB()
        spotify, plan, publish=self.run_job(db, requested='2026-11-21')
        self.assertEqual(publish.call_count, 1)
        self.assertEqual(db.state['last_cutoff'], '2026-11-14')
        self.assertEqual(plan.call_count, 1)

    def test_prepared_run_resumes_frozen_target_without_reselection(self):
        db=RetryDB(rankings=[])
        db.pending=[{'id': 'pending', 'cutoff': '2026-11-14', 'mode': 'weekly',
                     'target_tracks': ['frozen'], 'added_tracks': ['added'], 'evidence': {}}]
        spotify, plan, publish=self.run_job(db)
        plan.assert_not_called()
        self.assertEqual(publish.call_args.args[2]['target_tracks'], ['frozen'])


if __name__=='__main__':
    unittest.main()
