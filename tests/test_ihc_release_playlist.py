import copy
import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from ihc_release_core import NAME, build_candidates, latest_album, latest_saturday, next_saturday, qualifying_video, select_update, timestamp, weekly_scores
from ihc_release_playlist_job import bootstrap_plan, description_with_cover, publish
from ihc_release_sources import APIError

def track(i,group=None):
    return {'track_id':str(i),'group_id':str(i if group is None else group),
            'group_name':'Group '+str(i),'spotify_artist_id':'artist'+str(i)}

class Selection(unittest.TestCase):
    def setUp(self):
        self.old=[track(i) for i in range(50)]
        self.seen={r['track_id'] for r in self.old}

    def test_cap_and_relative_retention(self):
        new=[track(i) for i in range(100,140)]
        target,added=select_update(self.old,new,self.seen)
        self.assertEqual(added,new[:25]);self.assertEqual(target,new[:25]+self.old[:25])

    def test_shortage_includes_all_and_preserves_fifty(self):
        target,added=select_update(self.old,[track(100)],self.seen)
        self.assertEqual(target,[track(100)]+self.old[:49]);self.assertEqual(len(added),1)

    def test_zero_candidates_keeps_previous(self):
        self.assertEqual(select_update(self.old,[],self.seen),(self.old,[]))

    def test_history_and_per_update_group_cap(self):
        target,added=select_update(self.old,[track(70),track(100,1),track(101,1),track(102,2)],self.seen|{'70'})
        self.assertEqual([r['track_id'] for r in added],['100','102'])
        self.assertIn(track(1),target) # Same group's older retained track is allowed.

    def test_saturday_boundaries_from_tuesday_seed(self):
        self.assertEqual(next_saturday('2026-11-10'),date(2026,11,14))
        self.assertEqual(next_saturday('2026-11-14'),date(2026,11,21))
        self.assertEqual(latest_saturday(date(2026,11,11)),date(2026,11,7))
        self.assertEqual(latest_saturday(date(2026,11,14)),date(2026,11,14))

    def test_weekly_floor_happens_after_sum_and_not_per_day(self):
        rows=[{'snapshot_date':(date(2026,11,7)+timedelta(days=i)).isoformat(),'group_id':'g',
               'base_score':[-10,15,0,0,0,0,0][i],'score':999,'artist_popularity':20} for i in range(7)]
        scores,missing=weekly_scores(list(reversed(rows)),'2026-11-14')
        self.assertEqual(scores['g']['total_score'],5);self.assertEqual(missing,[])
        with self.assertRaises(ValueError):weekly_scores(rows[:-1],'2026-11-14')

    def test_individual_missing_date_is_held_without_zero_imputation(self):
        rows=[{'snapshot_date':(date(2026,11,7)+timedelta(days=i)).isoformat(),'group_id':'g',
               'base_score':1,'score':1,'artist_popularity':20} for i in range(7)]
        rows += [dict(r,group_id='partial') for r in rows[:6]]
        scores,missing=weekly_scores(rows,'2026-11-14')
        self.assertNotIn('partial',scores);self.assertEqual(missing,['partial'])

    def test_duration_publication_and_archive_boundaries(self):
        cutoff=timestamp('2026-11-14T00:00:00+09:00')
        video={'published_at':'2026-11-13T10:00:00Z','duration':'PT3M1S','privacy_status':'public',
               'upload_status':'processed','live_broadcast_content':'none'}
        self.assertTrue(qualifying_video(video,cutoff))
        self.assertFalse(qualifying_video(dict(video,duration='PT3M'),cutoff))
        self.assertFalse(qualifying_video(dict(video,privacy_status='private'),cutoff))
        self.assertFalse(qualifying_video(dict(video,live_broadcast_content='live'),cutoff))
        self.assertFalse(qualifying_video(dict(video,published_at='2026-11-14T00:00:00Z'),cutoff))
        self.assertTrue(qualifying_video(dict(video,live_details={'actualEndTime':'2026-11-13T11:00:00Z'}),cutoff))
        self.assertFalse(qualifying_video(dict(video,live_details={'actualEndTime':'2026-11-14T11:00:00Z'}),cutoff))

    def test_same_date_album_tie_and_future_or_coarse_dates(self):
        albums=[{'id':i,'release_date':d,'release_date_precision':p} for i,d,p in
                [('b','2026-11-13','day'),('a','2026-11-13','day'),('future','2026-11-14','day'),('coarse','2026','year')]]
        self.assertEqual(latest_album(albums,'2026-11-14')['id'],'a')

    def test_approved_seed_integrity(self):
        cutoff,rows,evidence=bootstrap_plan()
        self.assertEqual(cutoff,'2026-11-10');self.assertEqual(len(rows),50)
        self.assertEqual(rows[0]['track_id'],'5Z2t1LSqsMvihFsHLzXygH')
        self.assertEqual(sum(r['wildcard'] for r in rows),3)
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'seed.json';path.write_text(json.dumps({'tracks':rows,'tracks_sha256':'bad'}))
            with self.assertRaisesRegex(ValueError,'hash mismatch'):bootstrap_plan(path)

    def test_candidate_wildcards_add_independently_and_latest_seen_does_not_fallback(self):
        class Source:
            def catalog(self,group):
                return [{'id':'album','name':'Latest','release_date':'2026-11-13','release_date_precision':'day'}]
            def first_track(self,album):
                return {'id':'new-id-same-recording','name':'Overture','is_playable':True,'artist_ids':['artist']}
            def activity(self,group,cutoff):
                return {'news_ids':['news'],'video_ids':['a','b','c'],'youtube_coverage':['observed']}
        groups=[{'group_id':'g','group_name':'Group','spotify_artist_id':'artist'}]
        scores={'g':{'total_score':-8,'artist_popularity':2,'rank':600,'week_end_date':'2026-11-13'}}
        accepted,audit=build_candidates(groups,scores,Source(),'2026-11-14',{'old-id-same-recording'})
        self.assertEqual(accepted[0]['total_points'],16)
        self.assertTrue(accepted[0]['wildcard']);self.assertEqual(audit,[])
        accepted,audit=build_candidates(groups,scores,Source(),'2026-11-14',{'new-id-same-recording'})
        self.assertEqual(accepted,[]);self.assertEqual(audit[0]['reason'],'track_id_already_published')

class FakeDB:
    def __init__(self):
        self.state={'lock_token':'lease','locked_until':'2100-01-01T00:00:00Z','spotify_playlist_id':'fixed'}
        self.commits=[]
    def project(self,slug):return self.state
    def rpc(self,name,data):
        if name=='finish_release_playlist_run':self.commits.append(data)

class FakeSpotify:
    user='owner'
    def __init__(self,old):
        self.current=list(old);self.put_count=0;self.uploads=[];self.details={'name':NAME,'public':True,'owner':{'id':self.user},'snapshot_id':'snapshot','images':[]}
    def cover(self,aid,gid):return b'jpeg',{'group_id':gid,'sha256':'sourcehash','spotify_artist_id':aid}
    def get(self,path,params=None):
        return {'is_playable':True} if path.startswith('tracks/') else self.details
    def playlist_ids(self,pid):return self.current
    def put(self,path,data):
        if path.endswith('/items'):
            self.put_count+=1;self.current=[s.rsplit(':',1)[-1] for s in data['uris']]
        else:self.details.update(data)
    def upload_cover(self,pid,encoded):self.uploads.append(encoded)

class Publication(unittest.TestCase):
    def setUp(self):
        self.before=[track(i) for i in range(50)]
        self.target=[track(100)]+self.before[:49]
        self.run={'id':'run','mode':'weekly','cutoff':'2026-11-14','before_tracks':self.before,'target_tracks':self.target,'added_tracks':[track(100)]}
        self.db=FakeDB();self.spotify=FakeSpotify([r['track_id'] for r in self.before])

    def test_verify_order_and_cover_from_new_first_artist(self):
        receipt=publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(receipt['tracks'],50);self.assertEqual(self.spotify.put_count,1)
        self.assertEqual(self.db.commits[0]['p_cover']['group_id'],'100')

    def test_owner_description_changes_only_cover_group(self):
        prefix='「今ライブシーンでキテるアイドルは誰なのか!?」アイドル情報専門サイト「IDOL CROSSING」がセレクトしたアイドル楽曲を毎週更新。毎週土曜0時（日本時間）更新。'
        self.spotify.details['description']=prefix+'カバー：可憐なアイボリー'
        publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(self.spotify.details['description'],prefix+'カバー：Group 100')

    def test_reads_latest_copy_after_cover_upload(self):
        self.spotify.details['description']='Earlier copy。カバー：Old'
        def upload(*args):
            self.spotify.details['description']='Owner edited copy。カバー：Old'
        self.spotify.upload_cover=upload
        publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(self.spotify.details['description'],'Owner edited copy。カバー：Group 100')

    def test_description_verification_failure_leaves_run_pending(self):
        put=self.spotify.put
        def ignore_description(path,data):
            put(path,{k:v for k,v in data.items() if k!='description'})
        self.spotify.put=ignore_description
        with self.assertRaisesRegex(APIError,'verification failed'):
            publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(self.db.commits,[])

    def test_retry_after_spotify_write_does_not_add_twice(self):
        def fail(*args):raise APIError('temporary cover failure')
        self.spotify.upload_cover=fail
        with self.assertRaises(APIError):publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(self.spotify.put_count,1);self.assertEqual(self.db.commits,[])
        self.spotify.upload_cover=lambda *args:None
        publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(self.spotify.put_count,1);self.assertEqual(len(self.db.commits),1)

    def test_manual_drift_is_preserved(self):
        self.spotify.current=['manual-edit']
        with self.assertRaisesRegex(APIError,'drift'):publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(self.spotify.put_count,0);self.assertEqual(self.db.commits,[])

    def test_lost_lease_stops_writes(self):
        self.db.state['lock_token']='another-publisher'
        with self.assertRaisesRegex(APIError,'lease'):publish(self.db,self.spotify,self.run,'lease')
        self.assertEqual(self.spotify.put_count,0)

class Description(unittest.TestCase):
    def test_preserves_other_lines_and_credit_spacing(self):
        self.assertEqual(description_with_cover('本文\r\nカバー： Old\r\nその他の案内','New'),
                         '本文\r\nカバー： New\r\nその他の案内')

    def test_missing_credit_keeps_copy_and_appends_credit(self):
        for description,expected in [('Owner copy','Owner copy\nカバー：New'),
                                     ('Owner copy\n','Owner copy\nカバー：New'),
                                     (None,'カバー：New')]:
            with self.subTest(description=description):
                self.assertEqual(description_with_cover(description,'New'),expected)

if __name__=='__main__':unittest.main()
