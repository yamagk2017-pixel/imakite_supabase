"""Bootstrap/update the fixed new-release playlist; all mutations require --apply."""
import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from ihc_release_core import ALGORITHM, JST, NAME, SLUG, build_candidates, latest_saturday, next_saturday, select_update, timestamp, weekly_scores
from ihc_release_sources import APIError, Database, HTTP, Sources, Spotify

ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT/'config/imakite-idol-japan.bootstrap-2026-11-10.json'
MARKER = 'IDOL CROSSING 新曲プレイリスト'

def ids(rows):
    return [r['track_id'] for r in rows]

def description_with_cover(description, group_name):
    """Keep the owner's current copy and replace only the cover credit's value."""
    description=description or ''
    updated,count=re.subn(r'(カバー：[ \t]*)[^\r\n]*',
                          lambda match: match[1]+group_name,description,count=1)
    if count:
        return updated
    separator='' if not description or description.endswith(('\r','\n')) else '\n'
    return description+separator+'カバー：'+group_name

def bootstrap_plan(seed_path=SEED):
    seed=json.loads(Path(seed_path).read_text())
    rows=seed['tracks']
    canonical=json.dumps(rows,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
    if hashlib.sha256(canonical).hexdigest()!=seed['tracks_sha256']:
        raise ValueError('Bootstrap seed hash mismatch')
    if len(rows)!=50 or len(set(ids(rows)))!=50 or len({r['group_id'] for r in rows})!=50:
        raise ValueError('Bootstrap requires 50 unique tracks and groups')
    return seed['cutoff'],rows,{'algorithm':ALGORITHM,'seed_sha256':seed['tracks_sha256'],
                             'seed_source':seed['source'],'mode':'approved_simulation_seed'}

def weekly_plan(db, spotify, http, state, cutoff):
    groups=db.groups()
    end=date.fromisoformat(cutoff)-timedelta(days=1)
    start=end-timedelta(days=6)
    daily=db.rows('ihc','daily_rankings',{'select':'snapshot_date,group_id,score,base_score,artist_popularity',
        'snapshot_date':'gte.'+start.isoformat(),'and':'(snapshot_date.lte.'+end.isoformat()+')','order':'snapshot_date,group_id'})
    scores,missing=weekly_scores(daily,cutoff)
    history=db.rows('ihc','release_playlist_history',{'select':'track_id','project_slug':'eq.'+SLUG,'order':'track_id'})
    seen={r['track_id'] for r in history}
    source=Sources(http,spotify,groups)
    candidates,audit=build_candidates(groups,scores,source,cutoff,seen)
    target,added=select_update(state['current_tracks'],candidates,seen)
    evidence={'algorithm':ALGORITHM,'cutoff_jst':cutoff+'T00:00:00+09:00','release_days':7,
              'ihc_input':'sum of daily base_score (v1), floor only the weekly total',
              'daily_dates':[start.isoformat(),end.isoformat()],'daily_rows':len(daily),'incomplete_group_ids':missing,
              'registered_groups':len(groups),'qualified_candidates':len(candidates),'candidates':candidates,'exclusions':audit,
              'youtube_registered_groups':sum(bool(g['youtube_channels']) for g in groups),
              'youtube_resolved_sources':sum(bool(v) for v in source.channels.values()),
              'news_articles':len(source.news),'api_counts':dict(http.counts)}
    return target,added,evidence

def assert_lease(db, lock_token):
    state=db.project(SLUG)
    if state['lock_token']!=lock_token or timestamp(state['locked_until'])<=datetime.now(JST):
        raise APIError('Publisher lease expired; no further Spotify writes allowed')
    return state

def publish(db, spotify, run, lock_token):
    target,before=run['target_tracks'],run['before_tracks']
    # Download the existing artist JPEG without alteration, before changing any tracks.
    encoded,cover=spotify.cover(target[0]['spotify_artist_id'],target[0]['group_id'])
    if run['mode']=='bootstrap':
        for row in target:
            track=spotify.get('tracks/'+row['track_id'],{'market':'JP'})
            if track.get('is_playable') is not True:
                raise APIError('An approved bootstrap track is no longer confirmed playable in JP')
    state=assert_lease(db,lock_token)
    pid=state['spotify_playlist_id']
    if not pid:
        pid=spotify.find_or_create(MARKER,NAME)
        db.rpc('bind_release_playlist',{'p_run_id':run['id'],'p_lock_token':lock_token,'p_playlist_id':pid})
    details=spotify.get('playlists/'+pid)
    if details.get('owner',{}).get('id')!=spotify.user:
        raise APIError('The fixed playlist is not owned by the authenticated Spotify user')
    current=spotify.playlist_ids(pid)
    if current!=ids(target):
        if current!=ids(before):
            raise APIError('Spotify playlist drift detected; preserve the manual changes and reconcile first')
        assert_lease(db,lock_token)
        spotify.put(f'playlists/{pid}/items',{'uris':['spotify:track:'+r['track_id'] for r in target]})
    if spotify.playlist_ids(pid)!=ids(target):
        raise APIError('Spotify returned a different track order; frozen run remains pending')
    assert_lease(db,lock_token)
    spotify.upload_cover(pid,encoded)
    # Read again immediately before writing so manual copy edits during collection survive.
    description=description_with_cover(spotify.get('playlists/'+pid).get('description'),target[0]['group_name'])
    assert_lease(db,lock_token)
    spotify.put('playlists/'+pid,{'name':NAME,'public':True,'description':description})
    verified=spotify.get('playlists/'+pid)
    if (verified.get('name')!=NAME or verified.get('public') is not True
            or verified.get('description')!=description or spotify.playlist_ids(pid)!=ids(target)):
        raise APIError('Playlist verification failed; rerun to reconcile the frozen target')
    cover['spotify_image_urls']=[i['url'] for i in verified.get('images',[])]
    db.rpc('finish_release_playlist_run',{'p_run_id':run['id'],'p_lock_token':lock_token,
        'p_snapshot_id':verified['snapshot_id'],'p_cover':cover})
    return {'playlist_id':pid,'url':'https://open.spotify.com/playlist/'+pid,'run_id':run['id'],
            'cutoff':run['cutoff'],'tracks':50,'added':len(run['added_tracks']),'cover_artist':target[0]['group_name']}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['bootstrap','weekly'],default='weekly')
    parser.add_argument('--cutoff',help='Saturday YYYY-MM-DD for weekly; bootstrap is pinned to its approved seed')
    parser.add_argument('--apply',action='store_true',help='Publish to Spotify and commit dedicated DB history')
    parser.add_argument('--offline-bootstrap',action='store_true',help='Validate the approved seed without any network access')
    args=parser.parse_args()
    out=ROOT/'artifacts/ihc-new-release'
    out.mkdir(parents=True,exist_ok=True)
    seed_cutoff,seed_rows,seed_evidence=bootstrap_plan()
    if args.offline_bootstrap:
        print(json.dumps({'cutoff':seed_cutoff,'tracks':len(seed_rows),'first_group':seed_rows[0]['group_name'],
                          'seed_sha256':seed_evidence['seed_sha256']},ensure_ascii=False)); return
    requested=args.cutoff or latest_saturday().isoformat()
    if args.mode=='weekly':
        day=date.fromisoformat(requested)
        if day.weekday()!=5 or day>datetime.now(JST).date():
            raise ValueError('Weekly cutoff must be a completed Saturday 00:00 JST boundary')
    elif args.cutoff and args.cutoff!=seed_cutoff:
        raise ValueError('Bootstrap cutoff must match the user-approved simulation')
    http=HTTP(); db=Database(http); state=db.project(SLUG)
    spotify=Spotify(http)
    lock_token=str(uuid.uuid4())
    summaries=[]
    try:
        # Resume a prepared update before considering any later cutoff.
        for iteration in range(8):
            state=db.project(SLUG)
            pending=db.rows('ihc','release_playlist_runs',{'select':'*','project_slug':'eq.'+SLUG,'status':'eq.prepared','order':'cutoff'})
            if pending:
                if not args.apply:
                    print('Pending frozen run requires --apply to reconcile:',pending[0]['cutoff']); return
                run=pending[0]
                run=db.rpc('prepare_release_playlist_run',{'p_slug':SLUG,'p_cutoff':run['cutoff'],'p_mode':run['mode'],
                    'p_expected_revision':state['revision'],'p_target':run['target_tracks'],'p_added':run['added_tracks'],
                    'p_evidence':run['evidence'],'p_lock_token':lock_token})
            else:
                if args.mode=='bootstrap':
                    if state['revision']:
                        print('Bootstrap already committed:',state['spotify_playlist_id']); break
                    cutoff,target,added,evidence,mode=seed_cutoff,seed_rows,seed_rows,seed_evidence,'bootstrap'
                else:
                    if not state['last_cutoff']:
                        raise APIError('Run the approved bootstrap before weekly updates')
                    cutoff=next_saturday(state['last_cutoff']).isoformat()
                    if cutoff>requested:
                        print('Already current through:',state['last_cutoff']); break
                    target,added,evidence=weekly_plan(db,spotify,http,state,cutoff)
                    mode='weekly'
                plan={'cutoff':cutoff,'mode':mode,'target':target,'added':added,'evidence':evidence}
                (out/(cutoff+'-plan.json')).write_text(json.dumps(plan,ensure_ascii=False,indent=2)+'\n')
                if not args.apply:
                    print(json.dumps({'dry_run':True,'cutoff':cutoff,'tracks':50,'added':len(added)},ensure_ascii=False)); return
                run=db.rpc('prepare_release_playlist_run',{'p_slug':SLUG,'p_cutoff':cutoff,'p_mode':mode,
                    'p_expected_revision':state['revision'],'p_target':target,'p_added':added,'p_evidence':evidence,'p_lock_token':lock_token})
            if run['status']=='committed':
                continue
            summary=publish(db,spotify,run,lock_token)
            summaries.append(summary)
            (out/(run['cutoff']+'-published.json')).write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps(summary,ensure_ascii=False))
            if args.mode=='bootstrap':
                break
        else:
            raise APIError('More than eight catch-up weeks pending; dispatch explicitly in batches')
        if summaries and os.getenv('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as handle:
                for s in summaries:
                    handle.write(f"{NAME}: [{s['tracks']} tracks]({s['url']}) · cutoff {s['cutoff']} · added {s['added']} · cover {s['cover_artist']}\n\n")
    finally:
        if args.apply:
            db.rpc('unlock_release_playlist',{'p_slug':SLUG,'p_lock_token':lock_token})

if __name__=='__main__':
    try:
        main()
    except (APIError,ValueError) as error:
        print('ERROR:',str(error),file=sys.stderr)
        sys.exit(1)
