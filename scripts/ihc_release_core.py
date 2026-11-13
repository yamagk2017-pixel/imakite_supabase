"""Deterministic selection for イマキテアイドル：ジャパン (no I/O)."""
import math
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

JST = ZoneInfo('Asia/Tokyo')
SLUG = 'imakite-idol-japan'
NAME = 'イマキテアイドル：ジャパン'
ALGORITHM = 'ihc-new-release-v2'

def timestamp(raw):
    return datetime.fromisoformat(raw.replace('Z', '+00:00')).astimezone(JST)

def next_saturday(last_cutoff):
    day = date.fromisoformat(last_cutoff)
    return day + timedelta(days=(5-day.weekday()) % 7 or 7)

def latest_saturday(today=None):
    day = today or datetime.now(JST).date()
    return day-timedelta(days=(day.weekday()-5) % 7)

def seconds(raw):
    match = re.fullmatch(r'P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?', raw or '')
    if not match:
        return None
    days, hours, minutes, sec = [float(x or 0) for x in match.groups()]
    return days*86400+hours*3600+minutes*60+sec

def qualifying_video(video, cutoff):
    if not video.get('published_at') or video.get('privacy_status')!='public' or video.get('upload_status')!='processed':
        return False
    duration = seconds(video.get('duration'))
    if duration is None or duration<=180 or not cutoff-timedelta(days=7)<=timestamp(video['published_at'])<cutoff:
        return False
    live = video.get('live_details') or {}
    return video.get('live_broadcast_content')=='none' and (not live or
           bool(live.get('actualEndTime') and timestamp(live['actualEndTime'])<cutoff))

def weekly_scores(rows, cutoff):
    end = date.fromisoformat(cutoff)-timedelta(days=1)
    expected = {(end-timedelta(days=i)).isoformat() for i in range(7)}
    if {r['snapshot_date'] for r in rows} != expected:
        raise ValueError('Seven complete daily dates are required; leave the playlist unchanged')
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['group_id']].append(row)
    values, missing = [], []
    for gid, group in grouped.items():
        group = sorted(group, key=lambda r:r['snapshot_date'])
        if {r['snapshot_date'] for r in group} != expected or len(group)!=7 or group[-1].get('artist_popularity') is None:
            missing.append(gid)
            continue
        total = 0.0
        for row in group:
            value = row.get('base_score')
            value = row.get('score') if value is None else value
            if value is None or not math.isfinite(float(value)):
                raise ValueError('Invalid daily score; leave playlist unchanged')
            total += float(value)
        values.append({'group_id':gid,'total_score':total,'artist_popularity':group[-1]['artist_popularity'],
                       'week_end_date':end.isoformat()})
    values.sort(key=lambda r:(-r['total_score'],r['group_id']))
    return {r['group_id']:dict(r,rank=index+1) for index,r in enumerate(values)}, missing

def latest_album(albums, cutoff):
    valid = [a for a in albums if a.get('release_date_precision')=='day' and
             re.fullmatch(r'\d{4}-\d{2}-\d{2}',a.get('release_date') or '') and a['release_date']<cutoff]
    return min(valid, key=lambda a:(-int(a['release_date'].replace('-','')),a['id'])) if valid else None

def build_candidates(groups, scores, source, cutoff_date, ever_seen):
    cutoff = timestamp(cutoff_date+'T00:00:00+09:00')
    lower = (cutoff.date()-timedelta(days=7)).isoformat()
    accepted, audit = [], []
    for group in groups:
        gid = group['group_id']
        def reject(reason):
            audit.append({'group_id':gid,'group_name':group['group_name'],'reason':reason})
        weekly = scores.get(gid)
        if not weekly:
            reject('weekly_daily_input_incomplete'); continue
        album = latest_album(source.catalog(group), cutoff_date)
        if not album or album['release_date']<lower:
            reject('latest_release_outside_window_or_undated'); continue
        track = source.first_track(album['id'])
        if not track or track.get('is_playable') is not True or group['spotify_artist_id'] not in track.get('artist_ids',[]):
            reject('first_track_not_confirmed_playable_or_artist_mismatch'); continue
        if track['id'] in ever_seen:
            reject('track_id_already_published'); continue
        activity = source.activity(group, cutoff)
        news_count, video_count = len(activity['news_ids']),len(activity['video_ids'])
        popularity = weekly['artist_popularity']
        if popularity<10 and not news_count and not video_count:
            reject('popularity_below_10_without_observed_wildcard'); continue
        base, news, youtube = max(0.0,weekly['total_score']),10 if news_count else 0,3*min(video_count,2)
        accepted.append({'group_id':gid,'group_name':group['group_name'],'spotify_artist_id':group['spotify_artist_id'],
            'track_id':track['id'],'track_uri':'spotify:track:'+track['id'],'track_name':track['name'],
            'album_id':album['id'],'album_name':album['name'],'release_date':album['release_date'],
            'ihc_rank':weekly['rank'],'ihc_score':weekly['total_score'],'ihc_week_end':weekly['week_end_date'],
            'popularity':popularity,'base_points':base,'news_points':news,'youtube_points':youtube,
            'total_points':base+news+youtube,'news_count':news_count,'qualifying_video_count':video_count,
            'wildcard':popularity<10,'activity':activity})
    accepted.sort(key=lambda r:(-r['total_points'],r['ihc_rank'],-int(r['release_date'].replace('-','')),r['group_id'],r['album_id'],r['track_id']))
    unique, seen = [], set()
    for row in accepted:
        if row['track_id'] not in seen:
            unique.append(row); seen.add(row['track_id'])
        else:
            audit.append({'group_id':row['group_id'],'reason':'shared_track_id_deduplicated'})
    return unique,audit

def select_update(previous, candidates, ever_seen):
    added, groups, ids = [],set(),set()
    for row in candidates:
        if row['track_id'] in ever_seen or row['track_id'] in ids or row['group_id'] in groups:
            continue
        added.append(row); ids.add(row['track_id']); groups.add(row['group_id'])
        if len(added)==25:
            break
    retained = [row for row in previous if row['group_id'] not in groups]
    target = (added+retained)[:50]
    if len(target)!=50 or len({r['track_id'] for r in target})!=50 or len({r['group_id'] for r in target})!=50:
        raise ValueError('Update must retain exactly 50 unique tracks and groups')
    return target,added
