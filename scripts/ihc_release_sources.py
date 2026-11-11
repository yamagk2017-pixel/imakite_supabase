"""Bounded HTTP access for the new playlist. Never log credentials or response bodies."""
import base64
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from ihc_release_core import qualifying_video

class APIError(RuntimeError):
    pass

def require_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(name+' is required')
    return value

class HTTP:
    def __init__(self):
        self.counts = defaultdict(int)
        self.last_spotify = 0

    def request(self, service, method, url, *, headers=None, body=None, raw=False):
        if self.counts[service]>=3000:
            raise APIError(service+' request budget exceeded')
        for attempt in range(3):
            if service=='spotify':
                time.sleep(max(0,0.2-(time.monotonic()-self.last_spotify)))
                self.last_spotify = time.monotonic()
            self.counts[service] += 1
            try:
                with urllib.request.urlopen(urllib.request.Request(url,data=body,headers=headers or {},method=method),timeout=30) as response:
                    content = response.read(2*1024*1024)
                    return content if raw else (json.loads(content) if content else {})
            except urllib.error.HTTPError as error:
                # POST may have succeeded despite a lost response: caller reconciles it.
                retry = method in ('GET','PUT') and (error.code==429 or error.code>=500) and attempt<2
                if retry:
                    delay = min(60,max(1,int(error.headers.get('Retry-After','2'))))
                    time.sleep(delay); continue
                raise APIError(f'{service} {method} HTTP {error.code}') from None
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if method in ('GET','PUT') and attempt<2:
                    time.sleep(2**attempt); continue
                raise APIError(service+' '+method+' network failure; retry the frozen run') from None
        raise APIError(service+' retry limit')

    def json(self, service, method, url, *, headers=None, data=None):
        headers = dict(headers or {})
        body = None
        if data is not None:
            headers['Content-Type']='application/json'
            body=json.dumps(data,ensure_ascii=False).encode()
        return self.request(service,method,url,headers=headers,body=body)

class Database:
    def __init__(self, http):
        self.http = http
        self.url = require_env('SUPABASE_URL').rstrip('/')+'/rest/v1/'
        key = require_env('SUPABASE_SERVICE_ROLE_KEY')
        self.auth = {'apikey':key,'Authorization':'Bearer '+key}

    def rows(self, schema, table, params):
        out, offset = [],0
        while True:
            query = dict(params,limit=1000,offset=offset)
            batch = self.http.json('supabase','GET',self.url+table+'?'+urllib.parse.urlencode(query),
                    headers=dict(self.auth,**{'Accept-Profile':schema}))
            out.extend(batch)
            if len(batch)<1000:
                return out
            offset += 1000

    def rpc(self, name, data):
        return self.http.json('supabase','POST',self.url+'rpc/'+name,
                headers=dict(self.auth,**{'Content-Profile':'ihc','Accept-Profile':'ihc'}),data=data)

    def project(self, slug):
        rows = self.rows('ihc','release_playlist_projects',{'select':'*','slug':'eq.'+slug})
        if len(rows)!=1:
            raise APIError('Playlist project migration must be applied first')
        return rows[0]

    def groups(self):
        groups = self.rows('imd','groups',{'select':'id,name_ja,created_at','deleted_at':'is.null','order':'id'})
        ids = self.rows('imd','external_ids',{'select':'group_id,service,external_id,url',
                       'service':'in.(spotify,youtube_channel)','order':'group_id,service'})
        by_group = defaultdict(list)
        for row in ids:
            by_group[row['group_id']].append(row)
        result = []
        for g in groups:
            spotify = [r['external_id'] for r in by_group[g['id']] if r['service']=='spotify' and r['external_id']]
            if len(spotify)>1:
                raise APIError('Ambiguous Spotify artist mapping for '+g['id'])
            if spotify:
                result.append({'group_id':g['id'],'group_name':g['name_ja'],'spotify_artist_id':spotify[0],
                    'youtube_channels':[r for r in by_group[g['id']] if r['service']=='youtube_channel']})
        return result

class Spotify:
    REQUIRED_SCOPES = {'playlist-read-private','playlist-modify-public','playlist-modify-private','ugc-image-upload'}
    def __init__(self, http):
        self.http=http
        data = urllib.parse.urlencode({'grant_type':'refresh_token','client_id':require_env('SPOTIFY_CLIENT_ID'),
                                      'refresh_token':require_env('IHC_PLAYLIST_REFRESH_TOKEN')}).encode()
        payload=http.request('oauth','POST','https://accounts.spotify.com/api/token',
                             headers={'Content-Type':'application/x-www-form-urlencoded'},body=data)
        # Persist rotation immediately, even if later source collection or publication fails.
        rotated=payload.get('refresh_token')
        if rotated and os.getenv('SPOTIFY_REFRESH_TOKEN_FILE'):
            path=Path(os.environ['SPOTIFY_REFRESH_TOKEN_FILE'])
            path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
            with os.fdopen(fd,'w') as handle:
                handle.write(rotated)
        scopes=set(payload.get('scope','').split())
        missing=self.REQUIRED_SCOPES-scopes
        if missing:
            raise APIError('Spotify reauthorization required for scopes: '+', '.join(sorted(missing)))
        self.headers={'Authorization':'Bearer '+payload['access_token']}
        self.user=self.get('me')['id']
        expected=os.getenv('SPOTIFY_USER_ID')
        if expected and self.user!=expected:
            raise APIError('Spotify owner differs from configured user')

    def get(self, path, params=None):
        url='https://api.spotify.com/v1/'+path
        if params:
            url+='?'+urllib.parse.urlencode(params)
        return self.http.json('spotify','GET',url,headers=self.headers)

    def put(self, path, data):
        return self.http.json('spotify','PUT','https://api.spotify.com/v1/'+path,headers=self.headers,data=data)

    def playlist_ids(self, pid):
        out, offset = [],0
        while True:
            response=self.get(f'playlists/{pid}/items',{'limit':50,'offset':offset,'market':'JP'})
            for entry in response.get('items',[]):
                track=entry.get('item') or entry.get('track') or {}
                # Relinking must not make our history appear to contain a different recording ID.
                out.append((track.get('linked_from') or track).get('id'))
            if not response.get('next'):
                return out
            offset += 50

    def find_or_create(self, marker, name):
        matches, offset = [],0
        while True:
            payload=self.get('me/playlists',{'limit':50,'offset':offset})
            for p in payload.get('items',[]):
                if p.get('owner',{}).get('id')==self.user and p.get('name')==name:
                    description=p.get('description')
                    if description is None:
                        description=self.get('playlists/'+p['id']).get('description')
                    if marker in (description or ''):
                        matches.append(p['id'])
            if not payload.get('next'):
                break
            offset += 50
        if len(matches)>1:
            raise APIError('Multiple playlists with the project marker; manual reconciliation required')
        if matches:
            return matches[0]
        return self.http.json('spotify','POST','https://api.spotify.com/v1/me/playlists',headers=self.headers,
                  data={'name':name,'description':marker,'public':False})['id']

    def cover(self, artist_id, group_id):
        artist=self.get('artists/'+artist_id)
        for image in sorted(artist.get('images',[]),key=lambda i:-(i.get('width') or 0)):
            url=image.get('url','')
            parsed=urllib.parse.urlparse(url)
            if parsed.scheme!='https' or not parsed.hostname or not parsed.hostname.endswith(('.scdn.co','.spotifycdn.com','.spotifycdn.net')):
                continue
            image_data=self.http.request('artist_image','GET',url,raw=True)
            encoded=base64.b64encode(image_data)
            if image_data.startswith(b'\xff\xd8') and len(encoded)<=256*1024:
                return encoded,{'group_id':group_id,'spotify_artist_id':artist_id,'artist_name':artist['name'],
                                'source_url':url,'sha256':hashlib.sha256(image_data).hexdigest()}
        raise APIError('No artist JPEG fits the Spotify cover requirements')

    def upload_cover(self, pid, encoded):
        self.http.request('spotify','PUT',f'https://api.spotify.com/v1/playlists/{pid}/images',
                headers=dict(self.headers,**{'Content-Type':'image/jpeg'}),body=encoded)

class Sources:
    def __init__(self, http, spotify, groups):
        self.http,self.spotify=http,spotify
        self.albums,self.tracks,self.channels,self.uploads={},{},{},{}
        self.channel_groups=defaultdict(set)
        for group in groups:
            for src in group['youtube_channels']:
                key=src.get('external_id') or src.get('url') or ''
                if key not in self.channels:
                    self.channels[key]=self.resolve_channel(src)
                channel=self.channels[key]
                if channel:
                    self.channel_groups[channel['id']].add(group['group_id'])
        project=require_env('SANITY_PROJECT_ID')
        dataset=require_env('SANITY_DATASET')
        if not project.isalnum() or not all(c.isalnum() or c in '_-' for c in dataset):
            raise APIError('Invalid Sanity project or dataset')
        query='*[_type in ["newsArticle","eventAnnouncement","radioAnnouncement"] && defined(publishedAt) && dateTime(publishedAt)<=dateTime(now()) && count(relatedGroups[defined(imdGroupId)])>0]{_id,publishedAt,"group_ids":relatedGroups[defined(imdGroupId)].imdGroupId}'
        url=f'https://{project}.api.sanity.io/v2026-02-24/data/query/{dataset}?'+urllib.parse.urlencode({'perspective':'published','query':query})
        self.news=http.json('sanity','GET',url).get('result')
        if not isinstance(self.news,list):
            raise APIError('Sanity source did not return a complete result')

    def youtube(self, path, params):
        params=dict(params,key=require_env('YOUTUBE_API_KEY'))
        return self.http.json('youtube','GET','https://www.googleapis.com/youtube/v3/'+path+'?'+urllib.parse.urlencode(params))

    def resolve_channel(self, source):
        eid=source.get('external_id') or ''
        path=urllib.parse.unquote(urllib.parse.urlparse(source.get('url') or '').path)
        if eid.startswith('UC'):
            lookup={'id':eid}
        elif '/channel/' in path:
            lookup={'id':path.split('/channel/')[1].split('/')[0]}
        elif '/@' in path:
            lookup={'forHandle':path.split('/@')[1].split('/')[0]}
        elif '/user/' in path:
            lookup={'forUsername':path.split('/user/')[1].split('/')[0]}
        else:
            return None
        items=self.youtube('channels',dict(lookup,part='id,contentDetails')).get('items',[])
        return items[0] if items else None

    def catalog(self, group):
        aid=group['spotify_artist_id']
        if aid not in self.albums:
            albums={}
            for offset in range(0,10000,50):
                payload=self.spotify.get(f'artists/{aid}/albums',{'include_groups':'album,single','market':'JP','limit':50,'offset':offset})
                for album in payload.get('items',[]):
                    albums[album['id']]={k:album.get(k) for k in ('id','name','release_date','release_date_precision')}
                if not payload.get('next'):
                    break
            else:
                raise APIError('Spotify album pagination exceeded its bound')
            self.albums[aid]=list(albums.values())
        return self.albums[aid]

    def first_track(self, album_id):
        if album_id not in self.tracks:
            items=self.spotify.get(f'albums/{album_id}/tracks',{'market':'JP','limit':1}).get('items',[])
            track=items[0] if items else None
            if track and 'is_playable' not in track:
                track=dict(track,**self.spotify.get('tracks/'+track['id'],{'market':'JP'}))
            if track:
                track=dict(track,artist_ids=[a['id'] for a in track['artists']])
            self.tracks[album_id]=track
        return self.tracks[album_id]

    def videos(self, channel, cutoff):
        key=(channel['id'],cutoff.isoformat())
        if key in self.uploads:
            return self.uploads[key]
        playlist=channel['contentDetails']['relatedPlaylists']['uploads']
        from datetime import timedelta
        from ihc_release_core import timestamp
        lower=cutoff-timedelta(days=7)
        ids,token=set(),None
        for page in range(50):
            params={'part':'contentDetails','playlistId':playlist,'maxResults':50}
            if token:
                params['pageToken']=token
            payload=self.youtube('playlistItems',params)
            dates=[]
            for item in payload.get('items',[]):
                details=item.get('contentDetails',{})
                published=details.get('videoPublishedAt')
                if published:
                    dates.append(timestamp(published))
                if details.get('videoId') and (not published or timestamp(published)>=lower):
                    ids.add(details['videoId'])
            token=payload.get('nextPageToken')
            if not token or (dates and len(dates)==len(payload.get('items',[])) and max(dates)<lower):
                break
        else:
            raise APIError('YouTube uploads window is incomplete')
        videos=[]
        for start in range(0,len(ids),50):
            batch=sorted(ids)[start:start+50]
            payload=self.youtube('videos',{'part':'snippet,status,contentDetails,liveStreamingDetails','id':','.join(batch)})
            for v in payload.get('items',[]):
                s=v.get('snippet',{})
                videos.append({'id':v['id'],'published_at':s.get('publishedAt'),'channel_id':s.get('channelId'),
                    'privacy_status':v.get('status',{}).get('privacyStatus'),'upload_status':v.get('status',{}).get('uploadStatus'),
                    'duration':v.get('contentDetails',{}).get('duration'),'live_broadcast_content':s.get('liveBroadcastContent'),
                    'live_details':v.get('liveStreamingDetails')})
        self.uploads[key]=videos
        return videos

    def activity(self, group, cutoff):
        from ihc_release_core import timestamp
        news=sorted({a['_id'] for a in self.news if group['group_id'] in a.get('group_ids',[]) and timestamp(a['publishedAt'])<cutoff})
        ids,coverage=set(),[]
        for src in group['youtube_channels']:
            channel=self.channels[src.get('external_id') or src.get('url') or '']
            if not channel:
                coverage.append('unresolved_channel'); continue
            if len(self.channel_groups[channel['id']])>1:
                coverage.append('shared_channel_not_attributed'); continue
            coverage.append('observed')
            ids.update(v['id'] for v in self.videos(channel,cutoff) if v.get('channel_id')==channel['id'] and qualifying_video(v,cutoff))
        return {'news_ids':news,'video_ids':sorted(ids),'youtube_coverage':sorted(set(coverage)) or ['no_registered_channel']}
