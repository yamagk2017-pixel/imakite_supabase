"""One-time PKCE consent for playlist publication and artist-image covers.

The user opens the printed authorization URL. Tokens are never printed.
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

REDIRECT='http://127.0.0.1:8888/callback'
SCOPES='playlist-read-private playlist-modify-private playlist-modify-public ugc-image-upload'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client-id',required=True)
    parser.add_argument('--output',default='.secrets/ihc-playlist-token.json')
    args=parser.parse_args()
    verifier=secrets.token_urlsafe(64)
    challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    state=secrets.token_urlsafe(32)
    result={}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):
            pass # OAuth codes and state must not enter HTTP logs.
        def do_GET(self):
            query=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            valid=urllib.parse.urlparse(self.path).path=='/callback' and hmac.compare_digest(query.get('state',[''])[0],state)
            if not valid:
                self.send_response(400);self.end_headers();self.wfile.write(b'Invalid callback.');return
            if query.get('error'):
                result['error']='Authorization was declined'
            elif query.get('code'):
                result['code']=query['code'][0]
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers()
            self.wfile.write('<html lang="ja"><title>Spotify認証</title><p>認証を受け付けました。Codexのチャットに戻ってください。</p></html>'.encode())
    server=HTTPServer(('127.0.0.1',8888),Handler)
    server.timeout=1
    url='https://accounts.spotify.com/authorize?'+urllib.parse.urlencode({'client_id':args.client_id,'response_type':'code',
        'redirect_uri':REDIRECT,'scope':SCOPES,'code_challenge_method':'S256','code_challenge':challenge,'state':state})
    print('AUTHORIZE_URL='+url,flush=True)
    deadline=time.monotonic()+1800
    try:
        while not result and time.monotonic()<deadline:
            server.handle_request()
    finally:
        server.server_close()
    if not result.get('code'):
        raise SystemExit(result.get('error','Authorization timed out'))
    body=urllib.parse.urlencode({'grant_type':'authorization_code','client_id':args.client_id,'code':result['code'],
        'redirect_uri':REDIRECT,'code_verifier':verifier}).encode()
    request=urllib.request.Request('https://accounts.spotify.com/api/token',data=body,
            headers={'Content-Type':'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(request,timeout=30) as response:
        token=json.load(response)
    missing=set(SCOPES.split())-set(token.get('scope','').split())
    if missing:
        raise SystemExit('Required scopes were not granted: '+','.join(sorted(missing)))
    output=Path(args.output);output.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(output,os.O_CREAT|os.O_TRUNC|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as handle:
        json.dump(token,handle)
    print('Authorization saved securely. Scopes: '+token['scope'],flush=True)

if __name__=='__main__':
    main()
