import base64
import hashlib
import os
import random
import string
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import requests
import json

CLIENT_ID = '581c1bdd9d464fef81ade0e46271e24e'
REDIRECT_URI = 'http://127.0.0.1:8888/callback'
SCOPE = 'playlist-modify-private playlist-modify-public'

# PKCE用のcode verifierとcode challenge生成
def generate_code_verifier():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=64))

def generate_code_challenge(verifier):
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b'=').decode('utf-8')

class AuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed_path.query)
        if 'code' in query:
            self.server.auth_code = query['code'][0]
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write("認証が完了しました。このウィンドウは閉じて構いません。".encode('utf-8'))
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write("認証コードが見つかりませんでした。".encode('utf-8'))

def start_http_server():
    server = HTTPServer(('localhost', 8888), AuthHandler)
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    return server

def get_tokens(client_id, code, code_verifier, redirect_uri):
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': redirect_uri,
        'client_id': client_id,
        'code_verifier': code_verifier
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post('https://accounts.spotify.com/api/token', data=data, headers=headers)
    response.raise_for_status()
    return response.json()

def main():
    code_verifier = generate_code_verifier()
    code_challenge = generate_code_challenge(code_verifier)

    auth_url = (
        f"https://accounts.spotify.com/authorize?client_id={CLIENT_ID}"
        f"&response_type=code&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
        f"&scope={urllib.parse.quote(SCOPE)}"
        f"&code_challenge_method=S256&code_challenge={code_challenge}"
    )

    print("🔗 以下のURLを開いてSpotifyにログインしてください：")
    print(auth_url)

    webbrowser.open(auth_url)
    httpd = start_http_server()
    httpd.handle_request()
    code = getattr(httpd, 'auth_code', None)

    if code is None:
        print("❌ 認証コードが取得できませんでした。")
        return

    try:
        token_info = get_tokens(CLIENT_ID, code, code_verifier, REDIRECT_URI)
        with open("token.json", "w") as f:
            json.dump(token_info, f)
        print("✅ アクセストークン取得成功。'token.json' に保存しました。")
    except requests.HTTPError as e:
        print(f"❌ トークン取得エラー: {e}")
        print(e.response.text)

if __name__ == "__main__":
    main()
