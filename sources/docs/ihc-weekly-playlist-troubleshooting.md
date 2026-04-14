# IHC Weekly Playlist 障害対応マニュアル（2026-03）

## 目的
`IHC Weekly Playlist to Spotify` 実行時に発生した認証エラーの原因と復旧手順を記録し、次回同様の障害が起きた際に短時間で復旧するための手順書。

## 今回発生した事象
GitHub Actions で以下のエラーが発生して Workflow が失敗した。

```text
❌ Spotify token refresh failed.
status: 400
response: {"error":"invalid_grant","error_description":"Refresh token revoked"}
```

または:

```text
❌ Spotify token refresh failed.
status: 400
response: {"error":"invalid_grant","error_description":"Invalid refresh token"}
```

さらに、playlist 作成後の secret 自動更新ステップで以下が発生。

```text
requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: https://api.github.com/repos/<owner>/<repo>/actions/secrets/public-key
```

## 根本原因
1. `SPOTIFY_REFRESH_TOKEN` が失効/不正（revoked または別値）
2. `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` / `SPOTIFY_REFRESH_TOKEN` が同一 Spotify アプリ由来で揃っていない
3. `GH_PAT` の期限切れ（今回: 2026-03-06 失効）により secret 自動更新 API が 401

## 重要な前提
- GitHub Secrets は既存値を再表示しない（編集画面が空なのは仕様）
- `refresh_token` は発行した `client_id/client_secret` の組と対応している
- `access_token` を貼ると必ず失敗する（貼るべきは `refresh_token`）

## 復旧手順（実施順）
1. Spotify Secrets を揃える
   - `SPOTIFY_CLIENT_ID`
   - `SPOTIFY_CLIENT_SECRET`
   - `SPOTIFY_REFRESH_TOKEN`
   - この3つを同じ Spotify アプリ由来に統一する。

2. refresh token を再取得
   - 実行:
     ```bash
     cd /Users/yamada2/バイブコーディング/imakite_supabase
     python3 scripts/spotify_pkce_token.py
     ```
   - 認証後に `token.json` が作成される。
   - `token.json` 内の `refresh_token` を GitHub Secret `SPOTIFY_REFRESH_TOKEN` に設定する。
   - 注意: `redirect_uri` は `http://127.0.0.1:8888/callback` が Spotify Developer Dashboard 側に登録済みであること。

3. `GH_PAT` を有効化（自動更新維持の場合）
   - classic PAT を再発行し `repo` scope を付与
   - `GH_PAT` secret を更新
   - 期限切れ対策としてカレンダーにリマインダー設定

4. Workflow を再実行
   - `Actions > IHC Weekly Playlist to Spotify > Run workflow`
   - `week_end_date` は通常空欄で可（最新の `weekly_rankings` を利用）

## 成功判定
以下がログに出れば復旧完了。

1. weekly playlist job 側
```text
✅ プレイリスト作成・更新完了: ...
```

2. secret 自動更新側
```text
Updated GitHub secret: SPOTIFY_REFRESH_TOKEN
```

## エラー別の一次切り分け
### A. `invalid_grant` + `Refresh token revoked`
- 失効済みトークン。再発行して `SPOTIFY_REFRESH_TOKEN` を更新。

### B. `invalid_grant` + `Invalid refresh token`
- 値の貼り間違い、または client 情報と不一致。
- `SPOTIFY_CLIENT_ID/SECRET` と token 発行元アプリを一致させる。

### C. `401 Unauthorized`（GitHub API /actions/secrets/public-key）
- `GH_PAT` 無効/期限切れ/権限不足。
- PAT 再発行して `GH_PAT` を更新。

## 運用メモ
- `GH_PAT` は `No expiration` より期限あり（90日〜180日）を推奨。
- 期限通知メールに依存せず、カレンダー運用で管理する。
- playlist 作成成功後に secret 更新だけ失敗するケースがあるため、失敗時は「どのステップで失敗したか」を必ず確認する。
