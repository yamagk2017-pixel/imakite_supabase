# イマキテアイドル：ジャパン 運用手順

2026-11-11。ユーザー承認済みのIHC中心・独立加点アルゴリズムを、固定Spotifyプレイリストとして運用する。

## 起動結果（2026-11-11）

**[イマキテアイドル：ジャパン](https://open.spotify.com/playlist/6ACBBRFex0KOvF1V82ppvH)**を公開した。初期50曲・順序は承認済み11/10シミュレーションと完全一致。カバーは先頭の可憐なアイボリーのアーティスト画像。固定IDは`6ACBBRFex0KOvF1V82ppvH`、初回の掲載履歴50件を保存済み。

[本番ドライラン成功](https://github.com/yamagk2017-pixel/imakite_supabase/actions/runs/43003443833)後、[初回公開の全ステップが成功](https://github.com/yamagk2017-pixel/imakite_supabase/actions/runs/43003604216)。`IHC_NEW_RELEASE_PLAYLIST_ENABLED=true`を設定し、次の定期締切は**2026-11-14（土）00:00 JST**。旧週間処理の自動起動は停止済み。

## 運用設定

- プレイリスト名：**イマキテアイドル：ジャパン**。同じSpotify Playlist IDを継続使用し、公開する。
- 初期曲：承認済みシミュレーションの**11/10基準・初回50曲**。先頭は可憐なアイボリー「リナリアの花」。`config/imakite-idol-japan.bootstrap-2026-11-10.json`に曲順と点数を固定し、SHA-256で整合性を検証する。新規取得で選び直さない。
- 更新：**毎週土曜00:00 JST**。GitHub Actions cronは`0 15 * * 5`（金曜15:00 UTC）。対象期間は前の土曜〜金曜の7日間。初回定期更新の締切は2026-11-14。
- GitHub Actionsの開始時刻には遅延があり得る。締切は実行時刻によらず土曜00:00 JSTに固定し、処理終了後にSpotifyへ反映する。
- 1更新で新規追加は最大25曲、1グループ1曲。新しい曲を先頭へ置き、既存曲の順を保ち、末尾を押し出して50曲を維持する。追加が0なら旧50曲を維持する。
- 最新作品の先頭曲を選び、イントロ・SE・再配信・再収録も許容する。同じSpotify Track IDは企画内で一度しか追加しない。先頭曲が既掲載なら別の曲へ繰り下げない。
- 候補はpopularity 10以上、またはニュース／YouTubeの実績あり。ニュースはSanityの関連UUID付き公開ニュース・イベント・ラジオ告知を全期間照合し、WP由来は除外。YouTubeは直近7日の公開済み・180秒超。終了済み公開アーカイブを含む。
- 点数は`max(0, IHCの7日合計) + ニュース10点 + 動画3点×最大2本`。ニュース本数は加点に比例させない。
- カバー：**更新後の先頭曲のグループのSpotifyアーティスト画像**。初回は可憐なアイボリー。画像を改変せず、Spotifyが返すJPEGの中でアップロード要件（Base64で256KB以下）に合うサイズを使用する。

## 旧週間ランキングの休止

`ihc_weekly_ranking.yml`から火曜のscheduleを外し、`ihc_weekly_playlist.yml`から週間集計完了後の自動起動を外した。両Workflowの手動実行とPythonソース、保存済みランキング・旧プレイリストは保持する。

日次Snapshot・日次Ranking・日次Integrity Checkは継続する。新機能は日次の`base_score`（なければ旧`score`）を7日分合計して内部で選曲点を計算し、`ihc.weekly_rankings`へ新しい週間順位を書き込まない。日次v2のmomentumを7日合算しない。7日すべての日付がなければ公開を中止し、個別グループの7日分が不完全ならそのグループは保留して根拠を残す。

## 保存・復旧

内部のグループキーは`imd.groups.id`。外部IDは`imd.external_ids`から解決する。

- `ihc.release_playlist_projects`：固定Playlist ID、現在の50曲、revision、直前の締切、排他リース。
- `ihc.release_playlist_runs`：締切ごとに一つの更新。更新前・更新後・追加曲・全候補と除外理由・カバー情報を固定する。`prepared`→`committed`。
- `ihc.release_playlist_history`：過去に追加した全Track IDとグループUUID。押し出された後も残す。

RLSを有効にし、上記テーブルと更新RPCは`service_role`だけが使える。一般利用者やブラウザから書き込ませない。更新結果の確定と履歴保存は一つのDBトランザクション。

外部更新前に選曲を固定し、Spotify側で50曲・順序・所有者・名前・公開状態を照合してから履歴を確定する。カバーやDB保存で失敗しても、同じ`prepared`の曲順を再開し、二重追加しない。初回の作成応答が失われた場合は、所有者・名前・企画説明が一致する専用プレイリストを探して再接続する。複数一致なら停止する。

Spotify側に手動変更があり、更新前・更新後のどちらの曲順とも一致しない場合は停止して既存変更を保護する。通常運用中は専用プレイリストの曲順を手動変更しない。停止した際は状況を照合し、履歴を勝手に消して再初期化しない。

## GitHub Actions設定

既存Secrets：`SUPABASE_URL`、`SUPABASE_SERVICE_ROLE_KEY`、`SPOTIFY_CLIENT_ID`、`SPOTIFY_USER_ID`、`GH_PAT`。

新規Secrets：`IHC_PLAYLIST_REFRESH_TOKEN`、`YOUTUBE_API_KEY`。新企画のSpotifyトークンは旧`SPOTIFY_REFRESH_TOKEN`と分ける。必要なscopeは`playlist-read-private playlist-modify-private playlist-modify-public ugc-image-upload`。

Variables：`SANITY_PROJECT_ID`、`SANITY_DATASET`。Sanityは公開記事メタデータだけを読むので書込トークンを渡さない。

定期実行は`IHC_NEW_RELEASE_PLAYLIST_ENABLED=true`で有効になる。初期設定中は無効のままにし、初回公開と検証の完了後に有効化する。手動起動はこの変数によらず使える。

トークンがrefresh時に更新されたら、失敗時も`always()`ステップで新トークンを専用Secretへ保存する。`GH_PAT`はActions Secretsの更新権限が必要。トークンファイル・APIキーはログやartifactへ入れない。

## 初回起動

1. migration `20261111095843_ihc_new_release_playlist.sql`を適用し、DBトランザクション検証を実施する。
2. Spotifyの画像アップロード権限を含む認証を完了し、専用のrefresh tokenをSecretへ保存する。
3. Workflow **IHC New Release Playlist**を`mode=bootstrap`、`apply=false`で試行する。
4. 同じWorkflowを`mode=bootstrap`、`apply=true`で起動し、50曲と曲順・カバー・公開状態を検証する。
5. 固定Playlist ID、実行URL、検証結果を記録し、`IHC_NEW_RELEASE_PLAYLIST_ENABLED=true`にする。次回以降はscheduleが`weekly`・`apply=true`で実行する。

`mode=bootstrap`を再実行しても、すでに初期化済みなら新しいプレイリストや50曲を作り直さない。

## 手動更新・障害対応

Workflow Dispatchで`mode=weekly`、`cutoff=YYYY-MM-DD`（土曜日）を指定する。空欄なら最新の土曜締切。まず`apply=false`で確認し、反映するなら`apply=true`。

未完了の更新があれば先に同じ曲順を再開する。未処理の土曜締切は古い順に実行し、一度の起動では最大8週まで追いつく。過去の回復は現在取得できる公開情報を使うため、厳密な当時再現ではない。

失敗の確認先はGitHub Actionsの実行結果・Step Summary・artifact（90日保持）と`release_playlist_runs`。標準のActions失敗通知を受け取れるよう、リポジトリの通知設定を確認する。新たなSlack・メール自動送信は追加していない。

日次データの欠落なら既存の日次復旧を先に実行する。Spotify 401/403なら必要scopeと所有者、認証状態を確認する。429時はAPI側のRetry-Afterを尊重し、再実行で同じ更新を回復する。30分の排他リースが残っている場合は期限まで待ち、他の実行がないことを確認してから再実行する。

## 残る運用改善

旧週間集計の休止に伴う依存先の見直しが必要。`musicite_ai`の`src/app/imakite/ranking-list.tsx`は旧`weekly_playlists`の埋め込みを参照し、`src/lib/iam/weekly-targets.ts`の`fetchIhcTop20GroupIds`は最新の`weekly_rankings`上位20組を使っている。現在の実装のままでは旧週間値を参照し続ける。サイトの新プレイリストへの導線・埋め込みと、IAMのIHC由来対象の扱いは別途切り替える。旧アーカイブを消す必要はない。

YouTubeは未登録588グループ・URL未解決3グループがあり、これらの活動実績は評価できていない。登録情報を整備すれば候補の取りこぼしを減らせる。共有チャンネルが登録された場合は帰属の判断が必要で、現状は重複加点しない。

ニュースを全期間で評価するため、掲載実績が一度付くと継続的な加点・ワイルドカードになる。これは現行の承認済み設定であり、運用結果を見て調整する。Spotify指標の利用条件について以前行った照会の回答状況は、技術的な取得・公開成功とは別に確認を継続する。

## 参照

- [Spotify：カバー画像の要件とscope](https://developer.spotify.com/documentation/web-api/reference/upload-custom-playlist-cover)
- [Spotify：プレイリスト全曲の置換](https://developer.spotify.com/documentation/web-api/reference/reorder-or-replace-playlists-items)
- [Supabase：Database Functions](https://supabase.com/docs/guides/database/functions)

## 検証

既存テストを含む27件が合格。DBテストは`tests/ihc_release_database_transaction.sql`をトランザクション内で実行し、初期50曲・再掲載拒否・グループの旧曲継続・排他・未完了再開・確定の整合性を確認して全変更をrollbackした。本番用選曲エンジンは保存済みの4週シミュレーションと、全候補の曲ID・点数・追加曲順・最終曲順が一致した。
