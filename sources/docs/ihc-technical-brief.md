# イマキテランキング 技術ブリーフ

最終更新: 2026-07-02

## この文書の目的

この文書は、イマキテランキングの現在の設計、仕様、処理の流れ、既知の課題、今後の改善候補を、外部AIや編集者に説明するための技術ブリーフです。

Claude Fable 5、ChatGPT、編集者、PR担当者などに渡して、次の整理をしてもらうための前提資料として使うことを想定しています。

- イマキテランキングの価値説明
- プロダクト思想
- PR設計
- 利用シーン整理
- IDOL CROSSING Registryとの関係整理
- 技術的な注意点の共有

内部実装には `IHC` や `ihc` という名前が残っていますが、外部向けの正式名称は「イマキテランキング」です。この文書でも、コード、DBスキーマ、ファイル名、旧スプレッドシート名など実装上必要な箇所を除き、「イマキテランキング」と表記します。

## 1. イマキテランキングとは

イマキテランキングは、IDOL CROSSING Registryに登録されたアーティスト/グループについて、Spotify上の変化を日次で観測し、「いま動きが出ている存在」を見つけるためのランキング生成プロジェクトです。

重要なのは、イマキテランキングが「人気の絶対評価」ではないことです。単にフォロワー数が多い順、Spotifyで有名な順、再生数が多い順に並べる仕組みではありません。

イマキテランキングは、次のような変化を拾う観測装置です。

- Spotify上のアーティスト人気度が前日から上がったか
- フォロワー数が前日から増えたか
- Top Tracks上位曲の反応が前日から変わったか
- 直近リリースがTop Tracksに現れているか
- それらの変化が、前日や前週と比べて目立つか

そのため、イマキテランキングの価値は「一番人気のアーティストを決めること」ではなく、「編集者やファンが気づきにくい上昇の兆しを早く発見すること」にあります。

## 2. 維持すべき基本思想

イマキテランキングは、人気の絶対評価ではなく、動きに気づくための観測装置として維持するべきです。

この思想から、次の仕様は安易に変えない方がよいです。

- SpotifyをマスターDBにしない
- IDOL CROSSING Registryを正のマスターとして扱う
- Spotify IDは外部サービス上の接続IDとして扱う
- 保存時は必ずRegistry側の `group_id` に正規化する
- 日次スナップショットを残し、後から検証できるようにする
- ランキング結果はその日の確定結果として保存する
- スコアは総人気ではなく、変化のシグナルとして説明する
- 「Spotify公式ランキング」のように見える表現を避ける
- 編集者が説明できる指標名と意味を保つ
- 大手/既存人気だけが上位を占めないよう、変化量を見る設計を守る

PRや記事で使う場合は、「Spotify上の公開指標などをもとにした独自集計」「前日比や週次変化を観測するランキング」と説明するのが安全です。

## 3. 呼称と内部実装名

外部向けの正式名称は「イマキテランキング」です。

ただし、リポジトリ内には過去の内部略称として `IHC` / `ihc` が残っています。これは実装上の名前であり、外部向け名称ではありません。

主な対応は次の通りです。

| 外部向け表記 | 内部実装上の表記 | 備考 |
| --- | --- | --- |
| イマキテランキング | `IHC`, `ihc` | ファイル名、DBスキーマ、Workflow名に残る |
| IDOL CROSSING Registry | `imd` schema | Registryの内部DBスキーマ名 |
| グループ/アーティストID | `group_id`, `imd.groups.id` | システム内の正規キー |
| Spotify Artist ID | `spotify_id`, `imd.external_ids.external_id` | 外部サービス上のID |

旧設計メモや一部コメントでは別表現が残る可能性がありますが、今後の説明では「IDOL CROSSING Registry」をマスター名として使うのが望ましいです。

## 4. 全体構造

イマキテランキングは、大きく分けて次の4層で構成されています。

| 層 | 役割 |
| --- | --- |
| IDOL CROSSING Registry | どのグループ/アーティストを対象にするかを決めるマスター |
| Spotify API | popularity、followers、top tracks、artist image、playlistなどの取得元 |
| Supabase | 日次スナップショット、日次ランキング、週次ランキングなどの保存先 |
| Google Sheets / Notebook | 旧運用、過去ロジック、移行前の参照資料 |

現在の正系はSupabaseです。Google Sheetsは旧運用の名残と参照資料として残っていますが、今後のプロダクト説明ではSupabase中心の構成として説明するのが実態に近いです。

## 5. IDOL CROSSING Registryをマスターとして参照する方法

イマキテランキングでは、Spotify Artist IDを正のキーとして扱いません。正の識別子は、IDOL CROSSING Registry側の `imd.groups.id` です。

基本ルールは次の通りです。

- Registryの正規テーブルは `imd.groups`
- 正のグループ/アーティストIDは `imd.groups.id`
- 表示名は主に `imd.groups.name_ja`
- Spotify Artist IDなどの外部IDは `imd.external_ids` に集約
- Spotifyの場合は `imd.external_ids.service = 'spotify'`
- `imd.external_ids.external_id` がSpotify Artist ID
- イマキテランキング側の保存テーブルでは、最終的に必ず `group_id` に正規化する

これにより、Spotify上のIDが変わったり、別サービスのIDを追加したりしても、イマキテランキング側の履歴はRegistryの `group_id` を軸に維持できます。

### Registryをマスターにする理由

Spotifyは重要なデータ取得元ですが、マスターではありません。

理由は次の通りです。

- Spotifyには同名、別名義、統合、分裂、表記揺れがある
- Spotify Artist IDは外部サービスの都合で変わる可能性がある
- イマキテランキング以外の企画やサービスとも共通IDで連携したい
- X、YouTube、公式サイトなど他サービスIDも同じRegistryに集約したい
- 編集上の正式名称や表示名はRegistry側で管理したい

## 6. 主なデータ取得元

### IDOL CROSSING Registry

対象アーティスト/グループと外部IDの対応を管理します。

主に参照する情報は次の通りです。

- `imd.groups.id`
- `imd.groups.name_ja`
- `imd.external_ids.group_id`
- `imd.external_ids.service`
- `imd.external_ids.external_id`

スナップショット取得時には、`imd.external_ids` から `service = 'spotify'` の行を取得し、Spotify Artist ID一覧を作ります。

### Spotify API

Spotifyは、観測指標とプレイリスト生成の外部サービスです。

主に取得・利用する情報は次の通りです。

- Artist情報
- Artist popularity
- Followers count
- Top Tracks
- Top Tracks内のtrack popularity
- Album / singleのリリース情報
- Artist画像URL
- 最新曲または代表曲のSpotify埋め込みURL
- 週次Top20プレイリストの作成・更新

### Supabase

Supabaseは、現在の正系データ保存先です。

主な用途は次の通りです。

- 日次スナップショット保存
- 日次ランキング保存
- 日次Top20保存
- 日次統計保存
- 累積ランキング保存
- 週次ランキング保存
- 週次プレイリストURL保存
- 表示用ビューの提供

### Google Sheets

Google Sheetsは旧運用の出力先です。

Notebook内には次のようなスプレッドシート名が残っています。

- `IHC_Snapshot`
- `IHC_Ranking`
- `IHC_Daily_Stats`
- `IHC_Cumulative_Ranking`
- `IHC_Top20_Daily`
- `IHC_Weekly_Ranking`

現在の説明では、これらは「旧Sheets運用の履歴」「移行前ロジックの参照資料」「バックフィルや比較に使う可能性がある資料」と位置づけるのが安全です。

## 7. Spotify APIの使い方

Spotify APIは、読み取り系とプレイリスト更新系で認証方式が異なります。

### 読み取り系

日次スナップショットや日次Top20素材の取得では、Client Credentials方式を使います。

利用する主な環境変数は次の通りです。

- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`

主なAPI用途は次の通りです。

| 用途 | API |
| --- | --- |
| Artist情報取得 | `/v1/artists/{artist_id}` |
| Top Tracks取得 | `/v1/artists/{artist_id}/top-tracks?market=JP` |
| 最新アルバム/シングル候補取得 | `/v1/artists/{artist_id}/albums?include_groups=album,single&market=JP` |
| アルバム内トラック取得 | `/v1/albums/{album_id}/tracks` |

エラー処理として、429 rate limitでは `Retry-After` を待ち、500系では指数バックオフで再試行します。401の場合はトークン再取得を試みます。

### プレイリスト更新系

週次Top20プレイリストの作成・更新では、ユーザー認可済みのrefresh tokenを使います。

利用する主な環境変数は次の通りです。

- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`
- `SPOTIFY_REFRESH_TOKEN`
- `SPOTIFY_USER_ID`

プレイリスト名は `イマキテ週間ランキングTOP20` をベースにし、週の範囲ラベルを付けます。既存プレイリストがあれば更新し、なければ非公開プレイリストとして作成します。

## 8. データ取得からランキング生成までの流れ

全体の流れは次の通りです。

```text
IDOL CROSSING Registry
  ↓ service = 'spotify' の外部IDを取得
Spotify API
  ↓ popularity / followers / top tracks / image を取得
ihc.artist_snapshots
  ↓ 前日スナップショットと比較
ihc.daily_rankings
  ↓ 日次統計、Top20、累積ランキングを派生
ihc.daily_stats / ihc.daily_top20 / ihc.cumulative_rankings
  ↓ 直近7日分のdaily_rankingsを合算
ihc.weekly_rankings
  ↓ 週次Top20をSpotifyプレイリストへ反映
Spotify Weekly Playlist / ihc.weekly_playlists
```

内部DBスキーマ名として `ihc` が使われていますが、これはイマキテランキングの内部実装領域です。

## 9. 日次スナップショット処理

日次スナップショットは、対象アーティスト/グループのSpotify上の状態を日付ごとに保存する処理です。

実装ファイルは `scripts/ihc_snapshot_job.py` です。

処理手順は次の通りです。

1. `SNAPSHOT_DATE` が指定されていれば、その日付を集計対象日にする
2. `SNAPSHOT_DATE` がなければ、JSTの業務日ルールで対象日を決める
3. `imd.external_ids` から `service = 'spotify'` のSpotify Artist ID一覧を取得する
4. Spotify IDの重複があれば警告し、重複を落とす
5. Spotify APIからArtist情報とTop Tracksを取得する
6. `name`, `artist_popularity`, `followers`, `track_popularity_sum`, `new_release_count`, `artist_image_url` を作る
7. 取得失敗らしき行は最大3ラウンド再取得する
8. `spotify_id` をRegistryの `group_id` に変換する
9. `ihc.artist_snapshots` に `(snapshot_date, group_id)` でupsertする
10. Spotify画像URLをRegistry側の `imd.groups` にも反映する

デフォルトのWorkflowでは、日次スナップショットは22:30 JSTに実行されます。

## 10. 日次ランキング処理

日次ランキングは、当日のスナップショットと前日のスナップショットを比較して、変化量をスコア化する処理です。

実装ファイルは `scripts/ihc_ranking_job.py` です。

処理手順は次の通りです。

1. `SNAPSHOT_DATE` が指定されていれば、その日付をランキング対象日にする
2. 指定がなければ、`ihc.artist_snapshots` に存在する最新日を対象にする
3. 対象日の前日を比較対象日にする
4. 前日のスナップショットがない場合、通常は処理を止める
5. Registryから `group_id` に対応する `name_ja` を取得し、表示名を補う
6. 前日比で `popularity_delta`, `followers_ratio`, `track_popularity_sum_ratio` を計算する
7. 新規出現アーティストは差分系指標を0扱いにする
8. 独自スコアを計算する
9. Spotify popularityが0、1、2、または欠損の場合はスコアを0にする
10. スコア順に順位を付ける
11. DB保存時はスコアを10倍したポイント表記にする
12. 前日ランキングと比較し、前回順位、前回スコア、スコア差分、急上昇フラグを付与する
13. `ihc.daily_rankings` にupsertする
14. `ihc.daily_stats`, `ihc.cumulative_rankings`, `ihc.daily_top20` を派生生成する

日次ランキングWorkflowは、日次スナップショットWorkflowが成功したあとに自動実行されます。手動実行も可能です。

## 11. スコア計算

日次ランキングの基本スコアは次の式です。

```text
score =
  popularity_delta * 3.0
+ followers_ratio * 2.0
+ track_popularity_sum_ratio * 1.0
+ new_release_count * 0.3
```

DBに保存される `ihc.daily_rankings.score` は、この値を10倍したポイント表記です。

たとえば、内部計算上の `score = 12.3` は、DBでは `123.0` として保存されます。

### 指標の意味

| 指標 | 意味 | 注意点 |
| --- | --- | --- |
| `artist_popularity` | Spotifyが返すアーティスト人気度 | 月間リスナー数や再生数ではない |
| `popularity_delta` | 前日からのpopularity差分 | popularity自体がSpotify独自の相対指標 |
| `followers` | Spotify上のフォロワー数 | 絶対値はランキングスコアに直接使わない |
| `followers_ratio` | 前日比のフォロワー増加率 | フォロワー数そのものではない |
| `track_popularity_sum` | JPマーケットTop Tracks上位5曲のpopularity合計 | 全楽曲の総再生数ではない |
| `track_popularity_sum_ratio` | Top Tracks合計値の前日比 | Top Tracksの入れ替わりに影響される |
| `new_release_count` | Top Tracks内で直近7日以内リリースと判定された曲数 | 全新譜数ではない |
| `score` | イマキテランキング独自の合成スコア | Spotify公式スコアではない |
| `rising_icon` | 前日比でスコアが大きく伸びたフラグ | 実装上はboolean。閾値は50ポイント超 |

### 誤解されやすい点

イマキテランキングの上位は、「Spotifyで最も人気がある順」ではありません。

誤解を避けるため、次の説明は避けるべきです。

- Spotify公式ランキング
- 人気アーティストランキング
- 再生数ランキング
- フォロワー数ランキング
- 月間リスナーランキング

より正確な説明は次のようなものです。

- Spotify上の変化をもとにした独自ランキング
- 前日比の伸びを観測するランキング
- いま動きが出ているアーティスト/グループを見つけるランキング
- 編集者が注目候補を発見するための観測装置

## 12. 日次派生データ

日次ランキング処理では、ランキング本体以外にも複数の派生データを作ります。

### `ihc.daily_stats`

日次データの健全性や分布を見るための統計です。

保存される主な情報は次の通りです。

- 平均スコア
- popularityが0の件数
- track popularity ratioが0の件数
- popularityとtrack popularity ratioの両方が0の件数
- 前日からの差分
- 前日比率

これは編集用ランキングというより、データ品質監視に近いテーブルです。

### `ihc.cumulative_rankings`

前日の累積スコアに当日のスコアを足して、累積ランキングを作ります。

注意点として、これはSpotify上の累積再生数や累積人気ではありません。イマキテランキング独自スコアの累積です。

### `ihc.daily_top20`

日次ランキングのTop20に対して、Web表示や編集利用に便利な素材を付与します。

保存される主な情報は次の通りです。

- rank
- artist_name
- score
- latest_track_name
- latest_track_embed_link
- artist_image_url

最新曲が取れない場合は、Top Tracksの代表曲をfallbackとして使います。

## 13. 週次ランキング処理

週次ランキングは、直近7日分の日次ランキングスコアを合算して作ります。

実装ファイルは `scripts/ihc_weekly_ranking_job.py` です。

処理手順は次の通りです。

1. `WEEK_END_DATE` が指定されていれば、その日を週の最終日にする
2. 指定がなければ、JSTの昨日を週の最終日にする
3. `week_end_date - 6 days` から `week_end_date` までの7日間を対象にする
4. `ihc.daily_rankings.score` を取得する
5. `group_id` ごとに7日分のスコアを合算する
6. 週内の最新日の `artist_popularity` を付ける
7. Registryから `name_ja` を取得し、表示名を付ける
8. 前週の順位を参照して `prev_rank` を付ける
9. `ihc.weekly_rankings` にupsertする

デフォルトのWorkflowでは、週次ランキングは火曜00:00 JSTに実行されます。

注意点として、現行実装は「7日分が完全に揃っているか」を厳密には検証していません。指定期間内に存在する `daily_rankings` を集計するため、欠損日があると週次スコアが低く出る可能性があります。

## 14. 週次プレイリスト処理

週次プレイリスト処理は、週次ランキングTop20をもとにSpotifyプレイリストを作成・更新する処理です。

実装ファイルは `scripts/ihc_weekly_playlist_job.py` です。

処理手順は次の通りです。

1. `WEEK_END_DATE` が指定されていれば、その週を対象にする
2. 指定がなければ、最新の `ihc.weekly_rankings.week_end_date` を対象にする
3. 対象週のTop20 `group_id` を取得する
4. Registryの `imd.external_ids` からSpotify Artist IDに変換する
5. 各アーティストの最新曲URIを取得する
6. 最新曲が取れなければTop TrackのURIを使う
7. 既存プレイリストがあれば中身と名前を更新する
8. なければ非公開プレイリストを作成する
9. `ihc.weekly_playlists` にプレイリストURLと埋め込みURLを保存する
10. Spotifyから新しいrefresh tokenが返った場合は、GitHub Secret更新用ファイルに書き出す

プレイリストのベース名は `イマキテ週間ランキングTOP20` です。

## 15. 欠損チェックとバックフィル

日次の欠損チェックは `scripts/ihc_daily_integrity_check.py` で行います。

デフォルトのWorkflowでは、06:00 JSTに前日分を確認します。

確認対象は次の2つです。

- `ihc.artist_snapshots`
- `ihc.daily_rankings`

前日分のスナップショットがなければ、スナップショットWorkflowをdispatchし、完了後にランキングWorkflowをdispatchします。スナップショットはあるがランキングがなければ、ランキングWorkflowだけをdispatchします。

この処理は便利ですが、GitHub Actions、Supabase、GitHub Token、対象ブランチに依存します。実行ログテーブルがないため、最終的な処理結果をDB内だけで完全に追跡することはまだ難しいです。

## 16. スプレッドシート、JSON、画像、Web表示の関係

### スプレッドシート

旧運用では、日次スナップショット、日次ランキング、累積ランキング、週次ランキング、Top20などをGoogle Sheetsに出力していました。

現在の正系はSupabaseです。Notebook内のGoogle Sheets処理は、過去ロジックの参照や移行前データの確認用途と考えるのが安全です。

### JSON

このリポジトリには、公開用JSONを生成する正式なパイプラインは見当たりません。

JSONとして扱われている主なものは次の通りです。

- Spotify APIレスポンス
- Supabase APIレスポンス
- Spotify認可用の `token.json`

将来的にWeb/API公開する場合は、`daily_top20` や `weekly_rankings` から公開用JSONを生成する契約を別途定義する必要があります。

### 画像

現行実装には、画像生成処理はありません。

扱っているのは、Spotify APIから取得したArtist画像URLです。日次スナップショット時にはRegistry側の `imd.groups` に画像URLを反映し、日次Top20では `artist_image_url` として保存します。

つまり、現時点の画像は「生成画像」ではなく「外部サービス由来の画像URL」です。

### Web表示

このリポジトリには、本格的なWebフロントエンドはありません。

ただし、Web表示に使える素材は保存されています。

- `ihc.daily_top20.latest_track_embed_link`
- `ihc.daily_top20.artist_image_url`
- `ihc.weekly_playlists.spotify_embed_url`
- `ihc.v_artist_snapshots_display`

Webサイトや記事パーツを作る場合は、これらを表示素材として使う設計になります。

## 17. 主なDBテーブルと役割

| テーブル/ビュー | 役割 |
| --- | --- |
| `imd.groups` | IDOL CROSSING Registryのグループ/アーティストマスター |
| `imd.external_ids` | Spotifyなど外部サービスIDとの対応表 |
| `ihc.artist_snapshots` | 日次素材データの履歴。ランキング生成のsource of truth |
| `ihc.daily_rankings` | 日次ランキングの確定保存先 |
| `ihc.daily_stats` | 日次データ品質や分布の統計 |
| `ihc.daily_top20` | 日次Top20と表示用素材 |
| `ihc.cumulative_rankings` | イマキテランキング独自スコアの累積ランキング |
| `ihc.weekly_rankings` | 直近7日分のスコア合算による週次ランキング |
| `ihc.weekly_playlists` | 週次SpotifyプレイリストURLの保存先 |
| `ihc.v_artist_snapshots_display` | Registry表示名を結合したスナップショット表示用ビュー |

注意点として、`ihc.weekly_playlists` はスクリプトで使われていますが、このリポジトリ内のmigrationには作成SQLが見当たりません。また、`ihc.artist_snapshots` や `ihc.daily_rankings` など中核テーブルの作成migrationも、現リポジトリ内だけを見ると不足しているように見えます。

## 18. 主要ファイルと役割

| ファイル | 役割 |
| --- | --- |
| `scripts/ihc_snapshot_job.py` | Spotifyから日次スナップショットを取得し、`ihc.artist_snapshots` に保存する |
| `scripts/ihc_ranking_job.py` | 前日比から日次ランキング、統計、累積、Top20を作る |
| `scripts/ihc_weekly_ranking_job.py` | 直近7日分の日次ランキングを合算し、週次ランキングを作る |
| `scripts/ihc_weekly_playlist_job.py` | 週次Top20からSpotifyプレイリストを作成・更新する |
| `scripts/ihc_daily_integrity_check.py` | 日次データ欠損を確認し、必要に応じてWorkflowを再実行する |
| `scripts/spotify_pkce_token.py` | Spotifyプレイリスト操作用のrefresh tokenを取得する補助スクリプト |
| `.github/workflows/ihc_snapshot.yml` | 日次スナップショットWorkflow |
| `.github/workflows/ihc_ranking.yml` | 日次ランキングWorkflow |
| `.github/workflows/ihc_weekly_ranking.yml` | 週次ランキングWorkflow |
| `.github/workflows/ihc_weekly_playlist.yml` | 週次プレイリストWorkflow |
| `.github/workflows/ihc_daily_integrity_check.yml` | 欠損チェックWorkflow |
| `supabase/migrations/*.sql` | 表示ビュー、日次統計、Top20、累積、週次ランキングなどのDB定義 |
| `sources/*.ipynb` | 旧Notebook処理、Sheets運用、移行前ロジックの参照資料 |
| `sources/docs/ihc-supabase-migration.md` | SheetsからSupabaseへ移行する設計メモ |
| `sources/docs/ihc-weekly-playlist-troubleshooting.md` | 週次プレイリスト認証障害の復旧メモ |

`token.json` はSpotify認可時に作られるローカルファイルです。`.gitignore` に含まれており、Git管理しない前提です。

## 19. 自動実行スケジュール

現行Workflow上の主なスケジュールは次の通りです。

| 処理 | Workflow | 実行タイミング |
| --- | --- | --- |
| 日次スナップショット | `IHC Snapshot to Supabase` | 毎日22:30 JST |
| 日次ランキング | `IHC Ranking to Supabase` | スナップショット成功後 |
| 週次ランキング | `IHC Weekly Ranking to Supabase` | 火曜00:00 JST |
| 週次プレイリスト | `IHC Weekly Playlist to Spotify` | 週次ランキング成功後 |
| 日次欠損チェック | `IHC Daily Integrity Check` | 毎日06:00 JST |

手動実行時には `snapshot_date` や `week_end_date` を指定できます。

## 20. 現在できていること

現在の実装でできていることは次の通りです。

- RegistryからSpotify Artist ID一覧を取得できる
- Spotify IDをRegistryの `group_id` に正規化できる
- Spotify APIから日次観測値を取得できる
- 日次スナップショットをSupabaseへ保存できる
- 前日比から日次ランキングを生成できる
- 日次スコアを10倍ポイント表記で保存できる
- 急上昇フラグを付けられる
- 日次統計を保存できる
- 日次Top20に楽曲名、埋め込みURL、画像URLを付けられる
- 累積ランキングを作れる
- 週次ランキングを作れる
- 週次Top20のSpotifyプレイリストを自動更新できる
- 日次欠損時にWorkflow経由でバックフィルできる
- Google Sheets時代のNotebookを参照資料として残している

## 21. まだ不安定なこと

現在まだ不安定、または仕様未確定な部分は次の通りです。

- 日次ランキングの確定タイミングとfreezeルールが未整理
- いつまで再計算で上書きしてよいかが未定義
- スコア計算式に `algorithm_version` がない
- 実行ログテーブルがなく、いつ、どのコードで、何件処理したかをDB内で追跡しにくい
- 中核テーブルのmigrationがこのリポジトリ内に揃っていない可能性がある
- `weekly_playlists` のmigrationが見当たらない
- Spotify APIの一時失敗、rate limit、認証失効の影響を受ける
- 週次ランキングは7日分の完全性チェックが弱い
- Google Sheets時代のNotebookと現行Supabaseジョブが混在しており、正系が誤解されやすい
- refresh tokenやGitHub PATの更新運用が壊れやすい
- バックフィル時の `new_release_count` は、過去日ではなく実行日時基準になり得る

## 22. データ品質上の課題

データ品質で注意すべき点は次の通りです。

- Spotify IDがRegistryにない場合、対象から外れる
- Spotify IDの重複がある場合、現行実装は警告して重複を落とす
- Spotify側のアーティスト統合、分裂、別名義、ID変更を検知しにくい
- Spotify popularityが0、1、2、欠損の場合、現行実装ではスコアを0にする
- popularityが低いケースとAPI取得失敗の区別が弱い
- Top TracksはSpotify側の選定に依存する
- `track_popularity_sum` は上位5曲の合計であり、全曲の反応ではない
- `new_release_count` はTop Tracks内のリリース日から数えるため、全新譜を網羅しない
- フォロワー増加率は母数が小さいアーティストで大きく出やすい
- Spotify APIレスポンスの生データを丸ごと保存していないため、後から原因分析しにくい
- 日付の扱いがJST、Workflow時刻、手動入力で混ざる可能性がある

## 23. 指標の解釈で守るべきこと

編集者や外部AIに説明するときは、指標を次のように扱うべきです。

- `score` は「人気点」ではなく「変化シグナルの合成値」
- `rank` は「その日の伸びの順位」
- `weekly_rankings.total_score` は「7日分の変化シグナル合算」
- `cumulative_score` は「イマキテランキング独自スコアの累積」
- `artist_popularity` はSpotifyが返す相対的な人気度
- `followers_ratio` はフォロワー増加率であり、フォロワー総数ではない
- `new_release_count` はリリース活動の完全な記録ではない

イマキテランキングの指標は、編集判断の入口として使うべきです。記事化やPRに使う場合は、必ず個別の活動、リリース、SNS、ライブ情報などと合わせて確認するのが望ましいです。

## 24. 触ると壊れやすい部分

実装変更時に特に壊れやすい部分は次の通りです。

- Registryの正規キーを `group_id` にする前提
- `imd.external_ids.service = 'spotify'` の対応
- `snapshot_date, group_id` のupsert前提
- 日次ランキングの前日比較ロジック
- 新規出現アーティストの差分を0にする扱い
- popularityが0、1、2、欠損の場合にスコア0にする扱い
- スコア計算式と10倍ポイント保存ルール
- `rising_icon` の50ポイント超閾値
- JST基準の日付処理
- Workflow間の連鎖
- `RESET_DAILY_TOP20` によるTop20削除と再保存
- Spotify refresh tokenとGitHub Secret更新
- Notebook内の日本語カラム名と現行DBの英語カラム名の差
- `weekly_playlists` のDB定義がリポジトリ内に見当たらない点

これらを変更する場合は、過去データの再現性、Web表示、プレイリスト更新、編集説明への影響を先に確認する必要があります。

## 25. 今後の改善候補

今後の改善候補は次の通りです。

- `ihc.snapshot_runs` や `ihc.ranking_runs` のような実行ログテーブルを追加する
- `algorithm_version` と `code_version` をランキング保存時に持たせる
- 中核テーブルを含む全migrationをこのリポジトリに揃える
- `weekly_playlists` のmigrationを追加する
- 週次ランキング生成時に7日分の完全性チェックを入れる
- データ欠損、Spotify ID重複、Registry未対応を可視化する
- スコア0の理由を「低popularity」「API失敗」「対象外」などに分ける
- Spotify APIレスポンスの一部または監査ログを保存する
- 新譜判定をTop Tracks依存からAlbums API中心に改善する
- バックフィル時の `new_release_count` を対象日基準で計算する
- 公開Web/API用のJSONスキーマを定義する
- Supabaseを読む正式なWebフロントエンドを作る
- 日次Top20や週次Top20からSNS/記事用カード画像を生成する処理を別設計で追加する
- スコア計算のテストデータと回帰テストを作る
- refresh token更新とGitHub PAT更新の運用を簡素化する

## 26. 技術的な課題

技術的には、次の課題が残っています。

- DB schemaとmigrationの正史が一部不足している可能性がある
- GitHub Actions、Supabase、Spotify API、Registryの依存関係が強い
- 実行ログが不足しており、障害発生時の追跡性が弱い
- Spotify APIの一時的な欠損と本当の低スコアを分離しにくい
- Workflowの成功とDBの期待状態が完全には結びついていない
- Notebook時代の処理と現行Pythonジョブの境界が曖昧に見える
- スコア計算式を変更した場合の過去ランキング再計算方針が未定義
- Web/APIとして外部公開するためのレスポンス契約が未定義
- 認証情報とtoken更新まわりが運用依存になっている

## 27. 外部AIや編集者に渡すときの注意

この文書を外部AIや編集者に渡す場合は、次の前提を明示すると誤解が減ります。

- イマキテランキングは「Spotify公式ランキング」ではない
- イマキテランキングは「人気の絶対評価」ではない
- 主目的は「伸びの兆しを見つけること」
- IDOL CROSSING Registryがマスターで、Spotifyは観測データ取得元
- 指標は編集判断の入口であり、記事化には追加確認が必要
- 旧Google SheetsやNotebookは現行の正系ではなく、移行前の参照資料
- 現行の正系データはSupabaseに保存されている
- 画像生成や公開JSON生成は現時点では主機能ではない

外部向けの説明では、次のような言い方が最も安全です。

> イマキテランキングは、IDOL CROSSING Registryに登録されたアーティスト/グループを対象に、Spotify上の公開指標の変化を日次で観測し、前日比や週次の伸びを独自スコアとして可視化するランキングです。人気の絶対順位ではなく、いま動きが出ている候補に気づくための観測装置です。

この表現なら、技術仕様、編集利用、PR利用のどれにも展開しやすいです。
