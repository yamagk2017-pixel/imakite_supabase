# IHC: Sheets → Supabase 移行 基本設計（Draft）

このドキュメントは、IHC（イマキテランキング）を Google スプレッドシート依存から Supabase（Postgres）へ移行するための基本設計メモである。  
実装は Cursor + Codex（IDE拡張）で進める前提。

---

## 0. 背景とゴール

### 背景
- これまで IHC は日次でスプレッドシートへ蓄積していた（シート名 = `YYYY-MM-DD`）
- 今後、IHC 以外（バズッタラ / AIJ など）とも連携するため、IMDB（imd）をマスターとして統合しやすい形に寄せたい

### ゴール
- 日次の素材データ（スナップショット）を Supabase に履歴として蓄積できる
- 日次ランキング（確定結果）を Supabase に保存できる
- 参照UIでは日付を指定すると「その日のランキング全体が見える」状態を維持する
- 内部キー（master key）を統一し、外部サービスIDは集約管理する

---

## 1. キー戦略（最重要）

### 内部の正のキー（master key）
- `imd.groups.id`（uuid）を **唯一の正の識別子** とする

### 外部ID
- Spotify artist id / X / YouTube / 公式サイト等の外部IDは `imd.external_ids` に集約する
- 例：Spotify は `imd.external_ids.service = 'spotify'`、`external_id = spotify_artist_id`

#### 期待される関係
- `imd.external_ids.group_id` は `imd.groups.id` を参照する
- Spotify ID の重複は無い（重複チェックSQLは後述）

---

## 2. 現行のスプレッドシート構造（入力元）

### IHC_Snapshot（毎日1シート）
- シート名：`YYYY-MM-DD`（例外なし）
- 期間：`2025-05-21` 〜 今日まで毎日
- 列：
  - `spotify_id`
  - `name`
  - `artist_popularity`
  - `followers`
  - `track_popularity_sum`
  - `new_release_count`

※ `spotify_id` は必ず `imd.external_ids` に存在するとみなす（設計上の前提）

---

## 3. Supabase 側のDB構造（現状）

### schema
- `ihc` スキーマを使用

### テーブル
#### 3.1 `ihc.artist_snapshots`（素材の履歴：source of truth）
- IHC_Snapshot 相当の履歴を日次で保存する
- 主キーは uuid
- 一意制約：`(snapshot_date, group_id)`

主なカラム：
- `snapshot_date date not null`
- `group_id uuid not null references imd.groups(id)`
- `spotify_id text`（デバッグ/追跡用）
- `name text`
- `artist_popularity int`
- `followers bigint`
- `track_popularity_sum int`（※ numeric ではなく int）
- `new_release_count int`
- `created_at timestamptz default now()`

#### 3.2 `ihc.daily_rankings`（日次ランキング確定保存）
- `ihc.artist_snapshots` から集計して得られた「その日の日次ランキング」を保存
- 動的生成も可能だが、以下の理由で保存する方針：
  - 表示/APIの安定
  - “当時の結果”の再現性（ロジック変更の影響を受けない）
  - ニュース化/参照の確定性
- 一意制約：`(snapshot_date, group_id)`

主なカラム（現状）：
- `snapshot_date date not null`
- `group_id uuid not null references imd.groups(id)`
- `rank int`
- `prev_rank int`
- `score numeric`
- `score_prev numeric`
- `score_delta numeric`
- `rising_icon boolean`
- `artist_popularity int`
- `popularity_delta int`
- `followers bigint`
- `followers_ratio numeric`
- `new_release_count int`
- `track_popularity_sum_ratio numeric`
- `track_popularity_sum int`
- `created_at timestamptz default now()`

### ビュー
#### 3.3 `ihc.snapshot_dates`（日付一覧）
- `ihc.daily_rankings` に存在する日付の一覧
- UIで「日付一覧→日別ランキング表示」に使える

---

## 4. migration（履歴の正史）

- `ihc-db` リポジトリに migrations を保存し、GitHubへ push 済み
- リモートに存在した過去migrationのversion整合のため、placeholder migration を追加している
  - `20251115162955_placeholder.sql`
  - `20251115163230_placeholder.sql`
  - `20251115170232_placeholder.sql`
- IHCスキーマ作成の migration：
  - `20251224150948_create_ihc_schema_and_tables.sql`

---

## 5. バックフィル（過去データ投入）方針

### 目的
- `2025-05-21` 〜 今日までの IHC_Snapshot を `ihc.artist_snapshots` に投入する（再実行可能にする）

### 基本フロー
1. `YYYY-MM-DD` シートを日付として読み取る
2. 各行の `spotify_id` を `imd.external_ids`（service='spotify'）で引き `group_id` を取得
3. `ihc.artist_snapshots` に `(snapshot_date, group_id)` で upsert

#### 注意
- 取り込みは「再実行可能（idempotent）」にする  
  → upsert on conflict で上書き

---

## 6. 日次ランキング生成（daily_rankings への反映）

- まずは `ihc.artist_snapshots` を source とし、日付単位でランキングを生成して upsert
- `prev_rank / score_prev / score_delta` の定義は「初登場は NULL」の扱いを基本とする（見せ方は後で検討）

---

## 7. 未確定事項（要検討）

### 7.1 日次ランキングの確定タイミング（freeze）
- 手動実行が基本で、23時頃だが翌日になることもある
- そのため **実行時に “何月何日の集計か” を明示入力必須にする**（未実装）
- 確定期限（再計算で上書きできる期限）は要検討

### 7.2 実行ログ（run log）テーブル案
確定ルールを後回しにするなら、最低限「実行ログ」を残して追跡可能にする案が有効：
- 例：`ihc.snapshot_runs`
  - run_id, snapshot_date, started_at, finished_at, status, rows_written, algorithm_version, code_version など
- `daily_rankings` と紐付けできる設計が望ましい

---

## 8. 事故防止チェックSQL（参考）

### Spotify external_id の重複チェック（0行が期待）
```sql
select external_id, count(*)
from imd.external_ids
where service = 'spotify'
group by external_id
having count(*) > 1;
