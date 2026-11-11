# イマキテランキング v2 本番反映手順

## 目的

日次ランキングへconversion momentumを導入しつつ、従来のv1スコアを保持する。本番反映後も、週次ランキングの公開値はv1を維持し、v2候補値を並行保存する。

この手順書の対象migrationは次のファイルである。

```text
supabase/migrations/20261111050025_add_ihc_ranking_algorithm_versions.sql
```

## 反映前の状態（作業開始時）

- ローカルにはv2実装とmigrationがある
- 本番DBへのmigration適用は未実施
- GitHubへのコード反映も未実施
- 日次Workflowの既定値はv2
- 週次Workflowの既定値はv1
- 累積ランキングは従来どおりv1の`base_score`を使用する

本番反映では、**DB migrationを先に適用し、その後コードをGitHubへpushする**。順序を逆にすると、日次v2が新しいカラムへ書き込めず失敗する可能性がある。

## 2026-11-11のDB反映記録

- 本番プロジェクト: `supabase-buzzttara`（`oslpycadjyhnldloxvyl`）の`ihc`スキーマ
- Supabase MCPの`apply_migration`で本ファイルのSQLを適用済み
- 本番で採番されたmigration versionは`20261111050025`。ローカルのファイル名も同じversionに揃えた
- 日次4カラム・週次6カラムの追加と、既存データのv1初期化を確認済み
- 日次・週次ともに、新カラムの欠損や従来スコアとの不一致は0件
- 既存のランキング公開スコア・順位は再計算していない。日次v2はコード反映後の自動実行から開始する

本番DBは他システムと共有しており、このリポジトリ以外のmigrationも含まれる。以下のCLI手順は履歴の整合を確認できる場合に使用し、履歴の差分を一括修復・一括適用しない。

## 1. 事前確認

### 1.1 作業ディレクトリ

```bash
cd /Users/yamada2/バイブコーディング/imakite_supabase
git status --short
supabase --version
```

意図しない変更がないことを確認する。秘密情報、`.env`、`token.json`はcommitしない。

### 1.2 GitHub Actions Variables

GitHubリポジトリの次の画面を開く。

```text
Settings > Secrets and variables > Actions > Variables
```

次のRepository variablesを設定する。未設定でも括弧内の値がWorkflowの既定値になるが、ロールバックを容易にするため明示設定を推奨する。

```text
IHC_RANKING_ALGORITHM_VERSION=v2
IHC_WEEKLY_ALGORITHM_VERSION=v1
```

これらは秘密情報ではないため、SecretsではなくVariablesへ設定する。

### 1.3 Supabaseログインと本番プロジェクトへのリンク

Supabase Dashboardの本番プロジェクトでProject Refを確認する。

```bash
supabase login
supabase link --project-ref <本番PROJECT_REF>
supabase migration list
```

`supabase migration list`で、接続先が本番プロジェクトであることと、ローカル・リモートのmigration履歴に想定外の差分がないことを確認する。

## 2. Migration適用

### 2.1 Dry run

```bash
supabase db push --dry-run
```

適用対象に次のmigrationが含まれることを確認する。

```text
20261111050025_add_ihc_ranking_algorithm_versions.sql
```

想定外のmigrationが表示された場合は、`supabase db push`を実行せず、migration履歴を確認する。

### 2.2 本番適用

```bash
supabase db push
supabase migration list
```

適用後、LocalとRemoteの両方に`20261111050025`が表示されることを確認する。

## 3. 適用するSQL全文

原則として、SQL Editorから直接実行せず、前節の`supabase db push`でmigrationを適用する。CLI適用ができず、緊急にSQL Editorを使用する場合だけ、以下を本番プロジェクトで実行する。

SQL Editorで直接実行した場合も、このmigrationファイルを削除してはならない。migrationがDB変更履歴の正史である。

```sql
alter table if exists ihc.daily_rankings
  add column if not exists base_score numeric,
  add column if not exists conversion_bonus_awarded numeric not null default 0,
  add column if not exists conversion_momentum numeric not null default 0,
  add column if not exists algorithm_version text not null default 'v1';

update ihc.daily_rankings
set base_score = coalesce(score, 0)
where base_score is null;

alter table if exists ihc.daily_rankings
  alter column base_score set default 0,
  alter column base_score set not null;

alter table if exists ihc.weekly_rankings
  add column if not exists base_total_score numeric,
  add column if not exists conversion_momentum numeric not null default 0,
  add column if not exists v2_candidate_total_score numeric,
  add column if not exists v1_candidate_rank integer,
  add column if not exists v2_candidate_rank integer,
  add column if not exists algorithm_version text not null default 'v1';

update ihc.weekly_rankings
set base_total_score = coalesce(base_total_score, total_score, 0),
    v2_candidate_total_score = coalesce(
      v2_candidate_total_score,
      total_score,
      0
    ),
    v1_candidate_rank = coalesce(v1_candidate_rank, rank),
    v2_candidate_rank = coalesce(v2_candidate_rank, rank)
where base_total_score is null
   or v2_candidate_total_score is null
   or v1_candidate_rank is null
   or v2_candidate_rank is null;

alter table if exists ihc.weekly_rankings
  alter column base_total_score set default 0,
  alter column base_total_score set not null,
  alter column v2_candidate_total_score set default 0,
  alter column v2_candidate_total_score set not null;
```

SQL Editorで直接実行した場合は、後述のDB確認SQLがすべて正常であることを確認してから、リモートのmigration履歴を修復する。

```bash
supabase migration repair 20261111050025 --status applied
supabase migration list
```

SQLが途中で失敗した場合は`migration repair`を実行しない。

## 4. DB確認SQL

### 4.1 カラム追加確認

```sql
select
  table_name,
  column_name,
  data_type,
  is_nullable,
  column_default
from information_schema.columns
where table_schema = 'ihc'
  and table_name in ('daily_rankings', 'weekly_rankings')
  and column_name in (
    'base_score',
    'conversion_bonus_awarded',
    'conversion_momentum',
    'v2_candidate_total_score',
    'v1_candidate_rank',
    'v2_candidate_rank',
    'algorithm_version'
  )
order by table_name, ordinal_position;
```

### 4.2 既存データのv1バックフィル確認

次の結果がすべて0件であることを確認する。

```sql
select count(*) as invalid_daily_rows
from ihc.daily_rankings
where base_score is null
   or conversion_bonus_awarded is null
   or conversion_momentum is null
   or algorithm_version is null;

select count(*) as invalid_weekly_rows
from ihc.weekly_rankings
where base_total_score is null
   or v2_candidate_total_score is null
   or conversion_momentum is null
   or algorithm_version is null;
```

既存行がv1として保存されていることも確認する。

```sql
select algorithm_version, count(*)
from ihc.daily_rankings
group by algorithm_version
order by algorithm_version;

select algorithm_version, count(*)
from ihc.weekly_rankings
group by algorithm_version
order by algorithm_version;
```

## 5. コードのGitHub反映

テストを実行する。

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
git diff --check
```

変更対象を確認してから、明示的にstageする。

```bash
git add .github/workflows/ihc_ranking.yml
git add .github/workflows/ihc_weekly_ranking.yml
git add scripts/ihc_ranking_job.py
git add scripts/ihc_weekly_ranking_job.py
git add scripts/ihc_score_algorithms.py
git add tests/test_ihc_score_algorithms.py
git add supabase/migrations/20261111050025_add_ihc_ranking_algorithm_versions.sql
git add sources/docs/ihc-ranking-v2-production-runbook.md
git add sources/docs/ihc-technical-brief.md
git status --short
git commit -m "Add IHC ranking v2 conversion momentum"
git push origin main
```

`git add .`は使用せず、対象ファイルだけをstageする。既存の未追跡ファイルや無関係な作業を誤ってcommitしないためである。

## 6. デイリーv2初回確認

GitHub Actionsで次を開く。

```text
Actions > IHC Ranking to Supabase > Run workflow
```

入力値：

```text
snapshot_date: スナップショットが存在する確認対象日
reset_daily_top20: true
algorithm_version: v2
```

Workflowが成功したら、Supabase SQL Editorで確認する。

```sql
select
  snapshot_date,
  rank,
  artist_name,
  popularity_delta,
  base_score,
  conversion_bonus_awarded,
  conversion_momentum,
  score,
  algorithm_version
from ihc.daily_rankings
where snapshot_date = (
  select max(snapshot_date)
  from ihc.daily_rankings
)
order by rank
limit 50;
```

確認事項：

- `algorithm_version = 'v2'`
- `score = base_score + conversion_momentum`
- 直前28日以内にプラス変動がない状態での`+1`では`conversion_bonus_awarded = 0`
- 直前28日以内にプラス変動があるグループの新しい`+1`では、通常`conversion_bonus_awarded = 5`
- `conversion_momentum`が負数になっていない
- popularityが0、1、2または欠損の行では、従来どおりスコアが0

過去のボーナスイベントはバックフィルしないため、conversion momentumが完全に温まるまで最大28日かかる。過去の`popularity_delta`は、新しい`+1`が反復かどうかの判定には使用される。

## 7. 週次v1・v2シャドー集計の確認

週次Workflowの公開アルゴリズムはv1のままにする。

```text
IHC_WEEKLY_ALGORITHM_VERSION=v1
```

火曜日の自動実行後、次を確認する。

```sql
select
  week_end_date,
  rank as published_rank,
  v1_candidate_rank,
  v2_candidate_rank,
  artist_name,
  base_total_score,
  conversion_momentum,
  v2_candidate_total_score,
  total_score,
  algorithm_version
from ihc.weekly_rankings
where week_end_date = (
  select max(week_end_date)
  from ihc.weekly_rankings
)
order by rank
limit 50;
```

v1公開中は次の関係になる。

```text
algorithm_version = v1
total_score = base_total_score
rank = v1_candidate_rank
v2_candidate_total_score = base_total_score + 月曜時点のconversion_momentum
```

28日間のウォームアップ後、さらに4週間以上、v1とv2の候補順位を比較する。週次v2へ切り替える場合は、完全な火曜〜月曜週の境界で`IHC_WEEKLY_ALGORITHM_VERSION`を`v2`へ変更する。

## 8. ロールバック

### 8.1 デイリーをv1へ戻す

GitHub ActionsのRepository variableを変更する。

```text
IHC_RANKING_ALGORITHM_VERSION=v1
```

次回の自動実行から、次の関係に戻る。

```text
score = base_score
conversion_bonus_awarded = 0
conversion_momentum = 0
algorithm_version = v1
```

すでに確定保存された過去日のv2ランキングは、自動的には書き換えない。過去データをv1で再計算する場合は、対象日と影響範囲を確認してから手動実行する。

### 8.2 週次をv1へ戻す

```text
IHC_WEEKLY_ALGORITHM_VERSION=v1
```

次回の週次実行から、`base_total_score`を公開用`total_score`として使用する。

### 8.3 DBカラムについて

追加したカラムは削除しない。v1は追加カラムが存在しても動作し、カラム削除は不要かつ破壊的である。問題発生時はアルゴリズム変数をv1へ戻し、原因を調査する。

## 9. 本番反映完了条件

- migration履歴のLocal/Remoteが一致している
- デイリーv2 Workflowが成功した
- `score = base_score + conversion_momentum`を確認した
- `algorithm_version = 'v2'`を確認した
- 週次公開値がv1のままである
- `v1_candidate_rank`と`v2_candidate_rank`が保存されている
- Repository variablesの設定値を確認した
- 新しい運用期限や次回確認日をカレンダーへ登録した
