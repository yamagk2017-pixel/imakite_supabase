
---

## `AGENTS.md`

```md
# AGENTS.md（Codex/Cursor 用 作業ルール）

このリポジトリは「IHC（イマキテランキング）の Sheets 依存を Supabase へ移行する」ための DB/migration 管理用リポジトリです。

## 必読ドキュメント
- `docs/ihc-supabase-migration.md`  
  → 設計の前提（キー戦略、テーブルの役割、バックフィル方針、未確定事項）がまとまっています。

## 絶対に守る前提（最重要）
1. **内部キー（master key）は `imd.groups.id`（uuid）で統一する**
2. 外部サービスID（Spotify/X/YouTube等）は `imd.external_ids` に集約し、保存時は **必ず group_id に正規化**する
3. IHCの履歴は
   - `ihc.artist_snapshots`（素材の履歴：source of truth）
   - `ihc.daily_rankings`（日次ランキング：確定保存）
   を基本に運用する
4. DB変更は **必ず migration として記録**し、GitHubへ push して正史を残す  
   - Supabase Studio での手作業変更は原則禁止（緊急時は必ず migration で追認）

## 運用上の重要方針
- 手動実行があるため、「集計対象日（YYYY-MM-DD）」は **明示入力必須**にする方向（未実装）
- 日次ランキングの確定タイミング（freeze）や実行ログ（run log）は要検討
- まずはバックフィル（2025-05-21〜今日）で `ihc.artist_snapshots` を埋めるのが優先

## セキュリティ
- DB接続文字列、APIキー等の秘密情報は `.env` 等に隔離し、Git管理しない
- `.gitignore` を適切に設定する（.DS_Store等も含める）

## 変更時の手順（推奨）
1. `supabase migration new <name>`
2. SQLを書く（破壊的変更は特に慎重に）
3. `supabase db push`
4. `supabase migration list` で Local/Remote 整合確認
5. `git add/commit/push`

以上。
