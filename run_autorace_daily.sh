#!/usr/bin/env bash
set -Eeuo pipefail

# ==== 設定（必要に応じて調整） ====
RACEDY_JST="${1:-$(TZ=Asia/Tokyo date +%Y%m%d)}"

PYTHON="${PYTHON:-python3}"

CSV_DIR="/mnt/next/autorace_day_date/csv"
JSON_DIR="/mnt/next/autorace_day_date/date"

REPO_DIR="/path/to/your/local/autorace-repo"   # 例: /home/you/repos/autorace
REPO_SUBDIR="day_date/date"                     # リポ内の保存先ディレクトリ（好みで）

DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"  # 任意（通知が不要なら空でOK）
# ===================================

echo "[INFO] scrape ${RACEDY_JST}"
${PYTHON} scrape_autorace_oddspark.py "${RACEDY_JST}"

echo "[INFO] convert CSV -> JSON"
${PYTHON} convert_autorace_csv_to_json.py "${RACEDY_JST}"

JSON_FILE="${JSON_DIR}/autorace_race_list_${RACEDY_JST}.json"

if [[ ! -f "${JSON_FILE}" ]]; then
  echo "[ERROR] JSON not found: ${JSON_FILE}"
  exit 2
fi

# リポへコピー
DEST_DIR="${REPO_DIR}/${REPO_SUBDIR}"
mkdir -p "${DEST_DIR}"
cp -f "${JSON_FILE}" "${DEST_DIR}/"

pushd "${REPO_DIR}" >/dev/null
git add "${REPO_SUBDIR}/autorace_race_list_${RACEDY_JST}.json" || true
git commit -m "autorace ${RACEDY_JST}" || echo "[INFO] nothing to commit"
git pull --rebase
git push
popd >/dev/null

# 任意: Discord通知
if [[ -n "${DISCORD_WEBHOOK_URL}" ]]; then
  curl -sS -H "Content-Type: application/json" -X POST \
    -d "{\"content\":\"[autorace] ${RACEDY_JST} 更新: autorace_race_list_${RACEDY_JST}.json\"}" \
    "${DISCORD_WEBHOOK_URL}" || true
fi

echo "[DONE] ${JSON_FILE} -> ${REPO_DIR}/${REPO_SUBDIR}/"
