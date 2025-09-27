#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OddsPark オートレース スクレイパ（raceDy フィルタ対応版）
  (1) https://www.oddspark.com/autorace/KaisaiRaceList.do にアクセス
      → ページ全体から「1日出走表」(OneDayRaceList.do) リンクを収集
  (2) 収集直後に raceDy（対象日）で厳密フィルタ
  (3) 各 OneDayRaceList を巡回し、R単位のデータを抽出
  (4) 中間CSVを /mnt/next/autorace_day_date/csv/autorace_{YYYYMMDD}.csv に出力

出力CSV列（競輪の変換器に寄せる＝そのまま流用可能）:
  date, venue, grade, race_number, start_time, closed_at, players, class_category

要件:
- players は "1氏名" 形式の 6〜8 名
- closed_at は start_time の 2分前（HH:MM）
- レースURLは出力しない
"""

import csv
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://www.oddspark.com"
KAISAI_URL = f"{BASE}/autorace/KaisaiRaceList.do"

# 02=川口, 03=伊勢崎, 04=浜松, 05=飯塚, 06=山陽 （01欠番）
PLACECD_TO_VENUE = {
    "02": "川口",
    "03": "伊勢崎",
    "04": "浜松",
    "05": "飯塚",
    "06": "山陽",
}

JST = timezone(timedelta(hours=9))

OUT_DIR_CSV = "/mnt/next/autorace_day_date/csv"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def jst_today_str() -> str:
    return datetime.now(JST).strftime("%Y%m%d")


def http_get(url: str, max_retry: int = 3, sleep_sec: float = 0.8) -> requests.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MofuTimer-Auto/1.0; +https://example.invalid)",
        "Accept-Language": "ja,en;q=0.8",
        "Referer": "https://www.oddspark.com/autorace/",
        "Cache-Control": "no-cache",
    }
    last_exc = None
    for i in range(1, max_retry + 1):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200 and r.text:
                return r
            print(f"[WARN] GET {url} -> status={r.status_code}, len={len(r.text)} (try {i}/{max_retry})")
        except Exception as e:
            last_exc = e
            print(f"[WARN] GET {url} failed: {e} (try {i}/{max_retry})")
        time.sleep(sleep_sec * i)
    if last_exc:
        raise last_exc
    r.raise_for_status()
    return r


def collect_oneday_links(kaisai_html: str):
    """
    KaisaiRaceList.do から /autorace/OneDayRaceList.do?raceDy=YYYYMMDD&placeCd=PP を全件収集
    - #raceToday が無いケースを考慮し、まずはページ全体から拾う
    - (raceDy, placeCd) の重複は除去
    """
    soup = BeautifulSoup(kaisai_html, "lxml")

    anchors = soup.select('a[href*="/autorace/OneDayRaceList.do"]')
    if not anchors:
        race_today = soup.select_one("#raceToday")
        anchors = race_today.select('a[href*="/autorace/OneDayRaceList.do"]') if race_today else []

    found = []
    seen = set()
    for a in anchors:
        href = a.get("href", "")
        if "/autorace/OneDayRaceList.do" not in href:
            continue
        abs_url = urljoin(BASE, href)
        q = parse_qs(urlparse(abs_url).query)
        raceDy = (q.get("raceDy") or [""])[0]
        placeCd = (q.get("placeCd") or [""])[0]
        if not raceDy or not placeCd:
            continue
        key = (raceDy, placeCd)
        if key in seen:
            continue
        seen.add(key)
        found.append(abs_url)
    return found


def hhmm_to_closed(hhmm: str, date_str: str) -> str:
    """
    'HH:MM' -> 2分前 ('HH:MM') を返す。
    - 24時台（および将来の25時台など HH>=24）に対応。
    - フォーマット方針:
        * 入力が 24:xx 以上のときは、closed も 24:xx 形式で返す
          （ただし 2分戻して 23時台へ跨ぐときは通常の 23:xx）
    """
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", hhmm)
    if not m:
        return hhmm  # フォールバック

    h = int(m.group(1))
    mi = int(m.group(2))
    if not (0 <= mi < 60):
        return hhmm

    # 基準日
    base = datetime.strptime(date_str, "%Y%m%d")

    # 実際の日時に変換（24時台は翌日扱い）
    day_offset, h_real = (divmod(h, 24) if h >= 24 else (0, h))
    start_dt = base + timedelta(days=day_offset, hours=h_real, minutes=mi)

    # 2分前
    closed_dt = start_dt - timedelta(minutes=2)

    # 表示ルール：
    # 入力が 24時台以上なら、closed も 24時台表記を維持（同じ日帯なら）。
    if h >= 24:
        # 24時帯の“日境界”＝ base + 1日 00:00
        next_day = base + timedelta(days=1)
        if closed_dt.date() == next_day.date():
            # 24:xx 形式で返す（00:xx → 24:xx, 01:xx → 25:xx … を一般化）
            hours_24style = closed_dt.hour + 24
            return f"{hours_24style:02d}:{closed_dt.minute:02d}"
        else:
            # 23時台に戻った場合は通常表記
            return closed_dt.strftime("%H:%M")
    else:
        # 通常（<24時）
        return closed_dt.strftime("%H:%M")



def extract_races_from_oneday(html: str, raceDy: str, placeCd: str):
    """
    OneDayRaceList.do の1ページから R 単位データを抽出（DOMに合わせた実装）
    返却: dict {
      'venue': str,
      'grade': str (空可),
      'rows': [ (race_number, start_time, closed_at, players(list[str]), class_category) ... ],
    }
    """
    soup = BeautifulSoup(html, "lxml")

    venue = PLACECD_TO_VENUE.get(placeCd, "")
    grade = ""  # ページに無い場合は空で運用

    race_sections = []

    # 各レースのヘッダは div.h30 の中に strong > a[Racelist] と start-time がある
    for box in soup.select("div.h30"):
        a = box.select_one('strong > a[href*="/autorace/RaceList.do"]')
        if not a:
            continue

        # 例: "9R  予選  3100m(6周)"
        title_txt = a.get_text(" ", strip=True).replace("　", " ")
        m = re.search(r"(\d+)\s*R\b", title_txt)
        if not m:
            continue
        race_no = int(m.group(1))

        # 発走時間（例: <span class="start-time">発走時間 <strong>18:48</strong></span>）
        st = box.select_one("span.start-time strong")
        if not st:
            # 後方互換：近傍テキストから拾う
            near = box.get_text(" ", strip=True)
            m2 = re.search(r"(発走(?:時間|予定)?)[^\d]*(\d{1,2}:\d{2})", near)
            if not m2:
                near2 = " ".join(t.get_text(" ", strip=True) for t in box.find_all(limit=5))
                m2 = re.search(r"(発走(?:時間|予定)?)[^\d]*(\d{1,2}:\d{2})", near2)
            if m2:
                hh, mm = m2.group(2).split(":")
                start_time = f"{int(hh):02d}:{int(mm):02d}"
            else:
                start_time = None
        else:
            hh, mm = st.get_text(strip=True).split(":")
            start_time = f"{int(hh):02d}:{int(mm):02d}"

        # このヘッダ直後の出走表テーブル（通常 table.tb70）
        table = box.find_next("table")
        if not table:
            continue
        head_txt = table.get_text(" ", strip=True)
        if not re.search(r"(車\s*番|選手名|LG|ハンデ|現ランク|審査|試走)", head_txt):
            continue

        # 区分（一般/予選/準決/優勝戦 など）を見出しから推測
        cat = ""
        mcat = re.search(r"(一般|予選|準決勝?|優勝戦|特別|選抜)", title_txt)
        if mcat:
            cat = mcat.group(1)

        # テーブルから選手（車番+氏名）を抽出
        players = []
        for tr in table.select("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            # 先頭セルが 1..8 の行だけ
            car_td = tr.find("td", class_=re.compile(r"\bbg-[1-8]\b"))
            if not car_td:
                continue
            car_txt = car_td.get_text(strip=True)
            if not re.fullmatch(r"[1-8]", car_txt):
                continue

            # 選手名セルは td.racer1（予想印2列を無視できる）
            name_cell = tr.select_one("td.racer1")
            if not name_cell:
                # フォールバック：2列目以降から拾う（万一のDOM違い）
                cand = [td for td in tds[1:] if td.get_text(strip=True)]
                name_cell = cand[0] if cand else None
            if not name_cell:
                continue

            name_txt = name_cell.get_text("", strip=True)
            name_txt = re.sub(r"\s+", "", name_txt)  # 全スペース除去
            players.append(f"{car_txt}{name_txt}")

        # 6〜8名＆発走時刻ありのみ採用
        if 6 <= len(players) <= 8 and start_time:
            closed_at = hhmm_to_closed(start_time, raceDy)
            race_sections.append((race_no, start_time, closed_at, players, cat))

    race_sections.sort(key=lambda x: x[0])
    return {"venue": venue, "grade": grade, "rows": race_sections}


def _filter_links_by_day(_links, _raceDy):
    """収集済み OneDay リンクを raceDy で厳密フィルタ（重複(placeCd)も排除）"""
    filtered = []
    seen = set()
    for u in _links:
        q = parse_qs(urlparse(u).query)
        dy = (q.get("raceDy") or [""])[0]
        pc = (q.get("placeCd") or [""])[0]
        if dy == _raceDy and pc:
            key = (dy, pc)
            if key not in seen:
                seen.add(key)
                filtered.append(u)
    return filtered


def main():
    # 対象日（YYYYMMDD）: 引数無しはJST今日
    raceDy = sys.argv[1] if len(sys.argv) >= 2 else jst_today_str()

    ensure_dir(OUT_DIR_CSV)
    out_csv = os.path.join(OUT_DIR_CSV, f"autorace_{raceDy}.csv")

    # 1) 開催一覧 → 1日出走表リンク収集（まずはパラメータ無し）
    print(f"[INFO] GET (no param): {KAISAI_URL}")
    r = http_get(KAISAI_URL)
    links = collect_oneday_links(r.text)
    print(f"[INFO] found links (no param) = {len(links)}")

    # ★ raceDy で厳密フィルタ
    links = _filter_links_by_day(links, raceDy)
    print(f"[INFO] filtered (no param) to raceDy={raceDy}: {len(links)}")

    if not links:
        url_with_day = KAISAI_URL + f"?raceDy={raceDy}"
        print(f"[INFO] GET (with raceDy): {url_with_day}")
        r2 = http_get(url_with_day)
        links = collect_oneday_links(r2.text)
        print(f"[INFO] found links (with raceDy) = {len(links)}")

        # ★ 再度、raceDy で絞る
        links = _filter_links_by_day(links, raceDy)
        print(f"[INFO] filtered (with raceDy) to raceDy={raceDy}: {len(links)}")

    if not links:
        print("[WARN] OneDay links not found for target day; aborting early.")

    # 2) 各 OneDay ページを巡回 → CSV 行を組み立て
    print(f"[INFO] crawl OneDay pages: {len(links)} targets")
    rows_out = []
    for url in links:
        print(f"[INFO] OneDay: {url}")
        q = parse_qs(urlparse(url).query)
        dy = (q.get("raceDy") or [""])[0]
        placeCd = (q.get("placeCd") or [""])[0]
        if not dy or not placeCd:
            print("[WARN] skip (missing raceDy/placeCd)")
            continue
        if dy != raceDy:
            # ★ 念のため二重チェック
            print(f"[INFO] skip other day link dy={dy} (target={raceDy})")
            continue

        rr = http_get(url)
        data = extract_races_from_oneday(rr.text, dy, placeCd)
        venue = data["venue"]
        grade = data["grade"]
        print(f"[INFO] venue={venue!r} rows={len(data['rows'])}")

        for race_no, start_time, closed_at, players, class_cat in data["rows"]:
            rows_out.append([
                dy,             # date (YYYYMMDD)
                venue,          # venue
                grade,          # grade (空可)
                race_no,        # race_number
                start_time,     # start_time HH:MM
                closed_at,      # closed_at HH:MM (start -2m)
                ",".join(players),  # players "1氏名,2氏名,..."
                class_cat,      # class_category
            ])

    # 3) CSV 出力
    if rows_out:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "date", "venue", "grade", "race_number",
                "start_time", "closed_at", "players", "class_category"
            ])
            w.writerows(rows_out)
        print(f"[SAVE] {out_csv} ({len(rows_out)} rows)")
    else:
        print("[WARN] No rows extracted.")


if __name__ == "__main__":
    main()
