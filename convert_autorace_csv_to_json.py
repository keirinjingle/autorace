#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中間CSV (autorace_{YYYYMMDD}.csv) を
  競輪と同スキーマの JSON に変換して
  /mnt/next/autorace_day_date/date/autorace_race_list_{YYYYMMDD}.json
へ出力する。

CSV 列:
  date, venue, grade, race_number, start_time, closed_at, players, class_category

JSON 構造（1日＝配列）:
[
  {
    "venue": "伊勢崎",
    "grade": "一般",
    "races": [
      {
        "race_number": 1,
        "start_time": "15:00",
        "closed_at": "14:58",
        "players": ["1氏名","2氏名","..."],   // 6〜8件
        "class_category": "一般"
      }
    ]
  }
]
"""

import csv
import json
import os
import sys
from collections import defaultdict, OrderedDict

IN_DIR = "/mnt/next/autorace_day_date/csv"
OUT_DIR = "/mnt/next/autorace_day_date/date"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def main():
    if len(sys.argv) < 2:
        print("Usage: convert_autorace_csv_to_json.py YYYYMMDD")
        sys.exit(1)
    ymd = sys.argv[1]

    in_csv = os.path.join(IN_DIR, f"autorace_{ymd}.csv")
    out_json = os.path.join(OUT_DIR, f"autorace_race_list_{ymd}.json")
    ensure_dir(OUT_DIR)

    if not os.path.exists(in_csv):
        print(f"[ERROR] not found: {in_csv}")
        sys.exit(2)

    # venue × grade でグルーピング
    groups = defaultdict(list)
    with open(in_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # players はカンマ区切り → 配列化（空要素を除去）
            players = [p for p in (row["players"] or "").split(",") if p]
            # バリデーション: 6〜8名のみ
            if not (6 <= len(players) <= 8):
                continue
            races_item = OrderedDict([
                ("race_number", int(row["race_number"])),
                ("start_time", row["start_time"]),
                ("closed_at", row["closed_at"]),
                ("players", players),
                ("class_category", row.get("class_category", "")),
            ])
            key = (row["venue"], row.get("grade", ""))
            groups[key].append(races_item)

    # 各グループの races を race_number 昇順に
    out_arr = []
    for (venue, grade), items in groups.items():
        items.sort(key=lambda x: x["race_number"])
        out_arr.append(OrderedDict([
            ("venue", venue),
            ("grade", grade),
            ("races", items),
        ]))

    # 出力
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out_arr, f, ensure_ascii=False, indent=2)

    print(f"[SAVE] {out_json} (groups={len(out_arr)})")


if __name__ == "__main__":
    main()
