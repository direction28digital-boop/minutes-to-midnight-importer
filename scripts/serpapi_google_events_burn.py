#!/usr/bin/env python3
"""
Burn SerpAPI searches intentionally while capturing useful Google Events data.

Fix: Do NOT use the `location` parameter (it requires a SerpAPI canonical location string).
Instead, embed the city/region into the query text, which SerpAPI supports for Google Events.

- Reads one or more hubs JSON files (each is a JSON array of {hubId, city, regionCode, countryCode})
- Executes a configurable query pack against SerpAPI's Google Events engine
- Writes one JSON object per API call to a JSONL file
- Safe to resume: each line includes a stable taskId; existing taskIds are skipped

Requires:
  pip install requests
Env:
  SERPAPI_API_KEY=...
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"

DEFAULT_TERMS = [
    "dog friendly events",
    "dog adoption event",
    "pet adoption event",
    "dog rescue fundraiser",
    "pet friendly festival",
    "dog meetup",
]

# Values for htichips (date filters) per SerpAPI docs:
# date:today, date:tomorrow, date:week, date:next_week, date:month, date:next_month
DEFAULT_DATE_FILTERS = [
    "date:week",
    "date:next_week",
    "date:month",
    "date:next_month",
]


@dataclass(frozen=True)
class Hub:
    hubId: str
    city: str
    regionCode: str
    countryCode: str


@dataclass(frozen=True)
class Task:
    hub: Hub
    term: str
    htichips: str
    start: int

    @property
    def task_id(self) -> str:
        raw = f"{self.hub.hubId}|{self.term}|{self.htichips}|{self.start}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_hubs(paths: List[str]) -> List[Hub]:
    hubs: List[Hub] = []
    for p in paths:
        p = p.strip()
        if not p:
            continue
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError(f"{p} is not a JSON array.")
        for row in data:
            hubs.append(
                Hub(
                    hubId=str(row["hubId"]),
                    city=str(row["city"]),
                    regionCode=str(row["regionCode"]),
                    countryCode=str(row["countryCode"]),
                )
            )
    # Dedupe by hubId (keeps first occurrence)
    dedup: Dict[str, Hub] = {}
    for h in hubs:
        if h.hubId not in dedup:
            dedup[h.hubId] = h
    return list(dedup.values())


def build_query(task: Task) -> str:
    # Embed location directly in the query string to avoid strict `location=` validation.
    # Examples:
    #   "dog friendly events in Surrey, BC"
    #   "dog adoption event in Austin, TX"
    return f"{task.term} in {task.hub.city}, {task.hub.regionCode}"


def read_done_task_ids(out_path: Path) -> Set[str]:
    done: Set[str] = set()
    if not out_path.exists():
        return done
    with out_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                tid = obj.get("taskId")
                if isinstance(tid, str) and tid:
                    done.add(tid)
            except Exception:
                continue
    return done


def parse_csv_list(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def parse_int_list(s: str) -> List[int]:
    out: List[int] = []
    for part in parse_csv_list(s):
        out.append(int(part))
    return out


def generate_tasks(
    hubs: List[Hub],
    terms: List[str],
    date_filters: List[str],
    starts: List[int],
    done_task_ids: Set[str],
    max_searches: int,
) -> List[Task]:
    tasks: List[Task] = []
    for h in hubs:
        for term in terms:
            for df in date_filters:
                for s in starts:
                    t = Task(hub=h, term=term, htichips=df, start=s)
                    if t.task_id in done_task_ids:
                        continue
                    tasks.append(t)
                    if len(tasks) >= max_searches:
                        return tasks
    return tasks


def fetch_one(
    task: Task,
    api_key: str,
    hl: str,
    gl: str,
    no_cache: bool,
    timeout_s: int,
    throttle_s: float,
) -> Dict[str, Any]:
    q_full = build_query(task)

    params = {
        "engine": "google_events",
        "q": q_full,
        "hl": hl,
        "gl": gl,
        "start": task.start,
        "htichips": task.htichips,
        "api_key": api_key,
    }
    if no_cache:
        params["no_cache"] = "true"

    started = _now_iso()
    http_status: Optional[int] = None

    try:
        r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=timeout_s)
        http_status = r.status_code
        data = r.json()

        meta_status = None
        if isinstance(data, dict):
            meta_status = data.get("search_metadata", {}).get("status")
        if meta_status:
            status = meta_status
        elif isinstance(data, dict) and data.get("error"):
            status = "Error"
        else:
            status = None

        out = {
            "taskId": task.task_id,
            "ts": started,
            "engine": "google_events",
            "hub": {
                "hubId": task.hub.hubId,
                "city": task.hub.city,
                "regionCode": task.hub.regionCode,
                "countryCode": task.hub.countryCode,
            },
            "query": {
                "q": q_full,
                "term": task.term,
                "htichips": task.htichips,
                "start": task.start,
                "hl": hl,
                "gl": gl,
                "no_cache": bool(no_cache),
            },
            "httpStatus": http_status,
            "serpapiStatus": status,
            "response": data,
        }

    except Exception as e:
        out = {
            "taskId": task.task_id,
            "ts": started,
            "engine": "google_events",
            "hub": {
                "hubId": task.hub.hubId,
                "city": task.hub.city,
                "regionCode": task.hub.regionCode,
                "countryCode": task.hub.countryCode,
            },
            "query": {
                "q": q_full,
                "term": task.term,
                "htichips": task.htichips,
                "start": task.start,
                "hl": hl,
                "gl": gl,
                "no_cache": bool(no_cache),
            },
            "httpStatus": http_status,
            "serpapiStatus": "Error",
            "error": str(e),
        }

    if throttle_s > 0:
        time.sleep(throttle_s)

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--hubs",
        required=True,
        help="Comma-separated list of hub JSON files (each file is a JSON array).",
    )
    ap.add_argument("--out", required=True, help="Output JSONL file path (appends).")
    ap.add_argument(
        "--max-searches",
        type=int,
        default=2000,
        help="Hard cap on API calls in this run (after resume skipping).",
    )
    ap.add_argument(
        "--limit-hubs",
        type=int,
        default=0,
        help="If > 0, only process the first N hubs (useful for smoke tests).",
    )
    ap.add_argument(
        "--terms",
        default="",
        help="Comma-separated override for query terms. If empty, defaults are used.",
    )
    ap.add_argument(
        "--date-filters",
        default="",
        help="Comma-separated override for htichips values. If empty, defaults are used.",
    )
    ap.add_argument(
        "--starts",
        default="0",
        help="Comma-separated list of start offsets (e.g. '0,10,20'). Default: 0",
    )
    ap.add_argument("--concurrency", type=int, default=6)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument(
        "--throttle",
        type=float,
        default=0.0,
        help="Sleep this many seconds after each request in each worker (0 disables).",
    )
    ap.add_argument(
        "--allow-cache",
        action="store_true",
        help="If set, do NOT send no_cache=true (allows free cached responses).",
    )
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--progress-every", type=int, default=25)

    args = ap.parse_args()

    api_key = os.environ.get("SERPAPI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: SERPAPI_API_KEY is not set.", file=sys.stderr)
        return 2

    hub_paths = parse_csv_list(args.hubs)
    hubs = load_hubs(hub_paths)
    if args.limit_hubs and args.limit_hubs > 0:
        hubs = hubs[: args.limit_hubs]

    terms = parse_csv_list(args.terms) if args.terms.strip() else DEFAULT_TERMS
    date_filters = (
        parse_csv_list(args.date_filters)
        if args.date_filters.strip()
        else DEFAULT_DATE_FILTERS
    )
    starts = parse_int_list(args.starts)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    done_task_ids: Set[str] = set()
    if args.resume:
        done_task_ids = read_done_task_ids(out_path)

    tasks = generate_tasks(
        hubs=hubs,
        terms=terms,
        date_filters=date_filters,
        starts=starts,
        done_task_ids=done_task_ids,
        max_searches=args.max_searches,
    )

    print(
        f"plan hubs={len(hubs)} terms={len(terms)} date_filters={len(date_filters)} "
        f"starts={len(starts)} resume_skip={len(done_task_ids)} will_run={len(tasks)}"
    )

    completed = 0
    ok = 0
    err = 0

    with out_path.open("a", encoding="utf-8") as out_f:
        with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
            futures = []
            for task in tasks:
                gl = task.hub.countryCode.lower()
                hl = "en"
                futures.append(
                    ex.submit(
                        fetch_one,
                        task,
                        api_key,
                        hl,
                        gl,
                        (not args.allow_cache),
                        args.timeout,
                        args.throttle,
                    )
                )

            for fut in as_completed(futures):
                res = fut.result()
                out_f.write(json.dumps(res, ensure_ascii=False) + "\n")
                out_f.flush()

                completed += 1
                serp_status = res.get("serpapiStatus")
                if serp_status == "Success":
                    ok += 1
                else:
                    err += 1

                if completed % max(1, args.progress_every) == 0:
                    print(f"progress completed={completed} ok={ok} err={err}")

    print(f"done completed={completed} ok={ok} err={err} out={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
