#!/usr/bin/env python3
"""Backfill events from past CI artifacts into WordPress.

Between ~February and July 2026 the daily SerpAPI fetch saved its results
only as GitHub Actions artifacts (kept ~90 days). This script downloads the
recent ones, merges + dedupes them, drops clearly-expired events, and pushes
everything still relevant through scripts/upsert_events_to_wp.py, so months
of already-paid-for searches finally land on the site.

Run from the repo root (needs gh CLI logged in, which yours is):

    python3 scripts/backfill_events_from_artifacts.py

Options via env:
    RUNS_TO_SCAN=40     how many recent successful runs to pull (default 40)
    DRY=1               download + merge + report only, no WP upsert

The site picks the new events up from WP at its next daily import
(9:00 UTC cron), or sooner if an events workflow run fires its
"Trigger site import" step.
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)

ARTIFACT_NAME = "events-normalized-and-manifests"
WORKDIR = ROOT / ".backfill_artifacts"
MERGED = ROOT / "data_events" / "events.backfill.merged.jsonl"
RUNS_TO_SCAN = int(os.environ.get("RUNS_TO_SCAN", "40"))
DRY = os.environ.get("DRY", "0").strip() == "1"


def sh(args: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, **kw)


def load_env_local() -> dict:
    env = dict(os.environ)
    env_file = ROOT / ".env.local"
    if not env_file.exists():
        sys.exit(".env.local not found at repo root")
    for raw in env_file.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export "):]
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return env


def main() -> int:
    # 1. Find recent successful runs of the events workflow
    r = sh([
        "gh", "run", "list", "--workflow=events.yml", "--status=success",
        f"--limit={RUNS_TO_SCAN}", "--json", "databaseId",
    ])
    if r.returncode != 0:
        sys.exit(f"gh run list failed: {r.stderr[:300]}")
    run_ids = [str(x["databaseId"]) for x in json.loads(r.stdout or "[]")]
    print(f"Found {len(run_ids)} successful events runs to scan.")

    # 2. Download each run's artifact (older-than-90-days ones are gone;
    #    those only held long-expired events, so no loss).
    WORKDIR.mkdir(exist_ok=True)
    got = 0
    for rid in run_ids:
        dest = WORKDIR / rid
        already = dest.exists() and any(dest.rglob("events.normalized.jsonl"))
        if already:
            got += 1
            continue
        dl = sh(["gh", "run", "download", rid, "-n", ARTIFACT_NAME, "-D", str(dest)])
        if dl.returncode == 0:
            got += 1
            print(f"  run {rid}: downloaded")
        else:
            print(f"  run {rid}: no artifact (expired or absent), skipping")
    print(f"Artifacts in hand: {got}")

    # 3. Merge + dedupe (last occurrence wins = newest data for an event id)
    events: dict[str, dict] = {}
    total_lines = 0
    for f in sorted(WORKDIR.rglob("events.normalized.jsonl")):
        for line in f.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            total_lines += 1
            key = str(ev.get("id") or f"{ev.get('title','')}|{ev.get('startDateTime','')}")
            events[key] = ev

    # 4. Drop clearly-expired events (parseable start date before yesterday).
    #    Unparseable dates (e.g. "Aug 15") are KEPT; the site decides.
    cutoff = datetime.now() - timedelta(days=1)
    keep, expired = [], 0
    for ev in events.values():
        s = (ev.get("startDateTime") or "").strip()
        dropped = False
        if s:
            try:
                when = datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
                if when < cutoff:
                    expired += 1
                    dropped = True
            except ValueError:
                pass  # unparseable: keep
        if not dropped:
            keep.append(ev)

    MERGED.parent.mkdir(exist_ok=True)
    with MERGED.open("w") as out:
        for ev in keep:
            out.write(json.dumps(ev) + "\n")
    print(f"Merged {total_lines} lines -> {len(events)} unique -> "
          f"{len(keep)} still-relevant ({expired} expired dropped)")
    print(f"Merged file: {MERGED}")

    if DRY:
        print("DRY=1, stopping before WP upsert.")
        return 0
    if not keep:
        print("Nothing to upsert.")
        return 0

    # 5. Upsert to WordPress with creds from .env.local
    env = load_env_local()
    env["EVENTS_NORMALIZED_PATH"] = str(MERGED)
    up = subprocess.run(
        [sys.executable, "scripts/upsert_events_to_wp.py"], env=env, cwd=ROOT,
    )
    if up.returncode != 0:
        sys.exit(f"upsert exited with {up.returncode}")

    print("\nDone. The site pulls these from WP at its next daily import "
          "(9:00 UTC cron), so expect them on snouthub.com by tomorrow morning.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
