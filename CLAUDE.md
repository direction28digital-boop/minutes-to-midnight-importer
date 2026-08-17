# CLAUDE.md — Dog & Events Importer (minutes-to-midnight-importer)

Guidance for any agent working in this repo. This file is about **what this is + how to work here**.

## What this is

The **shared data importer** that feeds the dog properties — **SnoutHub, Minutes to Midnight, and SOSHub**. Python scripts that fetch + normalize:

1. **Adoptable dogs** from the **Rescue Groups API** (rescuegroups.org/v5) — NOT Petfinder.
2. **Local adoption/rescue events** via **SerpAPI**, grouped by city/hub.

Output is normalized data the downstream sites/apps consume. This is shared infrastructure, not a user-facing app.

## Stack

- Python (see `requirements.txt`)
- External APIs: Rescue Groups v5 (dogs), SerpAPI (events)

## Key scripts

- `rg_fetch_and_normalize_http.py` — main dog fetch + normalize from Rescue Groups
- `events_fetch_and_normalize_serpapi.py` — events fetch + normalize via SerpAPI
- `events_by_hub.py` / `events-by-city.py` — group events by hub/city
- `places_prefill.py` — places data prefill
- `debug_rg_photos.py`, `inspect_page.py` — debugging helpers

> ⚠️ Confirm the current entry point / run order with the repo owner before a full run — there are multiple scripts and no `package`-style task runner. Document the canonical pipeline here once confirmed.

## Setup

```bash
pip install -r requirements.txt
# API keys via environment / .env (names only in docs, never commit values):
# RESCUEGROUPS_API_KEY, SERPAPI_API_KEY
```

## Conventions

- Dogs: only those with **real photos** (no placeholder silhouettes).
- Data source is **Rescue Groups**, never Petfinder.
- Secrets via env only; never commit keys.
- This importer is consumed by multiple projects — changing output shape is a breaking change. Coordinate before altering the normalized schema.
