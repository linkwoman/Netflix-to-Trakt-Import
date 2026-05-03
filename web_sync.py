"""Direct Trakt sync over HTTP for the web app.

Builds a sync payload from a specific run's resolved.csv plus the user's
review picks for that run, then posts it to Trakt's /sync/history endpoint.
Honors a dry_run flag.
"""

import csv
import json
import logging
import os

import requests

from web_oauth import load_authorization

SYNC_URL = "https://api.trakt.tv/sync/history"


def _picks_path(run_id):
    return os.path.join("runs", run_id, "picks.json")


def load_picks(run_id):
    path = _picks_path(run_id)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def save_picks(run_id, picks):
    path = _picks_path(run_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(picks, f, indent=2)


def _read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_sync_payload(run_id, run_dir):
    """Build the payload Trakt expects from a run's resolved.csv + picks.

    Returns (payload_dict, summary_dict).

    Note: this MVP only syncs simple movies + show-level (no episode-level)
    because the CLI's episode-mapping path isn't easily replayable from CSV.
    For real episode-level sync, the existing CLI is the more complete option.
    """
    resolved = _read_csv(os.path.join(run_dir, "resolved.csv"))
    picks = load_picks(run_id)

    movies = []
    shows = []

    # All resolved items (auto-accepted by confidence)
    for row in resolved:
        if not row.get("tmdb_id"):
            continue
        item = {"ids": {"tmdb": int(row["tmdb_id"])}}
        if row["type"] == "movie":
            movies.append(item)
        else:
            shows.append(item)

    # Picks from the review queue
    for original_row_id, pick in picks.items():
        if pick.get("action") != "accept":
            continue
        tmdb_id = pick.get("tmdb_id")
        media_type = pick.get("media_type")
        if not tmdb_id or not media_type:
            continue
        item = {"ids": {"tmdb": int(tmdb_id)}}
        if media_type == "movie":
            movies.append(item)
        else:
            shows.append(item)

    payload = {}
    if movies:
        payload["movies"] = movies
    if shows:
        payload["shows"] = shows

    return payload, {"movies": len(movies), "shows": len(shows)}


def sync_to_trakt(run_id, run_dir, client_id, dry_run=True):
    """POST the built payload to Trakt /sync/history. Returns the response dict."""
    payload, summary = build_sync_payload(run_id=run_id, run_dir=run_dir)

    if dry_run:
        logging.info(f"[DRY RUN] Would sync to Trakt: {summary}")
        return {
            "dry_run": True,
            "summary": summary,
            "payload_preview": payload,
            "added": {"movies": summary["movies"], "shows": summary["shows"]},
        }

    auth = load_authorization()
    if not auth:
        raise RuntimeError("Not connected to Trakt — connect first via Settings.")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth['access_token']}",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
    }
    resp = requests.post(SYNC_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    body = resp.json()
    return {
        "dry_run": False,
        "summary": summary,
        "trakt_response": body,
        "added": body.get("added", {}),
    }
