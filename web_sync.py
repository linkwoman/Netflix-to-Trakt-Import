"""Direct Trakt sync over HTTP for the web app.

Loads a run's sync_data.json (per-entity TMDb mapping + Netflix watched_at
timestamps), applies the user's review picks, and POSTs the result to Trakt's
/sync/history endpoint in chunked pages. Honors a dry_run flag.
"""

import csv
import json
import logging
import os

import requests

from web_oauth import load_authorization

SYNC_URL = "https://api.trakt.tv/sync/history"
DEFAULT_PAGE_SIZE = 1000


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


def _load_sync_data(run_dir):
    path = os.path.join(run_dir, "sync_data.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _build_legacy_payload(run_id, run_dir):
    """Fallback for older runs without sync_data.json — show-level only,
    no watched_at. Kept so old run snapshots still work."""
    resolved = _read_csv(os.path.join(run_dir, "resolved.csv"))
    picks = load_picks(run_id)
    movies, shows = [], []
    for row in resolved:
        if not row.get("tmdb_id"):
            continue
        item = {"ids": {"tmdb": int(row["tmdb_id"])}}
        if row["type"] == "movie":
            movies.append(item)
        else:
            shows.append(item)
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
    summary = {
        "movies": len(movies),
        "episodes": 0,
        "shows": len(shows),
        "show_fallback_count": 0,
        "legacy": True,
    }
    return payload, summary


def build_sync_payload(run_id, run_dir):
    """Build the Trakt /sync/history payload from sync_data.json + picks.

    Movies: each watched_at gets its own movie history entry.
    Shows: if the user's pick (or auto-resolved id) matches the show id used
        when the pipeline mapped episodes, send each episode with watched_at.
        Otherwise we don't have an episode mapping for the picked id, so fall
        back to show-level entries (one per unique watched_at).

    Returns (payload_dict, summary_dict).
    """
    sync_data = _load_sync_data(run_dir)
    if sync_data is None:
        return _build_legacy_payload(run_id, run_dir)

    picks = load_picks(run_id)
    movies, episodes, shows = [], [], []
    show_fallback = 0

    for m in sync_data.get("movies", []):
        rid = str(m["original_row_id"])
        pick = picks.get(rid)
        if pick is not None:
            if pick.get("action") != "accept":
                continue
            try:
                tmdb_id = int(pick.get("tmdb_id") or 0)
            except (TypeError, ValueError):
                tmdb_id = 0
        elif m.get("auto_resolved"):
            tmdb_id = m.get("auto_tmdb_id") or 0
        else:
            continue
        if not tmdb_id:
            continue
        for w in m.get("watched_at") or []:
            movies.append({"watched_at": w, "ids": {"tmdb": tmdb_id}})

    for s in sync_data.get("shows", []):
        rid = str(s["original_row_id"])
        pick = picks.get(rid)
        if pick is not None:
            if pick.get("action") != "accept":
                continue
            try:
                picked = int(pick.get("tmdb_id") or 0)
            except (TypeError, ValueError):
                picked = 0
        elif s.get("auto_resolved"):
            picked = s.get("auto_tmdb_id") or 0
        else:
            continue
        if not picked:
            continue

        auto_id = s.get("auto_tmdb_id")
        eps = s.get("episodes") or []
        if auto_id and picked == auto_id and eps:
            for ep in eps:
                episodes.append(
                    {"watched_at": ep["watched_at"], "ids": {"tmdb": ep["tmdb_id"]}}
                )
        else:
            # Picked a different show than the one episodes were mapped to,
            # or no episode mapping was available. Send show-level entries
            # with each unique watched_at as a best-effort fallback.
            show_fallback += 1
            for w in s.get("show_watches") or []:
                shows.append({"watched_at": w, "ids": {"tmdb": picked}})

    payload = {}
    if movies:
        payload["movies"] = movies
    if episodes:
        payload["episodes"] = episodes
    if shows:
        payload["shows"] = shows

    summary = {
        "movies": len(movies),
        "episodes": len(episodes),
        "shows": len(shows),
        "show_fallback_count": show_fallback,
        "legacy": False,
    }
    return payload, summary


def _chunk_payload(payload, page_size):
    """Yield sub-payloads where the sum of items across all categories is
    <= page_size. Preserves category structure inside each chunk."""
    flat = []
    for category in ("movies", "episodes", "shows"):
        for item in payload.get(category, []):
            flat.append((category, item))

    for i in range(0, len(flat), page_size):
        chunk = {}
        for category, item in flat[i : i + page_size]:
            chunk.setdefault(category, []).append(item)
        yield chunk


def sync_to_trakt(run_id, run_dir, client_id, dry_run=True, page_size=DEFAULT_PAGE_SIZE):
    """POST the built payload to Trakt /sync/history (chunked). Returns a
    dict describing what was sent / added."""
    payload, summary = build_sync_payload(run_id=run_id, run_dir=run_dir)

    if dry_run:
        logging.info(f"[DRY RUN] Would sync to Trakt: {summary}")
        # Trim large preview to avoid stuffing megabytes into the response.
        preview = {
            k: v[:5] + ([{"...": f"and {len(v)-5} more"}] if len(v) > 5 else [])
            for k, v in payload.items()
        }
        return {
            "dry_run": True,
            "summary": summary,
            "payload_preview": preview,
            "added": {
                "movies": summary["movies"],
                "episodes": summary["episodes"],
                "shows": summary["shows"],
            },
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

    total_added = {"movies": 0, "episodes": 0, "shows": 0}
    chunks_sent = 0
    for chunk in _chunk_payload(payload, page_size):
        if not chunk:
            continue
        resp = requests.post(SYNC_URL, json=chunk, headers=headers, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        added = body.get("added", {}) or {}
        for k in total_added:
            total_added[k] += int(added.get(k, 0) or 0)
        chunks_sent += 1
        logging.info(
            f"Trakt sync chunk {chunks_sent}: added {added}, "
            f"running total {total_added}"
        )

    return {
        "dry_run": False,
        "summary": summary,
        "chunks_sent": chunks_sent,
        "added": total_added,
    }
