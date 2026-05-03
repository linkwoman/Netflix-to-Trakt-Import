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
WATCHED_SHOWS_URL = "https://api.trakt.tv/sync/watched/shows"
WATCHED_MOVIES_URL = "https://api.trakt.tv/sync/watched/movies"
DEFAULT_PAGE_SIZE = 1000


def fetch_already_watched(client_id):
    """Fetch the user's full Trakt watched library and return
    (watched_episode_tmdb_ids, watched_movie_tmdb_ids) as int sets.

    Returns (None, None) if the user isn't connected. Raises on API errors.
    Two HTTP requests total (one for shows, one for movies). The shows
    response is nested seasons -> episodes; we collect every episode tmdb id.
    """
    auth = load_authorization()
    if not auth:
        return None, None

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth['access_token']}",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
    }

    # Trakt's /sync/watched/shows doesn't expose per-episode TMDb ids, so
    # we dedupe using (show_tmdb, season_number, episode_number) triples.
    # Also collect a set of show_tmdb ids that have any watched episodes
    # at all, so we can dedupe show-level fallback entries.
    episode_triples = set()
    show_ids_with_any_watched = set()
    resp = requests.get(WATCHED_SHOWS_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    for show in resp.json() or []:
        show_tmdb = (show.get("show", {}).get("ids") or {}).get("tmdb")
        if not show_tmdb:
            continue
        any_eps = False
        for season in show.get("seasons") or []:
            sn = season.get("number")
            for ep in season.get("episodes") or []:
                en = ep.get("number")
                if sn is not None and en is not None:
                    episode_triples.add((int(show_tmdb), int(sn), int(en)))
                    any_eps = True
        if any_eps:
            show_ids_with_any_watched.add(int(show_tmdb))

    movie_ids = set()
    resp = requests.get(WATCHED_MOVIES_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    for entry in resp.json() or []:
        tmdb = (entry.get("movie", {}).get("ids") or {}).get("tmdb")
        if tmdb:
            movie_ids.add(int(tmdb))

    return {
        "episode_triples": episode_triples,
        "show_ids_with_any_watched": show_ids_with_any_watched,
        "movie_ids": movie_ids,
    }


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


def build_sync_payload(run_id, run_dir, already_watched=None):
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
    skipped_already_watched = {"movies": 0, "episodes": 0, "shows": 0}

    aw_movies = (already_watched or {}).get("movie_ids") or set()
    aw_eps = (already_watched or {}).get("episode_triples") or set()
    aw_shows = (already_watched or {}).get("show_ids_with_any_watched") or set()

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
        if tmdb_id in aw_movies:
            skipped_already_watched["movies"] += len(m.get("watched_at") or [])
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
                triple = None
                if ep.get("show_tmdb_id") and ep.get("season") is not None and ep.get("episode") is not None:
                    triple = (int(ep["show_tmdb_id"]), int(ep["season"]), int(ep["episode"]))
                if triple is not None and triple in aw_eps:
                    skipped_already_watched["episodes"] += 1
                    continue
                episodes.append(
                    {"watched_at": ep["watched_at"], "ids": {"tmdb": ep["tmdb_id"]}}
                )
        else:
            # Picked a different show than the one episodes were mapped to,
            # or no episode mapping was available. Send show-level entries
            # with each unique watched_at as a best-effort fallback.
            show_fallback += 1
            if picked in aw_shows:
                skipped_already_watched["shows"] += len(s.get("show_watches") or [])
                continue
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
        "skipped_already_watched": skipped_already_watched,
        "already_watched_checked": already_watched is not None,
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


def sync_to_trakt(run_id, run_dir, client_id, dry_run=True, page_size=DEFAULT_PAGE_SIZE, already_watched=None):
    """POST the built payload to Trakt /sync/history (chunked). Returns a
    dict describing what was sent / added.

    If `already_watched` is None and we have a Trakt connection, we fetch the
    user's watched library and dedupe against it. Pass an explicit value (incl.
    `{}`) to skip or override that lookup.
    """
    if already_watched is None:
        try:
            already_watched = fetch_already_watched(client_id)
        except Exception as e:
            logging.warning(f"Could not fetch Trakt watched library for dedup: {e}")
            already_watched = None

    payload, summary = build_sync_payload(
        run_id=run_id, run_dir=run_dir, already_watched=already_watched
    )

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
    total_not_found = {"movies": 0, "episodes": 0, "shows": 0}
    chunks_sent = 0
    last_response = None
    for chunk in _chunk_payload(payload, page_size):
        if not chunk:
            continue
        resp = requests.post(SYNC_URL, json=chunk, headers=headers, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        last_response = body
        added = body.get("added", {}) or {}
        not_found = body.get("not_found", {}) or {}
        for k in total_added:
            total_added[k] += int(added.get(k, 0) or 0)
            nf = not_found.get(k)
            if isinstance(nf, list):
                total_not_found[k] += len(nf)
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
        "not_found_counts": total_not_found,
        "trakt_response": {
            "added": total_added,
            "not_found_counts": total_not_found,
            "chunks_sent": chunks_sent,
            "last_chunk_response": last_response,
        },
    }
