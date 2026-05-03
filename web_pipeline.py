"""Pipeline wrapper for the web interface.

Reuses the functions in netflix2trakt.py but runs them with explicit overrides
(mode, dry_run, input file) and a progress callback so the web UI can show
live status. Outputs the same CSV files as the CLI plus a snapshot under
runs/<run_id>/ for history.
"""

import csv
import datetime
import json
import logging
import os
import shutil
import uuid

import config
from NetflixTvShow import NetflixTvHistory
from TraktIO import TraktIO
from tmdb_client import create_tmdb_client
from review_queue import generate_review_queue
import netflix2trakt as pipeline


RUNS_DIR = "runs"
OUTPUT_FILES = [
    "resolved.csv",
    "needs_review.csv",
    "skipped.csv",
    "failures.csv",
    "review_queue.csv",
    "run_summary.txt",
]


def make_run_id(prefix="web"):
    return f"{prefix}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def parse_csv(input_path, delimiter=","):
    """Parse a Netflix viewing history CSV. Returns (NetflixTvHistory, row_count)."""
    history = NetflixTvHistory()
    row_count = 0
    with open(input_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=("Title", "Date"), delimiter=delimiter)
        line_count = 0
        for row in reader:
            if line_count == 0:
                line_count += 1
                continue
            history.addEntry(row["Title"], row["Date"])
            line_count += 1
            row_count += 1
    return history, row_count


def run_pipeline(
    input_csv_path,
    mode,
    run_id=None,
    progress_callback=None,
    tmdb_api_key=None,
    tmdb_language="en",
):
    """Run the matching pipeline (parse + match + route + review queue).

    Does NOT sync to Trakt; that's a separate step the user triggers explicitly.

    Args:
        input_csv_path: Path to the uploaded Netflix CSV.
        mode: "stub" or "real".
        run_id: Optional run id. Generated if not provided.
        progress_callback: callable(stage: str, current: int, total: int, message: str)
        tmdb_api_key: API key for real mode.
        tmdb_language: TMDb language code.

    Returns:
        dict with run_id, summary_path, counts, queue_count, snapshot_dir.
    """
    if run_id is None:
        run_id = make_run_id()

    log_path = pipeline.setup_logging(run_id)
    logging.info(f"=== Web run {run_id} started (mode={mode}) ===")

    def report(stage, current, total, message=""):
        if progress_callback:
            try:
                progress_callback(stage, current, total, message)
            except Exception:
                pass

    report("starting", 0, 1, "Initializing")

    # TMDb client
    client = create_tmdb_client(
        mode=mode,
        api_key=tmdb_api_key or config.TMDB_API_KEY,
        language=tmdb_language,
        debug=False,
    )

    # TraktIO is needed by the pipeline functions for accumulation, but we
    # always run it as dry_run here. Real sync happens later via web_sync.py.
    trakt_io = TraktIO(page_size=10000, dry_run=True)

    # Parse CSV
    report("parsing", 0, 1, "Parsing Netflix CSV")
    history, input_row_count = parse_csv(input_csv_path, config.CSV_DELIMITER)
    total_shows = len(history.shows)
    total_movies = len(history.movies)
    total_entities = total_shows + total_movies
    report("parsing", 1, 1, f"Parsed {input_row_count} rows, {total_entities} entities")

    # Stub-mode sampling
    shows_to_process = history.shows
    movies_to_process = history.movies
    if mode == "stub":
        sample_size = pipeline.compute_stub_sample_size()
        if total_entities > sample_size:
            shows_to_process, movies_to_process = pipeline.sample_entities(
                history.shows, history.movies, sample_size
            )
            sampled_count = len(shows_to_process) + len(movies_to_process)
            logging.info(f"Stub mode sampling: {sampled_count} of {total_entities} entities")
            total_entities_actual = sampled_count
        else:
            total_entities_actual = total_entities
    else:
        total_entities_actual = total_entities

    data_source = "test" if mode == "stub" else "live"
    review_router = pipeline.ReviewRouter(data_source=data_source)

    # sync_data captures per-entity TMDb mapping + watched_at timestamps so
    # the web sync can push episode-level data with the original watch dates,
    # matching what the CLI does. We collect this alongside the routing CSVs.
    sync_data = {"shows": [], "movies": []}

    def _last_assigned_row(router):
        """Return (row_dict, status) for whichever bucket got the most recent
        _assign_id() call, by comparing the highest original_row_id across
        all four buckets. Returns (None, None) if no row was added."""
        best_row, best_status, best_id = None, None, -1
        for bucket, status in (
            (router._resolved, "resolved"),
            (router._needs_review, "needs_review"),
            (router._skipped, "skipped"),
            (router._failures, "failures"),
        ):
            for r in bucket:
                rid = int(r.get("original_row_id") or 0)
                if rid > best_id:
                    best_id, best_row, best_status = rid, r, status
        return best_row, best_status

    # Process shows
    total_steps = len(shows_to_process) + len(movies_to_process)
    step = 0
    for show in shows_to_process:
        step += 1
        report("matching", step, total_steps, f"Matching show: {show.name}")
        before_row, _ = _last_assigned_row(review_router)
        before_id = int(before_row["original_row_id"]) if before_row else 0
        try:
            pipeline.getShowInformation(show, client, False, trakt_io, review_router)
        except Exception as e:
            logging.error(f"Failed to process show '{show.name}': {e}")
            review_router.add_failure(show.name, "tv_show", str(e))

        # Collect sync data for this show if it landed in resolved/needs_review.
        row, status = _last_assigned_row(review_router)
        if row and int(row["original_row_id"]) > before_id and status in ("resolved", "needs_review"):
            if status == "resolved":
                auto_tmdb_id = int(row["tmdb_id"]) if row.get("tmdb_id") else None
            else:
                ids = (row.get("candidate_ids") or "").split(";")
                auto_tmdb_id = int(ids[0]) if ids and ids[0].strip() else None

            episodes = []
            all_watches = set()
            for season in show.seasons:
                for ep in season.episodes:
                    for w in ep.watchedAt:
                        all_watches.add(w)
                    if ep.tmdbId is not None:
                        for w in ep.watchedAt:
                            episodes.append({"tmdb_id": ep.tmdbId, "watched_at": w})

            sync_data["shows"].append({
                "original_row_id": row["original_row_id"],
                "name": show.name,
                "auto_tmdb_id": auto_tmdb_id,
                "auto_resolved": status == "resolved",
                "episodes": episodes,
                "show_watches": sorted(all_watches),
            })

    for movie in movies_to_process:
        step += 1
        report("matching", step, total_steps, f"Matching movie: {movie.name}")
        before_row, _ = _last_assigned_row(review_router)
        before_id = int(before_row["original_row_id"]) if before_row else 0
        try:
            pipeline.getMovieInformation(movie, False, client, trakt_io, review_router)
        except Exception as e:
            logging.error(f"Failed to process movie '{movie.name}': {e}")
            review_router.add_failure(movie.name, "movie", str(e))

        row, status = _last_assigned_row(review_router)
        if row and int(row["original_row_id"]) > before_id and status in ("resolved", "needs_review"):
            if status == "resolved":
                auto_tmdb_id = int(row["tmdb_id"]) if row.get("tmdb_id") else None
            else:
                ids = (row.get("candidate_ids") or "").split(";")
                auto_tmdb_id = int(ids[0]) if ids and ids[0].strip() else None
            sync_data["movies"].append({
                "original_row_id": row["original_row_id"],
                "name": movie.name,
                "auto_tmdb_id": auto_tmdb_id,
                "auto_resolved": status == "resolved",
                "watched_at": sorted(set(movie.watchedAt)),
            })

    # Write routing CSVs
    report("routing", 1, 1, "Writing routing CSVs")
    counts = review_router.write_csvs()

    accounting_ok = pipeline.verify_accounting(total_entities_actual, counts)
    if not accounting_ok:
        logging.warning("Accounting check failed!")

    # Review queue (with TMDb enrichment)
    report("enriching", 1, 1, "Building review queue with TMDb metadata")
    queue_count = generate_review_queue(client)

    review_reasons = pipeline.count_review_reasons("review_queue.csv")

    summary_path = pipeline.generate_run_summary(
        run_id=run_id,
        input_file=os.path.basename(input_csv_path),
        input_row_count=input_row_count,
        tmdb_mode=mode,
        trakt_dry_run=True,
        counts=counts,
        queue_count=queue_count,
        review_reasons=review_reasons,
        log_path=log_path,
        total_entities_before_sample=total_shows + total_movies,
        sampled_entity_count=total_entities_actual,
    )

    # Snapshot to runs/<run_id>/
    snapshot_dir = os.path.join(RUNS_DIR, run_id)
    os.makedirs(snapshot_dir, exist_ok=True)
    for name in OUTPUT_FILES:
        if os.path.exists(name):
            shutil.copy2(name, os.path.join(snapshot_dir, name))

    # Persist sync_data.json so web_sync can rebuild the Trakt payload with
    # episode-level mappings and watched_at timestamps.
    with open(os.path.join(snapshot_dir, "sync_data.json"), "w") as f:
        json.dump(sync_data, f, indent=2)

    # Save run metadata
    metadata = {
        "run_id": run_id,
        "created_at": datetime.datetime.now().isoformat(),
        "mode": mode,
        "input_file": os.path.basename(input_csv_path),
        "input_row_count": input_row_count,
        "counts": counts,
        "queue_count": queue_count,
        "total_entities": total_entities_actual,
    }
    with open(os.path.join(snapshot_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    report("done", 1, 1, "Pipeline complete")
    logging.info(f"=== Web run {run_id} completed ===")

    return {
        "run_id": run_id,
        "summary_path": summary_path,
        "counts": counts,
        "queue_count": queue_count,
        "snapshot_dir": snapshot_dir,
        "metadata": metadata,
    }


def list_past_runs():
    """Return a list of past run metadata, newest first."""
    if not os.path.exists(RUNS_DIR):
        return []
    runs = []
    for run_id in os.listdir(RUNS_DIR):
        meta_path = os.path.join(RUNS_DIR, run_id, "metadata.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path) as f:
                    runs.append(json.load(f))
            except Exception:
                continue
    runs.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return runs
