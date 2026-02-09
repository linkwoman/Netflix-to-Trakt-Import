#!/usr/bin/env python3
import csv
import os
import sys

os.environ.setdefault("TMDB_MODE", "stub")

import config
config.TMDB_MODE = "stub"
config.TRAKT_API_DRY_RUN = True
config.VIEWING_HISTORY_FILENAME = os.path.join("fixtures", "sample_viewing_history.csv")

from netflix2trakt import (
    setup_logging,
    setupTMDB,
    setupTrakt,
    getNetflixHistory,
    getShowInformation,
    getMovieInformation,
    syncToTrakt,
    ReviewRouter,
    verify_accounting,
    generate_run_summary,
    count_review_reasons,
    compute_stub_sample_size,
    sample_entities,
    CONFIDENCE_AUTO_ACCEPT,
    CONFIDENCE_REVIEW,
    STUB_SAMPLE_CAP,
    STUB_SAMPLE_MULTIPLIER,
)
from tmdb_client import compute_confidence
from review_queue import generate_review_queue
from tqdm import tqdm
import datetime
import uuid


def main():
    run_id = "smoke_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    log_path = setup_logging(run_id)

    print("=" * 60)
    print("  Netflix2Trakt Smoke Test  (TMDB_MODE=stub, DRY_RUN=true)")
    print(f"  Run ID: {run_id}")
    print("=" * 60)

    client = setupTMDB("stub", None, config.TMDB_LANGUAGE, config.TMDB_DEBUG)

    traktIO = setupTrakt(config.TRAKT_API_SYNC_PAGE_SIZE, True)
    traktIO.dry_run = True

    netflixHistory, input_row_count = getNetflixHistory(
        config.VIEWING_HISTORY_FILENAME, config.CSV_DELIMITER
    )

    total_shows = len(netflixHistory.shows)
    total_movies = len(netflixHistory.movies)
    total_entities = total_shows + total_movies

    print(f"\nParsed {total_shows} shows and {total_movies} movies ({total_entities} entities) from {input_row_count} CSV rows")

    shows_to_process = netflixHistory.shows
    movies_to_process = netflixHistory.movies
    sample_size = compute_stub_sample_size()
    if total_entities > sample_size:
        shows_to_process, movies_to_process = sample_entities(
            netflixHistory.shows, netflixHistory.movies, sample_size
        )
        total_entities = len(shows_to_process) + len(movies_to_process)
        print(f"Stub sampling: {total_entities} entities sampled (cap={STUB_SAMPLE_CAP}, multiplier={STUB_SAMPLE_MULTIPLIER})")
    print()

    reviewRouter = ReviewRouter()

    for show in tqdm(shows_to_process, desc="Processing shows"):
        try:
            getShowInformation(
                show, client, config.TMDB_EPISODE_LANGUAGE_SEARCH, traktIO, reviewRouter
            )
        except Exception as e:
            reviewRouter.add_failure(show.name, "tv_show", str(e))

    for movie in tqdm(movies_to_process, desc="Processing movies"):
        try:
            getMovieInformation(movie, config.TMDB_SYNC_STRICT, client, traktIO, reviewRouter)
        except Exception as e:
            reviewRouter.add_failure(movie.name, "movie", str(e))

    syncToTrakt(traktIO)

    counts = reviewRouter.write_csvs()

    accounting_ok = verify_accounting(total_entities, counts)

    queue_count = generate_review_queue(client)

    review_reasons = count_review_reasons("review_queue.csv")

    total_before = total_shows + total_movies
    summary_path = generate_run_summary(
        run_id=run_id,
        input_file=config.VIEWING_HISTORY_FILENAME,
        input_row_count=input_row_count,
        tmdb_mode="stub",
        trakt_dry_run=True,
        counts=counts,
        queue_count=queue_count,
        review_reasons=review_reasons,
        log_path=log_path,
        total_entities_before_sample=total_before,
        sampled_entity_count=total_entities,
    )

    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)
    print(f"  Input rows:     {input_row_count}")
    print(f"  Entities:       {total_entities} (shows={total_shows}, movies={total_movies})")
    print(f"  Resolved:       {counts['resolved']}")
    print(f"  Needs Review:   {counts['needs_review']}")
    print(f"  Skipped:        {counts['skipped']}")
    print(f"  Failures:       {counts['failures']}")
    print(f"  Review Queue:   {queue_count}")
    if not accounting_ok:
        print(f"  WARNING: Accounting mismatch!")
    else:
        print(f"  Accounting:     OK")
    print(f"  Summary:        {summary_path}")
    print(f"  Log:            {log_path}")
    print()

    for csv_name in ["resolved.csv", "needs_review.csv", "skipped.csv", "failures.csv"]:
        if os.path.exists(csv_name):
            print(f"--- {csv_name} ---")
            with open(csv_name, "r") as f:
                print(f.read())

    if os.path.exists("review_queue.csv"):
        print(f"--- review_queue.csv ---")
        with open("review_queue.csv", "r") as f:
            print(f.read())

    if os.path.exists("run_summary.txt"):
        print(f"--- run_summary.txt ---")
        with open("run_summary.txt", "r") as f:
            print(f.read())

    print("Smoke test complete.")


if __name__ == "__main__":
    main()
