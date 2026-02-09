#!/usr/bin/env python3
import csv
import os
import shutil
import sys

os.environ.setdefault("TMDB_MODE", "stub")

import config
config.TMDB_MODE = "stub"
config.TRAKT_API_DRY_RUN = True
config.VIEWING_HISTORY_FILENAME = os.path.join("fixtures", "sample_viewing_history.csv")

from netflix2trakt import (
    setupTMDB,
    setupTrakt,
    getNetflixHistory,
    getShowInformation,
    getMovieInformation,
    syncToTrakt,
    ReviewRouter,
)
from tmdb_client import compute_confidence
from review_queue import generate_review_queue
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)


def main():
    print("=" * 60)
    print("  Netflix2Trakt Smoke Test  (TMDB_MODE=stub, DRY_RUN=true)")
    print("=" * 60)

    client = setupTMDB("stub", None, config.TMDB_LANGUAGE, config.TMDB_DEBUG)

    traktIO = setupTrakt(config.TRAKT_API_SYNC_PAGE_SIZE, True)
    traktIO.dry_run = True

    netflixHistory = getNetflixHistory(
        config.VIEWING_HISTORY_FILENAME, config.CSV_DELIMITER
    )

    print(f"\nParsed {len(netflixHistory.shows)} shows and {len(netflixHistory.movies)} movies from CSV\n")

    reviewRouter = ReviewRouter()

    for show in tqdm(netflixHistory.shows, desc="Processing shows"):
        getShowInformation(
            show, client, config.TMDB_EPISODE_LANGUAGE_SEARCH, traktIO, reviewRouter
        )

    for movie in tqdm(netflixHistory.movies, desc="Processing movies"):
        getMovieInformation(movie, config.TMDB_SYNC_STRICT, client, traktIO, reviewRouter)

    syncToTrakt(traktIO)

    counts = reviewRouter.write_csvs()

    queue_count = generate_review_queue(client)

    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)
    print(f"  Resolved (auto-accepted): {counts['resolved']}")
    print(f"  Needs Review (ambiguous):  {counts['needs_review']}")
    print(f"  Skipped (no/low match):    {counts['skipped']}")
    print(f"  Review Queue:              {queue_count}")
    print()

    for csv_name in ["resolved.csv", "needs_review.csv", "skipped.csv"]:
        if os.path.exists(csv_name):
            print(f"--- {csv_name} ---")
            with open(csv_name, "r") as f:
                print(f.read())

    if os.path.exists("review_queue.csv"):
        print(f"--- review_queue.csv ---")
        with open("review_queue.csv", "r") as f:
            print(f.read())

    print("Smoke test complete.")


if __name__ == "__main__":
    main()
