#!/usr/bin/env python3

import csv
import logging
import os
import re
from time import sleep

from tenacity import retry, stop_after_attempt, wait_random
from tmdbv3api.exceptions import TMDbException
from tqdm import tqdm

import config
from NetflixTvShow import NetflixTvHistory
from TraktIO import TraktIO
from tmdb_client import create_tmdb_client, compute_confidence
from review_queue import generate_review_queue


CONFIDENCE_AUTO_ACCEPT = 0.80
CONFIDENCE_REVIEW = 0.40


def setupTMDB(tmdbMode, tmdbKey, tmdbLanguage, tmdbDebug):
    return create_tmdb_client(
        mode=tmdbMode,
        api_key=tmdbKey,
        language=tmdbLanguage,
        debug=tmdbDebug,
    )


def setupTrakt(traktPageSize, traktDryRun):
    traktIO = TraktIO(page_size=traktPageSize, dry_run=traktDryRun)
    return traktIO


def getNetflixHistory(inputFile, inputFileDelimiter):
    netflixHistory = NetflixTvHistory()
    with open(inputFile, mode="r", encoding="utf-8") as csvFile:
        csvReader = csv.DictReader(
            csvFile, fieldnames=("Title", "Date"), delimiter=inputFileDelimiter
        )
        line_count = 0
        for row in csvReader:
            if line_count == 0:
                line_count += 1
                continue

            entry = row["Title"]
            watchedAt = row["Date"]

            logging.debug("Parsed CSV file entry: {} : {}".format(watchedAt, entry))

            netflixHistory.addEntry(entry, watchedAt)

            line_count += 1
        logging.info(f"Processed {line_count} lines.")

    return netflixHistory


@retry(stop=stop_after_attempt(5), wait=wait_random(min=2, max=10))
def getShowInformation(show, client, languageSearch, traktIO, reviewRouter=None):
    tmdbShow = None
    try:
        if len(show.name.strip()) != 0:
            tmdbShow = client.search_tv(show.name)
        if tmdbShow is None or len(tmdbShow) == 0:
            logging.warning("Show %s not found on TMDB!" % show.name)
            if reviewRouter:
                reviewRouter.add_skipped(show.name, "tv_show", "No candidates found")
            return

        conf, best = compute_confidence(show.name, tmdbShow, media_type="show")

        if reviewRouter:
            candidate_ids = [str(c.get("id", "")) for c in tmdbShow[:5]]
            if conf >= CONFIDENCE_AUTO_ACCEPT:
                reviewRouter.add_resolved(
                    show.name, "tv_show", conf, best.get("id"), best.get("name", "")
                )
            elif conf >= CONFIDENCE_REVIEW:
                reviewRouter.add_needs_review(
                    show.name, "tv_show", conf, candidate_ids
                )
            else:
                reviewRouter.add_skipped(
                    show.name, "tv_show", f"Low confidence: {conf}"
                )
                return

        showId = best.get("id", tmdbShow[0]["id"])
        details = client.tv_details(show_id=showId, append_to_response="")
        numSeasons = details.number_of_seasons

        for season in show.seasons:
            if season.number is None and season.name is None:
                continue

            if season.number is None and season.name is not None:
                for i in range(1, numSeasons + 1):
                    logging.debug(
                        "Requesting show %s (id %s) season %d / %d\n"
                        % (show.name, showId, int(i), int(numSeasons))
                    )
                    tmp = client.season_details(
                        tv_id=showId, season_num=i, append_to_response="translations"
                    )
                    sleep(0.1)
                    if tmp.name == season.name:
                        season.number = tmp.season_number
                        break
                if season.number is None:
                    logging.info(
                        "No season number found for %s : %s" % (show.name, season.name)
                    )
                    continue

            if season.number is not None:
                logging.debug(showId)
                if int(season.number) > numSeasons:
                    season.number = numSeasons

                try:
                    tmdbResult = client.season_details(
                        tv_id=showId,
                        season_num=season.number,
                        append_to_response="translations",
                    )
                except (TMDbException, Exception) as err:
                    logging.error(
                        f"\nUnexpected error when searching for the season number of the show {show.name} "
                        f'by the season name "{season.name}", error at search for season {season.number}: {err}. \n'
                        "The entry will be skipped\n"
                    )
                    continue

                if languageSearch:
                    logging.info(
                        "Searching each episode individually for season %d of %s"
                        % (int(season.number), show.name)
                    )
                    for tmdbEpisode in tmdbResult.episodes:
                        try:
                            epInfo = client.episode_details(
                                tv_id=showId,
                                season_num=season.number,
                                episode_num=tmdbEpisode.episode_number,
                                append_to_response="translations",
                            )
                        except (TMDbException, Exception) as err:
                            logging.error(f"Error: {err}")
                            continue
                        for epTranslation in epInfo.translations.translations:
                            if epTranslation.iso_639_1 == client.language:
                                tmdbEpisode.name = epTranslation.data.name
                        sleep(0.1)
                count = 0
                for episode in season.episodes:
                    found = False
                    for tmdbEpisode in tmdbResult.episodes:
                        logging.debug(tmdbEpisode.name)
                        if tmdbEpisode.name == episode.name:
                            episode.setTmdbId(tmdbEpisode.id)
                            episode.setEpisodeNumber(tmdbEpisode.episode_number)
                            found = True
                            count += 1
                            break
                    if not (found):
                        tvshowregex = re.compile(r"(?:Folge|Episode) (\d{1,2})")
                        res = tvshowregex.search(episode.name)
                        if res is not None:
                            number = int(res.group(1))
                            if number <= len(tmdbResult.episodes):
                                episode.setEpisodeNumber(number)
                                for tmdbEpisode in tmdbResult.episodes:
                                    if tmdbEpisode.episode_number == number:
                                        episode.setTmdbId(tmdbEpisode.id)
                                        count += 1
                                        found = True
                                        break

                if len(tmdbResult.episodes) == len(season.episodes):
                    lastEpisodeNumber = len(season.episodes)
                    for episode in season.episodes:
                        if episode.tmdbId is not None:
                            lastEpisodeNumber -= 1
                            continue
                        for tmdbEpisode in tmdbResult.episodes:
                            if tmdbEpisode.episode_number == lastEpisodeNumber:
                                episode.setTmdbId(tmdbEpisode.id)
                                episode.setEpisodeNumber(tmdbEpisode.episode_number)
                                lastEpisodeNumber -= 1
                                break

                for episode in season.episodes:
                    if episode.tmdbId is None:
                        logging.info(
                            "No Tmdb ID found for %s : Season %d: %s"
                            % (show.name, int(season.number), episode.name)
                        )
                        break

        addShowToTrakt(show, traktIO)

    except TMDbException as err:
        logging.error(f"Could not add the following show to Trakt {show.name}: {err}")
    except IndexError as err:
        logging.error(f"TMDB does not contain show {show.name}: {err}")


def getMovieInformation(movie, strictSync, client, traktIO, reviewRouter=None):
    try:
        res = client.search_movie(movie.name)

        if res:
            conf, best = compute_confidence(movie.name, res, media_type="movie")

            if reviewRouter:
                candidate_ids = [str(c.get("id", "")) for c in res[:5]]
                if conf >= CONFIDENCE_AUTO_ACCEPT:
                    reviewRouter.add_resolved(
                        movie.name, "movie", conf, best.get("id"), best.get("title", "")
                    )
                elif conf >= CONFIDENCE_REVIEW:
                    reviewRouter.add_needs_review(
                        movie.name, "movie", conf, candidate_ids
                    )
                else:
                    reviewRouter.add_skipped(
                        movie.name, "movie", f"Low confidence: {conf}"
                    )
                    return

            movie.tmdbId = best.get("id", res[0]["id"])
            matched_title = best.get("title", res[0].get("title", ""))
            logging.info(
                "Found movie %s : %s (%d)" % (movie.name, matched_title, movie.tmdbId)
            )
            return addMovieToTrakt(movie, traktIO)

        else:
            logging.info("Movie not found: %s" % movie.name)
            if reviewRouter:
                reviewRouter.add_skipped(movie.name, "movie", "No candidates found")
    except TMDbException:
        if strictSync is True:
            raise
        else:
            logging.info(
                "Ignoring appeared exception while looking for movie %s" % movie.name
            )


@retry(stop=stop_after_attempt(5), wait=wait_random(min=2, max=10))
def addShowToTrakt(show, traktIO):
    for season in show.seasons:
        logging.info(
            f"Adding episodes to trakt: {len(season.episodes)} episodes from {show.name} season {season.number}"
        )
        for episode in season.episodes:
            if episode.tmdbId is not None:
                for watchedTime in episode.watchedAt:
                    episodeData = {
                        "watched_at": watchedTime,
                        "ids": {"tmdb": episode.tmdbId},
                    }
                    traktIO.addEpisodeToHistory(episodeData)


@retry(stop=stop_after_attempt(5), wait=wait_random(min=2, max=10))
def addMovieToTrakt(movie, traktIO):
    if movie.tmdbId is not None:
        for watchedTime in movie.watchedAt:
            logging.info("Adding movie to trakt: %s" % movie.name)
            movieData = {
                "title": movie.name,
                "watched_at": watchedTime,
                "ids": {"tmdb": movie.tmdbId},
            }
            traktIO.addMovie(movieData)
            return traktIO


@retry(stop=stop_after_attempt(5), wait=wait_random(min=2, max=10))
def syncToTrakt(traktIO):
    try:
        traktIO.sync()
    except Exception:
        pass


class ReviewRouter:
    def __init__(self, output_dir="."):
        self.output_dir = output_dir
        self._resolved = []
        self._needs_review = []
        self._skipped = []

    def add_resolved(self, title, media_type, confidence, tmdb_id, matched_title):
        self._resolved.append({
            "title": title,
            "type": media_type,
            "confidence": confidence,
            "tmdb_id": tmdb_id,
            "matched_title": matched_title,
        })

    def add_needs_review(self, title, media_type, confidence, candidate_ids):
        self._needs_review.append({
            "title": title,
            "type": media_type,
            "confidence": confidence,
            "candidate_ids": ";".join(candidate_ids),
        })

    def add_skipped(self, title, media_type, reason):
        self._skipped.append({
            "title": title,
            "type": media_type,
            "reason": reason,
        })

    def write_csvs(self):
        resolved_path = os.path.join(self.output_dir, "resolved.csv")
        with open(resolved_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["title", "type", "confidence", "tmdb_id", "matched_title"]
            )
            writer.writeheader()
            writer.writerows(self._resolved)

        review_path = os.path.join(self.output_dir, "needs_review.csv")
        with open(review_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["title", "type", "confidence", "candidate_ids"]
            )
            writer.writeheader()
            writer.writerows(self._needs_review)

        skipped_path = os.path.join(self.output_dir, "skipped.csv")
        with open(skipped_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["title", "type", "reason"])
            writer.writeheader()
            writer.writerows(self._skipped)

        return {
            "resolved": len(self._resolved),
            "needs_review": len(self._needs_review),
            "skipped": len(self._skipped),
        }

    def summary(self):
        return {
            "resolved": len(self._resolved),
            "needs_review": len(self._needs_review),
            "skipped": len(self._skipped),
        }


def main():
    logging.basicConfig(filename=config.LOG_FILENAME, level=config.LOG_LEVEL)

    client = setupTMDB(
        config.TMDB_MODE, config.TMDB_API_KEY, config.TMDB_LANGUAGE, config.TMDB_DEBUG
    )

    traktIO = setupTrakt(config.TRAKT_API_SYNC_PAGE_SIZE, config.TRAKT_API_DRY_RUN)

    if config.TMDB_MODE == "stub" or config.TRAKT_API_DRY_RUN:
        logging.info("Skipping Trakt authentication (stub/dry_run mode)")
        traktIO.dry_run = True
    else:
        traktIO.init()

    netflixHistory = getNetflixHistory(
        config.VIEWING_HISTORY_FILENAME, config.CSV_DELIMITER
    )

    reviewRouter = ReviewRouter()

    for show in tqdm(netflixHistory.shows, desc="Finding and adding shows to Trakt.."):
        getShowInformation(
            show, client, config.TMDB_EPISODE_LANGUAGE_SEARCH, traktIO, reviewRouter
        )

    for movie in tqdm(
        netflixHistory.movies, desc="Finding and adding movies to Trakt.."
    ):
        getMovieInformation(movie, config.TMDB_SYNC_STRICT, client, traktIO, reviewRouter)

    syncToTrakt(traktIO)

    counts = reviewRouter.write_csvs()

    queue_count = generate_review_queue(client)

    print(f"\n=== Pipeline Summary ===")
    print(f"  Resolved (auto-accepted): {counts['resolved']}")
    print(f"  Needs Review (ambiguous):  {counts['needs_review']}")
    print(f"  Skipped (no/low match):    {counts['skipped']}")
    print(f"  Review Queue:              {queue_count}")
    print(f"  Output files: resolved.csv, needs_review.csv, skipped.csv, review_queue.csv")


if __name__ == "__main__":
    main()
