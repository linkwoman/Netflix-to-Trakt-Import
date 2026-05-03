#!/usr/bin/env python3

import csv
import datetime
import json
import logging
import os
import random
import re
import uuid
from time import sleep

from tenacity import retry, stop_after_attempt, wait_random
from tmdbv3api.exceptions import TMDbException
from tqdm import tqdm

import config
from NetflixTvShow import NetflixTvHistory
from TraktIO import TraktIO
from tmdb_client import create_tmdb_client, compute_confidence, compute_all_confidences
from review_queue import generate_review_queue


CONFIDENCE_AUTO_ACCEPT = 0.90
CONFIDENCE_REVIEW = 0.40


def setup_logging(run_id):
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"run_{run_id}.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_fmt)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_fmt = logging.Formatter("[%(levelname)s] %(message)s")
    console_handler.setFormatter(console_fmt)
    root_logger.addHandler(console_handler)

    return log_path


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
    row_count = 0
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
            row_count += 1
        logging.info(f"Processed {line_count} lines ({row_count} data rows).")

    return netflixHistory, row_count


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

        scored = compute_all_confidences(show.name, tmdbShow, media_type="show")
        if scored:
            best, conf, best_components = scored[0]
        else:
            conf, best, best_components = 0.0, None, {}

        if reviewRouter:
            top5 = scored[:5]
            candidate_ids = [str(c.get("id", "")) for c, _, _comp in top5]
            candidate_confidences = [s for _, s, _comp in top5]
            candidate_components = [comp for _, _, comp in top5]
            if conf >= CONFIDENCE_AUTO_ACCEPT:
                reviewRouter.add_resolved(
                    show.name, "tv_show", conf, best.get("id"), best.get("name", ""),
                    components=best_components,
                )
            elif conf >= CONFIDENCE_REVIEW:
                reviewRouter.add_needs_review(
                    show.name, "tv_show", conf, candidate_ids, candidate_confidences, best.get("name", ""),
                    candidate_components=candidate_components,
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
            scored = compute_all_confidences(movie.name, res, media_type="movie")
            if scored:
                best, conf, best_components = scored[0]
            else:
                conf, best, best_components = 0.0, None, {}

            if reviewRouter:
                top5 = scored[:5]
                candidate_ids = [str(c.get("id", "")) for c, _, _comp in top5]
                candidate_confidences = [s for _, s, _comp in top5]
                candidate_components = [comp for _, _, comp in top5]
                if conf >= CONFIDENCE_AUTO_ACCEPT:
                    reviewRouter.add_resolved(
                        movie.name, "movie", conf, best.get("id"), best.get("title", ""),
                        components=best_components,
                    )
                elif conf >= CONFIDENCE_REVIEW:
                    reviewRouter.add_needs_review(
                        movie.name, "movie", conf, candidate_ids, candidate_confidences, best.get("title", ""),
                        candidate_components=candidate_components,
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
    def __init__(self, output_dir=".", data_source="test"):
        self.output_dir = output_dir
        self.data_source = data_source
        self._resolved = []
        self._needs_review = []
        self._skipped = []
        self._failures = []
        self._next_id = 1

    def _assign_id(self):
        row_id = self._next_id
        self._next_id += 1
        return row_id

    def add_resolved(self, title, media_type, confidence, tmdb_id, matched_title, components=None):
        row = {
            "original_row_id": self._assign_id(),
            "title": title,
            "type": media_type,
            "confidence": confidence,
            "tmdb_id": tmdb_id,
            "matched_title": matched_title,
            "data_source": self.data_source,
        }
        if components:
            row["title_similarity"] = components.get("title_similarity", "")
            row["popularity"] = components.get("popularity", "")
            row["popularity_bonus"] = components.get("popularity_bonus", "")
            row["vote_count"] = components.get("vote_count", "")
            row["vote_count_bonus"] = components.get("vote_count_bonus", "")
        self._resolved.append(row)

    def add_needs_review(self, title, media_type, confidence, candidate_ids, candidate_confidences, best_candidate_title="", candidate_components=None):
        row = {
            "original_row_id": self._assign_id(),
            "title": title,
            "type": media_type,
            "confidence": confidence,
            "best_candidate_title": best_candidate_title,
            "candidate_ids": ";".join(candidate_ids),
            "candidate_confidences": ";".join(str(s) for s in candidate_confidences),
            "data_source": self.data_source,
        }
        if candidate_components:
            row["candidate_title_similarities"] = ";".join(str(comp.get("title_similarity", "")) for comp in candidate_components)
            row["candidate_popularities"] = ";".join(str(comp.get("popularity", "")) for comp in candidate_components)
            row["candidate_popularity_bonuses"] = ";".join(str(comp.get("popularity_bonus", "")) for comp in candidate_components)
            row["candidate_vote_counts"] = ";".join(str(comp.get("vote_count", "")) for comp in candidate_components)
            row["candidate_vote_count_bonuses"] = ";".join(str(comp.get("vote_count_bonus", "")) for comp in candidate_components)
        self._needs_review.append(row)

    def add_skipped(self, title, media_type, reason):
        self._skipped.append({
            "original_row_id": self._assign_id(),
            "title": title,
            "type": media_type,
            "reason": reason,
            "data_source": self.data_source,
        })

    def add_failure(self, title, media_type, error_msg):
        self._failures.append({
            "original_row_id": self._assign_id(),
            "title": title,
            "type": media_type,
            "error": error_msg,
            "data_source": self.data_source,
        })

    def write_csvs(self):
        resolved_path = os.path.join(self.output_dir, "resolved.csv")
        with open(resolved_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=[
                    "original_row_id", "title", "type", "confidence", "tmdb_id", "matched_title",
                    "title_similarity", "popularity", "popularity_bonus", "vote_count", "vote_count_bonus",
                    "data_source",
                ]
            )
            writer.writeheader()
            writer.writerows(self._resolved)

        review_path = os.path.join(self.output_dir, "needs_review.csv")
        with open(review_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=[
                    "original_row_id", "title", "type", "confidence", "best_candidate_title",
                    "candidate_ids", "candidate_confidences",
                    "candidate_title_similarities", "candidate_popularities", "candidate_popularity_bonuses",
                    "candidate_vote_counts", "candidate_vote_count_bonuses",
                    "data_source",
                ]
            )
            writer.writeheader()
            writer.writerows(self._needs_review)

        skipped_path = os.path.join(self.output_dir, "skipped.csv")
        with open(skipped_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["original_row_id", "title", "type", "reason", "data_source"])
            writer.writeheader()
            writer.writerows(self._skipped)

        failures_path = os.path.join(self.output_dir, "failures.csv")
        with open(failures_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["original_row_id", "title", "type", "error", "data_source"])
            writer.writeheader()
            writer.writerows(self._failures)

        return {
            "resolved": len(self._resolved),
            "needs_review": len(self._needs_review),
            "skipped": len(self._skipped),
            "failures": len(self._failures),
        }

    def summary(self):
        return {
            "resolved": len(self._resolved),
            "needs_review": len(self._needs_review),
            "skipped": len(self._skipped),
            "failures": len(self._failures),
        }

    def total_routed(self):
        return (
            len(self._resolved)
            + len(self._needs_review)
            + len(self._skipped)
            + len(self._failures)
        )


def verify_accounting(total_entities, counts):
    routed = counts["resolved"] + counts["needs_review"] + counts["skipped"] + counts["failures"]
    if total_entities != routed:
        logging.error(
            f"ACCOUNTING MISMATCH: {total_entities} entities processed but "
            f"{routed} routed (resolved={counts['resolved']}, "
            f"needs_review={counts['needs_review']}, skipped={counts['skipped']}, "
            f"failures={counts['failures']})"
        )
        return False
    logging.info(
        f"Accounting OK: {total_entities} entities == {routed} routed "
        f"(resolved={counts['resolved']}, needs_review={counts['needs_review']}, "
        f"skipped={counts['skipped']}, failures={counts['failures']})"
    )
    return True


def generate_run_summary(
    run_id, input_file, input_row_count, tmdb_mode, trakt_dry_run,
    counts, queue_count, review_reasons, log_path, output_dir=".",
    total_entities_before_sample=None, sampled_entity_count=None,
):
    resolved_n = counts["resolved"]
    needs_review_n = counts["needs_review"]
    skipped_n = counts["skipped"]
    failures_n = counts["failures"]

    paragraph = (
        f"Processed {input_row_count} Netflix viewing history rows from '{input_file}' "
        f"in {'stub' if tmdb_mode == 'stub' else 'real'} mode with "
        f"Trakt dry-run {'enabled' if trakt_dry_run else 'disabled'}. "
        f"The matcher auto-resolved {resolved_n} items at confidence >= 0.95, "
        f"flagged {needs_review_n} items for human review due to ambiguity or lower confidence, "
        f"and skipped {skipped_n} items with no viable TMDb candidates. "
        f"{failures_n} rows failed due to parsing or enrichment errors"
        f"{' and were written to failures.csv' if failures_n > 0 else ''}. "
        f"A consolidated review_queue.csv was generated with enriched metadata "
        f"for each candidate to support efficient manual review."
    )

    ambiguous_items = review_reasons.get("ambiguous_candidates", {}).get("items", 0)
    ambiguous_rows = review_reasons.get("ambiguous_candidates", {}).get("rows", 0)
    no_match_items = review_reasons.get("no_match", 0)
    low_conf_items = review_reasons.get("low_confidence_resolved", 0)

    sampling_line = None
    if total_entities_before_sample is not None and sampled_entity_count is not None:
        if sampled_entity_count < total_entities_before_sample:
            sampling_line = f"- Sampling: {sampled_entity_count} of {total_entities_before_sample} entities sampled (cap={STUB_SAMPLE_CAP}, multiplier={STUB_SAMPLE_MULTIPLIER})"

    lines = [
        paragraph,
        "",
        f"- Run ID: {run_id}",
        f"- Mode: TMDb={tmdb_mode}, Trakt dry_run={trakt_dry_run}",
        f"- Input: {input_file} ({input_row_count} rows)",
    ]
    if sampling_line:
        lines.append(sampling_line)
    lines += [
        "- Outputs:",
        f"  - resolved.csv: {resolved_n} rows (confidence >= 0.95)",
        f"  - needs_review.csv: {needs_review_n} rows",
        f"  - skipped.csv: {skipped_n} rows",
        f"  - failures.csv: {failures_n} rows",
        f"  - review_queue.csv: {queue_count} rows",
        "- Review reasons (from review_queue.csv):",
        f"  - ambiguous_candidates: {ambiguous_items} items / {ambiguous_rows} candidate rows",
        f"  - no_match: {no_match_items} items",
        f"  - low_confidence_resolved: {low_conf_items} items",
        "- Logs:",
        f"  - {log_path}",
        "- Next action:",
        "  - Open review_queue.csv and filter by review_reason to select tmdb_id per original_row_id.",
    ]

    summary_path = os.path.join(output_dir, "run_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    logging.info(f"Wrote run summary to {summary_path}")
    return summary_path


def count_review_reasons(review_queue_path):
    reasons = {
        "ambiguous_candidates": {"items": 0, "rows": 0},
        "no_match": 0,
        "low_confidence_resolved": 0,
    }
    if not os.path.exists(review_queue_path):
        return reasons

    seen_ambiguous_titles = set()
    with open(review_queue_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            reason = row.get("review_reason", "")
            if reason == "ambiguous_candidates":
                reasons["ambiguous_candidates"]["rows"] += 1
                title_key = row.get("input_title", "")
                if title_key not in seen_ambiguous_titles:
                    seen_ambiguous_titles.add(title_key)
                    reasons["ambiguous_candidates"]["items"] += 1
            elif reason == "no_match":
                reasons["no_match"] += 1
            elif reason == "low_confidence_resolved":
                reasons["low_confidence_resolved"] += 1
    return reasons


STUB_SAMPLE_MULTIPLIER = 3
STUB_SAMPLE_CAP = 50  # raise to 2000 if you want to test performance with a large input


def count_fixture_titles(fixtures_path=None):
    if fixtures_path is None:
        fixtures_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "fixtures", "tmdb_stub.json"
        )
    with open(fixtures_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    movie_count = len(data.get("movies", {}))
    show_count = len(data.get("shows", {}))
    return movie_count + show_count


def compute_stub_sample_size(fixtures_path=None):
    fixture_count = count_fixture_titles(fixtures_path)
    return min(fixture_count * STUB_SAMPLE_MULTIPLIER, STUB_SAMPLE_CAP)


def sample_entities(shows, movies, sample_size):
    all_entities = [("show", s) for s in shows] + [("movie", m) for m in movies]
    if len(all_entities) <= sample_size:
        return shows, movies
    sampled = random.sample(all_entities, sample_size)
    sampled_shows = [e for t, e in sampled if t == "show"]
    sampled_movies = [e for t, e in sampled if t == "movie"]
    return sampled_shows, sampled_movies


def main():
    run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    log_path = setup_logging(run_id)

    logging.info(f"=== Run {run_id} started ===")
    logging.info(f"Config: TMDB_MODE={config.TMDB_MODE}, TRAKT_DRY_RUN={config.TRAKT_API_DRY_RUN}")
    logging.info(f"Input: {config.VIEWING_HISTORY_FILENAME}")
    logging.info(f"Thresholds: AUTO_ACCEPT={CONFIDENCE_AUTO_ACCEPT}, REVIEW_FLOOR={CONFIDENCE_REVIEW}")

    client = setupTMDB(
        config.TMDB_MODE, config.TMDB_API_KEY, config.TMDB_LANGUAGE, config.TMDB_DEBUG
    )

    traktIO = setupTrakt(config.TRAKT_API_SYNC_PAGE_SIZE, config.TRAKT_API_DRY_RUN)

    if config.TMDB_MODE == "stub" or config.TRAKT_API_DRY_RUN:
        logging.info("Skipping Trakt authentication (stub/dry_run mode)")
        traktIO.dry_run = True
    else:
        traktIO.init()

    netflixHistory, input_row_count = getNetflixHistory(
        config.VIEWING_HISTORY_FILENAME, config.CSV_DELIMITER
    )

    total_shows = len(netflixHistory.shows)
    total_movies = len(netflixHistory.movies)
    total_entities = total_shows + total_movies
    logging.info(f"Parsed {total_shows} shows and {total_movies} movies ({total_entities} entities) from {input_row_count} CSV rows")

    shows_to_process = netflixHistory.shows
    movies_to_process = netflixHistory.movies
    sampled = False

    if config.TMDB_MODE == "stub":
        sample_size = compute_stub_sample_size()
        if total_entities > sample_size:
            shows_to_process, movies_to_process = sample_entities(
                netflixHistory.shows, netflixHistory.movies, sample_size
            )
            sampled = True
            total_entities = len(shows_to_process) + len(movies_to_process)
            logging.info(
                f"Stub mode: sampled {total_entities} entities (from {total_shows + total_movies} total) "
                f"using cap={STUB_SAMPLE_CAP}, multiplier={STUB_SAMPLE_MULTIPLIER}"
            )

    data_source = "test" if config.TMDB_MODE == "stub" else "live"
    reviewRouter = ReviewRouter(data_source=data_source)

    for show in tqdm(shows_to_process, desc="Finding and adding shows to Trakt.."):
        try:
            getShowInformation(
                show, client, config.TMDB_EPISODE_LANGUAGE_SEARCH, traktIO, reviewRouter
            )
        except Exception as e:
            logging.error(f"Failed to process show '{show.name}': {e}")
            reviewRouter.add_failure(show.name, "tv_show", str(e))

    for movie in tqdm(
        movies_to_process, desc="Finding and adding movies to Trakt.."
    ):
        try:
            getMovieInformation(movie, config.TMDB_SYNC_STRICT, client, traktIO, reviewRouter)
        except Exception as e:
            logging.error(f"Failed to process movie '{movie.name}': {e}")
            reviewRouter.add_failure(movie.name, "movie", str(e))

    syncToTrakt(traktIO)

    counts = reviewRouter.write_csvs()
    logging.info(f"Wrote routing CSVs: {counts}")

    accounting_ok = verify_accounting(total_entities, counts)

    queue_count = generate_review_queue(client)
    logging.info(f"Review queue: {queue_count} rows")

    review_reasons = count_review_reasons("review_queue.csv")

    total_before = total_shows + total_movies
    summary_path = generate_run_summary(
        run_id=run_id,
        input_file=config.VIEWING_HISTORY_FILENAME,
        input_row_count=input_row_count,
        tmdb_mode=config.TMDB_MODE,
        trakt_dry_run=config.TRAKT_API_DRY_RUN,
        counts=counts,
        queue_count=queue_count,
        review_reasons=review_reasons,
        log_path=log_path,
        total_entities_before_sample=total_before,
        sampled_entity_count=total_entities,
    )

    logging.info(f"=== Run {run_id} completed ===")

    print(f"\n=== Pipeline Complete (run {run_id}) ===")
    print(f"  Input rows:     {input_row_count}")
    print(f"  Entities:       {total_entities} (shows={total_shows}, movies={total_movies})")
    print(f"  Resolved:       {counts['resolved']}")
    print(f"  Needs Review:   {counts['needs_review']}")
    print(f"  Skipped:        {counts['skipped']}")
    print(f"  Failures:       {counts['failures']}")
    print(f"  Review Queue:   {queue_count}")
    if not accounting_ok:
        print(f"  WARNING: Accounting mismatch detected! Check {log_path}")
    print(f"  Summary:        {summary_path}")
    print(f"  Log:            {log_path}")
    print(f"  Outputs: resolved.csv, needs_review.csv, skipped.csv, failures.csv, review_queue.csv")


if __name__ == "__main__":
    main()
