import hashlib
import json
import logging
import os
import re
from abc import ABC, abstractmethod
from time import sleep

from difflib import SequenceMatcher


class AttrDict(dict):
    def __getattr__(self, key):
        try:
            val = self[key]
        except KeyError:
            raise AttributeError(key)
        if isinstance(val, dict) and not isinstance(val, AttrDict):
            val = AttrDict(val)
            self[key] = val
        if isinstance(val, list):
            val = [AttrDict(v) if isinstance(v, dict) else v for v in val]
            self[key] = val
        return val


class TMDbClientBase(ABC):
    @abstractmethod
    def search_movie(self, query):
        pass

    @abstractmethod
    def search_tv(self, query):
        pass

    @abstractmethod
    def tv_details(self, show_id, append_to_response=""):
        pass

    @abstractmethod
    def season_details(self, tv_id, season_num, append_to_response=""):
        pass

    @abstractmethod
    def episode_details(self, tv_id, season_num, episode_num, append_to_response=""):
        pass


class RealTMDbClient(TMDbClientBase):
    def __init__(self, api_key, language="en", debug=False):
        from tmdbv3api import TV, Episode, Movie, Season, TMDb

        self._tmdb = TMDb()
        self._tmdb.api_key = api_key
        self._tmdb.language = language
        self._tmdb.debug = debug
        self._tv = TV()
        self._movie = Movie()
        self._season = Season()
        self._episode = Episode()

    @property
    def language(self):
        return self._tmdb.language

    def search_movie(self, query):
        return self._movie.search(query)

    def search_tv(self, query):
        return self._tv.search(query)

    def tv_details(self, show_id, append_to_response=""):
        return self._tv.details(show_id=show_id, append_to_response=append_to_response)

    def season_details(self, tv_id, season_num, append_to_response=""):
        return self._season.details(
            tv_id=tv_id, season_num=season_num, append_to_response=append_to_response
        )

    def episode_details(self, tv_id, season_num, episode_num, append_to_response=""):
        return self._episode.details(
            tv_id=tv_id,
            season_num=season_num,
            episode_num=episode_num,
            append_to_response=append_to_response,
        )


class StubTMDbClient(TMDbClientBase):
    def __init__(self, fixtures_path=None, language="en"):
        if fixtures_path is None:
            fixtures_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "fixtures", "tmdb_stub.json"
            )
        with open(fixtures_path, "r", encoding="utf-8") as f:
            self._fixtures = json.load(f)
        self._language = language

    @property
    def language(self):
        return self._language

    def _normalize_query(self, query):
        return query.strip().lower()

    def _generate_heuristic_candidates(self, query, media_type="movie"):
        seed = int(hashlib.md5(query.encode()).hexdigest()[:8], 16)
        count = seed % 4
        if count == 0:
            return []
        candidates = []
        for i in range(count):
            cid = (seed + i * 1000) % 999999
            pop = round(10 + (seed % 80) + i * 3.5, 1)
            votes = 50 + (seed % 5000)
            prefixes = ["The Story of ", "About ", "Tales of ", "Beyond ", "Inside "]
            prefix = prefixes[i % len(prefixes)]
            candidate_title = f"{prefix}{query.title()}"
            if media_type == "movie":
                candidates.append({
                    "id": cid,
                    "title": candidate_title,
                    "release_date": f"{2015 + (seed % 10)}-{1 + (seed % 12):02d}-{1 + (seed % 28):02d}",
                    "overview": f"A film about {query}.",
                    "popularity": pop,
                    "vote_count": votes,
                })
            else:
                candidates.append({
                    "id": cid,
                    "name": candidate_title,
                    "first_air_date": f"{2015 + (seed % 10)}-{1 + (seed % 12):02d}-{1 + (seed % 28):02d}",
                    "overview": f"A show about {query}.",
                    "popularity": pop,
                    "vote_count": votes,
                })
        return candidates

    def _generate_heuristic_show_details(self, show_id):
        return AttrDict({
            "id": show_id,
            "name": f"Show {show_id}",
            "number_of_seasons": 2,
        })

    def _generate_heuristic_season(self, tv_id, season_num):
        episodes = []
        for ep_num in range(1, 9):
            episodes.append({
                "id": tv_id * 100 + season_num * 10 + ep_num,
                "episode_number": ep_num,
                "name": f"Episode {ep_num}",
                "air_date": f"2020-01-{ep_num:02d}",
            })
        return AttrDict({
            "name": f"Season {season_num}",
            "season_number": season_num,
            "episodes": episodes,
        })

    def search_movie(self, query):
        norm = self._normalize_query(query)
        results = self._fixtures.get("movies", {}).get(norm, None)
        if results is not None:
            return results
        return self._generate_heuristic_candidates(norm, "movie")

    def search_tv(self, query):
        norm = self._normalize_query(query)
        results = self._fixtures.get("shows", {}).get(norm, None)
        if results is not None:
            return results
        return self._generate_heuristic_candidates(norm, "show")

    def tv_details(self, show_id, append_to_response=""):
        details = self._fixtures.get("show_details", {}).get(str(show_id), None)
        if details is not None:
            return AttrDict(details)
        return self._generate_heuristic_show_details(show_id)

    def season_details(self, tv_id, season_num, append_to_response=""):
        key = f"{tv_id}_{season_num}"
        season_data = self._fixtures.get("seasons", {}).get(key, None)
        if season_data is not None:
            return AttrDict(season_data)
        return self._generate_heuristic_season(tv_id, season_num)

    def episode_details(self, tv_id, season_num, episode_num, append_to_response=""):
        key = f"{tv_id}_{season_num}"
        season_data = self._fixtures.get("seasons", {}).get(key, None)
        if season_data is not None:
            for ep in season_data.get("episodes", []):
                if ep["episode_number"] == episode_num:
                    result = AttrDict(ep)
                    result["translations"] = AttrDict({"translations": []})
                    return result
        result = AttrDict({
            "id": tv_id * 100 + season_num * 10 + episode_num,
            "episode_number": episode_num,
            "name": f"Episode {episode_num}",
            "air_date": f"2020-01-{episode_num:02d}",
            "translations": AttrDict({"translations": []}),
        })
        return result


def compute_confidence(query, candidates, media_type="movie"):
    if not candidates:
        return 0.0, None

    best_score = 0.0
    best_candidate = candidates[0]
    query_lower = query.strip().lower()

    for c in candidates:
        if media_type == "movie":
            candidate_title = c.get("title", "").lower()
        else:
            candidate_title = c.get("name", "").lower()

        similarity = SequenceMatcher(None, query_lower, candidate_title).ratio()

        pop = c.get("popularity", 0)
        pop_bonus = min(pop / 200.0, 0.15)

        votes = c.get("vote_count", 0)
        vote_bonus = min(votes / 50000.0, 0.1)

        score = similarity * 0.75 + pop_bonus + vote_bonus

        if score > best_score:
            best_score = score
            best_candidate = c

    return round(min(best_score, 1.0), 4), best_candidate


def create_tmdb_client(mode, api_key=None, language="en", debug=False):
    if mode == "stub":
        logging.info("Using StubTMDbClient (no network calls)")
        return StubTMDbClient(language=language)
    elif mode == "real":
        if not api_key or api_key == "None":
            raise ValueError("TMDB_MODE is 'real' but no valid api_key is configured.")
        logging.info("Using RealTMDbClient (live TMDb API)")
        return RealTMDbClient(api_key=api_key, language=language, debug=debug)
    else:
        raise ValueError(f"Unknown TMDB_MODE: {mode}. Must be 'real' or 'stub'.")
