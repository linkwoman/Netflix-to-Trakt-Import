# netflix2trakt

## Overview
A Python CLI tool that imports Netflix viewing history into Trakt.tv. It reads a Netflix CSV export, matches titles against TMDB, and syncs the watch history to a Trakt account. Supports a "stub mode" for testing without API keys.

## Project Architecture
- **Language**: Python 3.10
- **Entry Point**: `netflix2trakt.py`
- **Config**: `config_defaults.ini` (defaults), `config.ini` (user overrides, gitignored)
- **Key Files**:
  - `netflix2trakt.py` - Main pipeline: parses CSV, matches via TMDb client, confidence scoring, review routing, syncs to Trakt
  - `tmdb_client.py` - TMDb client abstraction: `TMDbClientBase` (ABC), `RealTMDbClient` (live API), `StubTMDbClient` (fixture-driven), `compute_confidence()`, `create_tmdb_client()` factory, `get_details_with_credits()` enrichment
  - `review_queue.py` - Generates review_queue.csv: consolidates low-confidence resolved items and ambiguous candidates with full TMDb metadata (poster, cast, director, genres, year, etc.)
  - `TraktIO.py` - Trakt API interaction and authentication
  - `NetflixTvShow.py` - Netflix TV show/movie data models and CSV parsing
  - `config.py` - Configuration loader (reads INI files)
  - `run_smoke_test.py` - Smoke test script for stub+dry_run mode
  - `test_NetflixTvShows.py` - Unit tests
  - `history-dates-fixer.py` - Utility to fix date formats in CSV
  - `fixtures/tmdb_stub.json` - TMDb stub fixture data + enrichment data (real movie/show metadata for review queue)
  - `fixtures/sample_viewing_history.csv` - Sample Netflix CSV for testing

## Recent Changes
- Added review_queue.py: generates review_queue.csv with REVIEW_THRESHOLD=0.95
  - Low-confidence resolved items (conf < 0.95) get 1 row each with full metadata
  - Ambiguous items from needs_review.csv get 1 row per candidate ID, expanded with TMDb details+credits
  - Enrichment uses get_details_with_credits() with in-memory caching
  - StubTMDbClient returns real movie/show metadata from fixtures (real cast, directors, genres, poster paths)
  - Columns: source_file, review_reason, original_row_id, original_confidence, input_title, input_type, confidence, status, tmdb_id, media_type, tmdb_url, year, genres, stars, released_by, vision_by_label, vision_by, poster_path, candidate_rank, candidate_ids
- Added TMDB_MODE config (stub/real) defaulting to stub
- Changed Trakt dry_run default to True
- Created TMDb client abstraction layer (tmdb_client.py) with RealTMDbClient and StubTMDbClient
- Added fixture-driven stub responses (fixtures/tmdb_stub.json)
- Added confidence scoring and review routing (resolved.csv / needs_review.csv / skipped.csv)
- Refactored netflix2trakt.py to use injected client via factory pattern
- Added smoke test script (run_smoke_test.py) with sample CSV

## Output Files
The pipeline produces four CSV files:

- **resolved.csv** — Titles matched with high confidence (auto-accepted, confidence >= 0.80)
- **needs_review.csv** — Ambiguous matches with multiple candidates (confidence 0.40–0.80)
- **skipped.csv** — Titles with no TMDb match found
- **review_queue.csv** — Human review queue (see below)

### Review Queue (`review_queue.csv`)
A single consolidated CSV containing everything a human needs to review. Controlled by `REVIEW_THRESHOLD = 0.95` in `review_queue.py`.

**What goes in:**
- **Low-confidence resolved** — Items from resolved.csv with confidence < 0.95 (e.g., "Push" at 0.88). One row each with full metadata.
- **No match** — Items from skipped.csv with no TMDb candidates found. One row each with blank metadata fields — the human needs to manually look these up.
- **Ambiguous candidates** — Items from needs_review.csv, expanded to one row per candidate TMDb ID. Each row enriched with full metadata so the human can pick the right one.

**Columns (in order):**
`source_file, review_reason, original_row_id, original_confidence, input_title, input_type, confidence, status, tmdb_id, media_type, tmdb_url, year, genres, stars, released_by, vision_by_label, vision_by, poster_path, candidate_rank, candidate_ids`

**Enrichment metadata per row:**
- `tmdb_url` — Direct link to TMDb page
- `poster_path` — TMDb poster image path
- `year` — Release year (movie) or first air date year (TV)
- `genres` — Pipe-separated genre names
- `stars` — Top 5 cast members, pipe-separated
- `released_by` — Production companies (movies) or network (TV)
- `vision_by_label` / `vision_by` — "Directed by" + director name (movies) or "Created by" + creator names (TV)

## Setup Requirements
### Stub Mode (no API keys needed)
1. Run `python run_smoke_test.py` to test the full pipeline

### Real Mode
1. Copy `config_defaults.ini` to `config.ini`
2. Set `mode = real` in `[TMDB]` section
3. Set TMDB API key, Trakt client ID, and Trakt client secret in `config.ini`
4. Set `dry_run = False` in `[Trakt]` section
5. Place Netflix viewing history CSV as `NetflixViewingHistory.csv`
6. Run `python netflix2trakt.py`

## Dependencies
Managed via `requirements.txt`. Key libraries: requests, tmdbv3api, trakt.py, arrow, tqdm.

## Running Tests
```bash
pytest
python run_smoke_test.py
```
