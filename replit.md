# netflix2trakt

## Overview
A Python tool that imports Netflix viewing history into Trakt.tv. It reads a Netflix CSV export, matches titles against TMDB, and syncs the watch history to a Trakt account. Supports a "stub mode" for testing without API keys.

Available as **two interfaces** sharing the same pipeline code:
- **Web UI** (`python app.py`) — Flask app on port 5000 with drag-drop upload, OAuth, visual review queue, sync.
- **CLI** (`python netflix2trakt.py` or `python run_smoke_test.py`) — original command-line entry points, unchanged.

## Pipeline Flow
One command produces all outputs. Running `python netflix2trakt.py` (or `python run_smoke_test.py` for stub mode) executes the full pipeline:

1. **Parse** — Reads Netflix CSV, creates show/movie entities
2. **Match** — Searches TMDb for each entity, scores confidence
3. **Route** — Sorts entities into resolved / needs_review / skipped / failures CSVs
4. **Review Queue** — Automatically generates `review_queue.csv` with enriched metadata
5. **Summary** — Writes `run_summary.txt` and per-run log file to `logs/`
6. **Accounting** — Asserts every entity is accounted for in exactly one output CSV

## Project Architecture
- **Language**: Python 3.10
- **Web Entry Point**: `app.py` (Flask, port 5000) — primary user interface
- **CLI Entry Point**: `netflix2trakt.py` (single orchestrated run)
- **Config**: `config_defaults.ini` (defaults), `config.ini` (user overrides, gitignored)
- **Key Files**:
  - `netflix2trakt.py` - Main pipeline: parses CSV, matches via TMDb client, confidence scoring, review routing, accounting, run summary, logging setup, syncs to Trakt
  - `tmdb_client.py` - TMDb client abstraction: `TMDbClientBase` (ABC), `RealTMDbClient` (live API), `StubTMDbClient` (fixture-driven), `compute_confidence()`, `compute_all_confidences()`, `create_tmdb_client()` factory, `get_details_with_credits()` enrichment
  - `review_queue.py` - Generates review_queue.csv: consolidates low-confidence resolved items and ambiguous candidates with full TMDb metadata (poster, cast, director, genres, year, etc.)
  - `TraktIO.py` - Trakt API interaction and authentication
  - `NetflixTvShow.py` - Netflix TV show/movie data models and CSV parsing
  - `config.py` - Configuration loader (reads INI files)
  - `run_smoke_test.py` - Smoke test script for stub+dry_run mode
  - `test_NetflixTvShows.py` - Unit tests
  - `history-dates-fixer.py` - Utility to fix date formats in CSV
  - `fixtures/tmdb_stub.json` - TMDb stub fixture data + enrichment data (real movie/show metadata for review queue)
  - `fixtures/sample_viewing_history.csv` - Sample Netflix CSV for testing

### Web UI files
  - `app.py` - Flask app: routes for `/`, `/settings`, `/upload`, `/processing/<id>`, `/api/jobs/<id>`, `/results`, `/review`, `/api/picks`, `/sync`, `/history`, `/run/<id>/<file>`, `/auth/connect`, `/auth/callback`, `/auth/disconnect`. Background pipeline jobs run in threads, tracked in an in-memory dict.
  - `web_pipeline.py` - `run_pipeline()` wraps the existing CLI functions (parse → match → route → review queue → summary) with a progress callback. Snapshots all outputs to `runs/<run_id>/`.
  - `web_oauth.py` - Trakt authorization-code OAuth flow (web-friendly), saves token to `traktAuth.json`.
  - `web_sync.py` - Builds sync payload from `resolved.csv` + `review_picks.json`, posts to Trakt `/sync/history`. Honors a dry-run toggle.
  - `web_config.py` - Read/write `config.ini` for the settings page.
  - `templates/` - Jinja templates: `base.html`, `index.html`, `settings.html`, `upload.html`, `processing.html`, `results.html`, `review.html`, `sync.html`, `sync_result.html`, `history.html`.
  - `static/style.css` - Self-contained styles (no framework).

### Web UI runtime files (gitignored)
  - `uploads/` - Temporary location for uploaded CSVs
  - `runs/<run_id>/` - Per-run snapshots of all outputs + `metadata.json`
  - `review_picks.json` - User's review queue picks (which TMDb candidate to accept per row)
  - `traktAuth.json` - OAuth token (also used by CLI)

## Confidence Thresholds
- `CONFIDENCE_AUTO_ACCEPT = 0.95` — Entity goes to resolved.csv
- `CONFIDENCE_REVIEW = 0.40` — Entity goes to needs_review.csv (between 0.40 and 0.95)
- Below 0.40 or no candidates — Entity goes to skipped.csv
- Errors during processing — Entity goes to failures.csv

## Recent Changes
- **Web UI**: Added Flask web interface (`app.py`) with drag-drop upload, live progress, results dashboard, visual review queue (poster cards), Trakt OAuth (authorization code flow), one-click sync with dry-run toggle, and per-run history. Pipeline code in `netflix2trakt.py` is reused unchanged via `web_pipeline.py`. CLI continues to work exactly as before.
- **candidate_title in review_queue.csv**: Added a `candidate_title` column populated from TMDb enrichment so the review UI can show each candidate's actual TMDb title (not just network/year).

- **scoring_breakdown**: Added 5 scoring component columns (`title_similarity`, `popularity`, `popularity_bonus`, `vote_count`, `vote_count_bonus`) to `review_queue.csv`, `resolved.csv`, and `needs_review.csv`. Shows the full breakdown of how each candidate's confidence score was computed. In `needs_review.csv`, components are stored as semicolon-separated values per candidate (e.g., `candidate_title_similarities`, `candidate_popularities`, etc.).
- **candidate_confidence**: Added per-candidate confidence scoring via `compute_all_confidences()`. Candidates sorted by `candidate_confidence` descending; `candidate_rank` assigned from that ordering. Row-level `confidence` = max `candidate_confidence`. `review_queue.csv` includes both `confidence` and `candidate_confidence`.
- **data_source**: Added `data_source` column to all routing CSVs and `review_queue.csv`. Values: `test` (stub mode) or `live` (real TMDb). Never mixed within a single run.
- **best_candidate_title**: Added `best_candidate_title` column to `needs_review.csv` showing the top candidate's title for quick reference (not used downstream)
- **Orchestration**: Single-command run always produces all CSVs + run_summary.txt + log file
- **Threshold alignment**: CONFIDENCE_AUTO_ACCEPT raised from 0.80 to 0.95; resolved.csv now contains only high-confidence matches
- **Logging**: Per-run log file in `logs/` directory with unique run_id; Python logging module with INFO/WARNING/ERROR levels; only short summary printed to stdout
- **Row accounting**: Each entity gets a stable `original_row_id`; `failures.csv` captures errored rows; end-of-run assertion verifies total_entities == resolved + needs_review + skipped + failures
- **Run summary**: `run_summary.txt` generated every run with paragraph description, bullet-point stats, review reason breakdown, log path, and next action guidance
- Added review_queue.py: generates review_queue.csv with REVIEW_THRESHOLD=0.95
- Added TMDB_MODE config (stub/real) defaulting to stub
- Changed Trakt dry_run default to True
- Created TMDb client abstraction layer (tmdb_client.py) with RealTMDbClient and StubTMDbClient
- Added fixture-driven stub responses (fixtures/tmdb_stub.json)
- Added confidence scoring and review routing
- Refactored netflix2trakt.py to use injected client via factory pattern
- Added smoke test script (run_smoke_test.py) with sample CSV

## Output Files
Every run produces these files:

- **resolved.csv** — Titles matched with confidence >= 0.95 (auto-accepted); includes `data_source`
- **needs_review.csv** — Ambiguous matches (confidence 0.40–0.95); includes `best_candidate_title`, `candidate_confidences` (semicolon-separated per candidate), and `data_source`
- **skipped.csv** — Titles with no TMDb match or confidence < 0.40; includes `data_source`
- **failures.csv** — Titles that errored during processing; includes `data_source`
- **review_queue.csv** — Consolidated human review queue (see below)
- **run_summary.txt** — Human-readable run report (see below)
- **logs/run_<run_id>.log** — Detailed log file for the run

### Review Queue (`review_queue.csv`)
A single consolidated CSV containing everything a human needs to review. Controlled by `REVIEW_THRESHOLD = 0.95` in `review_queue.py`.

**What goes in:**
- **Low-confidence resolved** — Items from resolved.csv with confidence < 0.95. One row each with full metadata.
- **No match** — Items from skipped.csv with no TMDb candidates found. One row each with blank metadata fields — the human needs to manually look these up.
- **Ambiguous candidates** — Items from needs_review.csv, expanded to one row per candidate TMDb ID. Each row enriched with full metadata so the human can pick the right one.

**Columns (in order):**
`source_file, review_reason, original_row_id, original_confidence, input_title, input_type, confidence, candidate_confidence, status, tmdb_id, media_type, tmdb_url, title_similarity, popularity, popularity_bonus, vote_count, vote_count_bonus, year, genres, stars, released_by, vision_by_label, vision_by, poster_path, candidate_rank, candidate_ids, data_source`

**Key columns:**
- `original_confidence` — The entity-level confidence score at routing time (before review queue expansion)

**Scoring breakdown per row:**
- `title_similarity` — How closely the candidate title matches the search query (0–1, from SequenceMatcher)
- `popularity` — Raw TMDb popularity number
- `popularity_bonus` — Capped contribution to score (0–0.15); computed as min(popularity / 200, 0.15)
- `vote_count` — Raw number of TMDb votes/ratings
- `vote_count_bonus` — Capped contribution to score (0–0.10); computed as min(vote_count / 50000, 0.10)

**Enrichment metadata per row:**
- `tmdb_url` — Direct link to TMDb page
- `poster_path` — TMDb poster image path
- `year` — Release year (movie) or first air date year (TV)
- `genres` — Pipe-separated genre names
- `stars` — Top 5 cast members, pipe-separated
- `released_by` — Production companies (movies) or network (TV)
- `vision_by_label` / `vision_by` — "Directed by" + director name (movies) or "Created by" + creator names (TV)

### Run Summary (`run_summary.txt`)
A human-readable report generated after every run. Contains:
- **Paragraph** — Prose description of what happened (row count, mode, results)
- **Bullet points** — Run ID, mode, input/output counts, review reason breakdown, log file path, suggested next action

To interpret: open `run_summary.txt` and check the counts. If `failures.csv` has rows, investigate the log file. If `review_queue.csv` has rows, open it and filter by `review_reason` to decide on each item.

## Setup Requirements
### Stub Mode (no API keys needed)
1. Run `python run_smoke_test.py` to test the full pipeline
2. Check outputs: resolved.csv, needs_review.csv, skipped.csv, failures.csv, review_queue.csv, run_summary.txt, logs/
3. **Input sampling**: In stub mode, the pipeline automatically caps the number of entities processed to `min(fixture_count × 3, 50)`. This keeps smoke tests fast while still exercising a representative mix of matches. If the input has fewer entities than the sample size, all are processed. To test with large inputs, change `STUB_SAMPLE_CAP` in `netflix2trakt.py` to 2000.

### Real Mode
1. Copy `config_defaults.ini` to `config.ini`
2. Set `mode = real` in `[TMDB]` section
3. Set TMDB API key, Trakt client ID, and Trakt client secret in `config.ini`
4. Set `dry_run = False` in `[Trakt]` section
5. Place Netflix viewing history CSV as `NetflixViewingHistory.csv`
6. Run `python netflix2trakt.py`
7. Check `run_summary.txt` for results, `review_queue.csv` for items needing human review

## Dependencies
Managed via `requirements.txt`. Key libraries: requests, tmdbv3api, trakt.py, arrow, tqdm.

## Running Tests
```bash
pytest
python run_smoke_test.py
```
