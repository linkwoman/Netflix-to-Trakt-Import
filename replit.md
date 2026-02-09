# netflix2trakt

## Overview
A Python CLI tool that imports Netflix viewing history into Trakt.tv. It reads a Netflix CSV export, matches titles against TMDB, and syncs the watch history to a Trakt account. Supports a "stub mode" for testing without API keys.

## Project Architecture
- **Language**: Python 3.10
- **Entry Point**: `netflix2trakt.py`
- **Config**: `config_defaults.ini` (defaults), `config.ini` (user overrides, gitignored)
- **Key Files**:
  - `netflix2trakt.py` - Main pipeline: parses CSV, matches via TMDb client, confidence scoring, review routing, syncs to Trakt
  - `tmdb_client.py` - TMDb client abstraction: `TMDbClientBase` (ABC), `RealTMDbClient` (live API), `StubTMDbClient` (fixture-driven), `compute_confidence()`, `create_tmdb_client()` factory
  - `TraktIO.py` - Trakt API interaction and authentication
  - `NetflixTvShow.py` - Netflix TV show/movie data models and CSV parsing
  - `config.py` - Configuration loader (reads INI files)
  - `run_smoke_test.py` - Smoke test script for stub+dry_run mode
  - `test_NetflixTvShows.py` - Unit tests
  - `history-dates-fixer.py` - Utility to fix date formats in CSV
  - `fixtures/tmdb_stub.json` - TMDb stub fixture data
  - `fixtures/sample_viewing_history.csv` - Sample Netflix CSV for testing

## Recent Changes
- Added TMDB_MODE config (stub/real) defaulting to stub
- Changed Trakt dry_run default to True
- Created TMDb client abstraction layer (tmdb_client.py) with RealTMDbClient and StubTMDbClient
- Added fixture-driven stub responses (fixtures/tmdb_stub.json)
- Added confidence scoring and review routing (resolved.csv / needs_review.csv / skipped.csv)
- Refactored netflix2trakt.py to use injected client via factory pattern
- Added smoke test script (run_smoke_test.py) with sample CSV

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
