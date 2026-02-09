# netflix2trakt

## Overview
A Python CLI tool that imports Netflix viewing history into Trakt.tv. It reads a Netflix CSV export, matches titles against TMDB, and syncs the watch history to a Trakt account.

## Project Architecture
- **Language**: Python 3.10
- **Entry Point**: `netflix2trakt.py`
- **Config**: `config_defaults.ini` (defaults), `config.ini` (user overrides, gitignored)
- **Key Files**:
  - `netflix2trakt.py` - Main script: parses CSV, matches via TMDB, syncs to Trakt
  - `TraktIO.py` - Trakt API interaction and authentication
  - `NetflixTvShow.py` - Netflix TV show data models and parsing
  - `config.py` - Configuration loader (reads INI files)
  - `test_NetflixTvShows.py` - Unit tests
  - `history-dates-fixer.py` - Utility to fix date formats in CSV

## Setup Requirements
1. Copy `config_defaults.ini` to `config.ini`
2. Set TMDB API key, Trakt client ID, and Trakt client secret in `config.ini`
3. Place Netflix viewing history CSV as `NetflixViewingHistory.csv`
4. Run `python netflix2trakt.py`

## Dependencies
Managed via `requirements.txt`. Key libraries: requests, tmdbv3api, trakt.py, arrow, tqdm.

## Running Tests
```bash
pytest
```
