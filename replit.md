# netflix2trakt

## Overview
`netflix2trakt` is a Python tool designed to import Netflix viewing history into Trakt.tv. It processes a Netflix CSV export, matches viewing entries with titles from The Movie Database (TMDb), and then synchronizes the watch history with a user's Trakt account. The project offers both a web-based user interface and a command-line interface, sharing a common processing pipeline. Its core capabilities include robust title matching with confidence scoring, a visual review queue for ambiguous matches, and comprehensive logging and reporting for each synchronization run.

The business vision for `netflix2trakt` is to provide a seamless and efficient way for users to migrate and maintain their viewing history across platforms, enhancing their media tracking experience. By automating the often tedious process of manual entry, it aims to save users time and ensure accuracy, thereby increasing engagement with Trakt.tv and offering a valuable utility in the media consumption ecosystem.

## User Preferences
I prefer clear and concise communication. When explaining technical concepts, please use simple language and avoid excessive jargon. I value an iterative development approach, where changes are proposed and discussed before implementation. Please ask for confirmation before making any significant changes to the codebase or architectural decisions. I appreciate detailed explanations for complex issues or design choices.

## System Architecture

### UI/UX Decisions
The project features a Flask-based web UI providing a user-friendly experience with drag-and-drop CSV upload, OAuth-based authentication for Trakt.tv, a visual review queue displaying enriched metadata (including posters), and an intuitive interface for initiating and monitoring synchronization jobs. The design prioritizes clarity and ease of use, with self-contained CSS and Jinja templates.

### Technical Implementations
The core logic is implemented in Python 3.10. The system employs a pipeline architecture:
1.  **Parse**: Reads Netflix CSV and creates movie/show entities.
2.  **Match**: Searches TMDb for each entity, computes confidence scores.
3.  **Route**: Categorizes entities into `resolved`, `needs_review`, `skipped`, or `failures` based on confidence.
4.  **Review Queue**: Generates `review_queue.csv` with enriched metadata for human review.
5.  **Summary**: Produces `run_summary.txt` and a detailed log file per run.
6.  **Accounting**: Ensures all entities are tracked through the pipeline outputs.

A factory pattern is used for TMDb client abstraction, supporting both real API calls (`RealTMDbClient`) and stubbed responses (`StubTMDbClient`) for testing. Trakt API interaction is handled by `TraktIO.py`, including OAuth for authentication. Web UI operations run in background threads, with progress tracked in-memory.

### Feature Specifications
-   **Confidence Scoring**: Titles are matched against TMDb with a confidence score based on title similarity, popularity, and vote count.
    -   `CONFIDENCE_AUTO_ACCEPT = 0.90`: Items above this are auto-accepted.
    -   `CONFIDENCE_REVIEW = 0.40`: Items between 0.40 and 0.90 require review.
    -   Items below 0.40 or with no candidates are skipped.
-   **Review Queue**: `review_queue.csv` consolidates low-confidence resolved items, no-match items, and ambiguous candidates. It includes enriched metadata like TMDb URL, poster path, year, genres, cast, and crew for informed decision-making.
-   **Trakt Synchronization**:
    -   Supports dry-run mode.
    -   Dedupes entries against existing Trakt watched history.
    -   Syncs `watched_at` timestamps for movies and episodes.
    -   Handles show-level entries as fallback when episode-specific mapping is unavailable or user-picked.
    -   POSTs data in chunks of up to 1000 items to avoid oversized payloads.
-   **Configuration**: Uses `config_defaults.ini` for defaults and `config.ini` for user overrides, which is gitignored.

### System Design Choices
-   **Modularity**: Clear separation of concerns, e.g., `tmdb_client.py` for TMDb, `TraktIO.py` for Trakt, `review_queue.py` for review logic.
-   **Robust Error Handling**: `failures.csv` captures errored rows, and end-of-run assertions ensure data integrity.
-   **Logging**: Detailed per-run log files are generated for debugging and auditing.

## External Dependencies
-   **The Movie Database (TMDb)**: Used for matching Netflix viewing history entries to movie and TV show titles, enriching metadata, and providing poster images.
-   **Trakt.tv**: The target platform for synchronizing and storing the user's watch history.
-   **`requests`**: HTTP library for making API calls.
-   **`tmdbv3api`**: Python wrapper for The Movie Database API.
-   **`trakt.py`**: Python library for interacting with the Trakt.tv API.
-   **`Flask`**: Web framework for building the user interface.
-   **`arrow`**: Library for handling dates and times.
-   **`tqdm`**: Library for displaying progress bars.
-   **`pytest`**: Testing framework.