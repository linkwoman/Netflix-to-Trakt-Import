"""Read/write the user's config.ini for the web settings page."""

import configparser
import os

USER_CONFIG_FILE = "config.ini"
DEFAULTS_FILE = "config_defaults.ini"


def _load():
    cp = configparser.ConfigParser()
    cp.read(DEFAULTS_FILE)
    if os.path.exists(USER_CONFIG_FILE):
        cp.read(USER_CONFIG_FILE)
    return cp


def get_settings():
    cp = _load()
    return {
        "tmdb_mode": cp.get("TMDB", "mode", fallback="stub"),
        "tmdb_api_key": cp.get("TMDB", "api_key", fallback="None"),
        "tmdb_language": cp.get("TMDB", "language", fallback="en"),
        "trakt_client_id": cp.get("Trakt", "id", fallback="None"),
        "trakt_client_secret": cp.get("Trakt", "secret", fallback="None"),
        "trakt_dry_run": cp.getboolean("Trakt", "dry_run", fallback=True),
    }


def save_settings(settings):
    """Write only the user-facing fields to config.ini, preserving structure."""
    cp = configparser.ConfigParser()
    if os.path.exists(USER_CONFIG_FILE):
        cp.read(USER_CONFIG_FILE)

    if not cp.has_section("TMDB"):
        cp.add_section("TMDB")
    if not cp.has_section("Trakt"):
        cp.add_section("Trakt")

    cp.set("TMDB", "mode", settings.get("tmdb_mode", "stub"))
    if settings.get("tmdb_api_key"):
        cp.set("TMDB", "api_key", settings["tmdb_api_key"])
    cp.set("TMDB", "language", settings.get("tmdb_language", "en"))

    if settings.get("trakt_client_id"):
        cp.set("Trakt", "id", settings["trakt_client_id"])
    if settings.get("trakt_client_secret"):
        cp.set("Trakt", "secret", settings["trakt_client_secret"])
    cp.set("Trakt", "dry_run", str(settings.get("trakt_dry_run", True)))

    with open(USER_CONFIG_FILE, "w") as f:
        cp.write(f)


def has_tmdb_key():
    s = get_settings()
    return s["tmdb_api_key"] and s["tmdb_api_key"] != "None"


def has_trakt_app():
    s = get_settings()
    return (
        s["trakt_client_id"] and s["trakt_client_id"] != "None"
        and s["trakt_client_secret"] and s["trakt_client_secret"] != "None"
    )
