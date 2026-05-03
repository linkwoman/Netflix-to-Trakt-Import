"""Trakt OAuth (authorization code) flow for the web app.

Uses Trakt's standard OAuth code flow rather than the device/PIN flow used by
the CLI. The resulting authorization dict is saved to traktAuth.json in the
same format the existing TraktIO expects, so the sync code can use it
unchanged.
"""

import datetime
import json
import os
from urllib.parse import urlencode

import requests

TRAKT_AUTH_FILE = "traktAuth.json"
AUTHORIZE_URL = "https://trakt.tv/oauth/authorize"
TOKEN_URL = "https://api.trakt.tv/oauth/token"
USER_SETTINGS_URL = "https://api.trakt.tv/users/settings"


def build_authorize_url(client_id, redirect_uri, state=None):
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
    }
    if state:
        params["state"] = state
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


def exchange_code_for_token(code, client_id, client_secret, redirect_uri):
    """Exchange an authorization code for an access token.

    Returns the authorization dict on success, raises on failure.
    """
    payload = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    resp = requests.post(TOKEN_URL, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def refresh_token(authorization, client_id, client_secret, redirect_uri):
    """Refresh an expired token. Returns the new authorization dict."""
    payload = {
        "refresh_token": authorization["refresh_token"],
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "refresh_token",
    }
    resp = requests.post(TOKEN_URL, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def save_authorization(authorization):
    with open(TRAKT_AUTH_FILE, "w") as f:
        json.dump(authorization, f, indent=2)


def load_authorization():
    if not os.path.exists(TRAKT_AUTH_FILE):
        return None
    try:
        with open(TRAKT_AUTH_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def clear_authorization():
    if os.path.exists(TRAKT_AUTH_FILE):
        os.remove(TRAKT_AUTH_FILE)


def is_token_valid(authorization):
    if not authorization:
        return False
    created = authorization.get("created_at")
    expires = authorization.get("expires_in")
    if not created or not expires:
        return False
    return int(datetime.datetime.now().timestamp()) < (created + expires)


def get_user_info(authorization, client_id):
    """Fetch the connected user's Trakt username/info."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {authorization['access_token']}",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
    }
    resp = requests.get(USER_SETTINGS_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()
