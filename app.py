"""Flask web app for netflix2trakt.

Routes:
  /                      - Home/dashboard
  /settings              - API keys + mode toggle
  /upload                - CSV upload + start import
  /processing/<job_id>   - Live progress page (polls /api/jobs/<id>)
  /api/jobs/<id>         - Job status JSON
  /results               - Results dashboard (latest run)
  /review                - Review queue with pick UI
  /api/picks             - Save review picks (POST)
  /sync                  - Sync confirmation + execution
  /history               - Past runs
  /run/<id>/<file>       - Download a run's CSV
  /auth/connect          - Start Trakt OAuth
  /auth/callback         - Trakt OAuth callback
  /auth/disconnect       - Forget Trakt token
"""

import csv
import datetime
import json
import logging
import os
import re
import secrets
import threading
import traceback

from flask import (
    Flask, render_template, request, redirect, url_for, jsonify,
    flash, send_from_directory, abort, session,
)
from werkzeug.utils import secure_filename

import web_config
import web_pipeline
import web_oauth
import web_sync


UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs("runs", exist_ok=True)


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET", secrets.token_hex(16))
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB upload limit

    # ---- in-memory job tracking ----
    jobs = {}
    jobs_lock = threading.Lock()
    # Serialize pipeline runs: existing CLI code writes routing CSVs to the
    # project root, so concurrent runs would race. One at a time.
    pipeline_lock = threading.Lock()

    RUN_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")

    def safe_run_dir(run_id):
        """Return runs/<run_id> only if run_id is safe. Else None."""
        if not run_id or not RUN_ID_RE.match(run_id):
            return None
        path = os.path.realpath(os.path.join("runs", run_id))
        runs_root = os.path.realpath("runs")
        if not path.startswith(runs_root + os.sep):
            return None
        if not os.path.isdir(path):
            return None
        return path

    def update_job(job_id, **fields):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id].update(fields)

    def make_progress_callback(job_id):
        def cb(stage, current, total, message=""):
            update_job(
                job_id,
                stage=stage,
                progress_current=current,
                progress_total=total,
                message=message,
            )
        return cb

    # ---- Trakt connection helpers ----
    def trakt_status():
        settings = web_config.get_settings()
        auth = web_oauth.load_authorization()
        if not auth or not web_oauth.is_token_valid(auth):
            return {"connected": False, "username": None}
        username = None
        if web_config.has_trakt_app():
            try:
                info = web_oauth.get_user_info(auth, settings["trakt_client_id"])
                username = info.get("user", {}).get("username")
            except Exception:
                pass
        return {"connected": True, "username": username}

    @app.context_processor
    def inject_globals():
        s = web_config.get_settings()
        return {
            "tmdb_mode": s["tmdb_mode"],
            "trakt_status": trakt_status(),
        }

    # ---- ROUTES ----

    @app.route("/")
    def index():
        latest = web_pipeline.list_past_runs()
        latest_run = latest[0] if latest else None
        return render_template("index.html", latest_run=latest_run)

    @app.route("/settings", methods=["GET", "POST"])
    def settings_page():
        if request.method == "POST":
            web_config.save_settings({
                "tmdb_mode": request.form.get("tmdb_mode", "stub"),
                "tmdb_api_key": request.form.get("tmdb_api_key", "").strip() or None,
                "tmdb_language": request.form.get("tmdb_language", "en"),
                "trakt_client_id": request.form.get("trakt_client_id", "").strip() or None,
                "trakt_client_secret": request.form.get("trakt_client_secret", "").strip() or None,
                "trakt_dry_run": request.form.get("trakt_dry_run") == "on",
            })
            flash("Settings saved.", "success")
            return redirect(url_for("settings_page"))

        s = web_config.get_settings()
        # mask the secret on display
        s_display = dict(s)
        if s_display["tmdb_api_key"] and s_display["tmdb_api_key"] != "None":
            s_display["tmdb_api_key_masked"] = s_display["tmdb_api_key"][:4] + "…" + s_display["tmdb_api_key"][-4:]
        else:
            s_display["tmdb_api_key_masked"] = ""
        if s_display["trakt_client_secret"] and s_display["trakt_client_secret"] != "None":
            s_display["trakt_client_secret_masked"] = "•" * 12
        else:
            s_display["trakt_client_secret_masked"] = ""

        redirect_uri = url_for("auth_callback", _external=True)
        return render_template("settings.html", s=s_display, redirect_uri=redirect_uri)

    @app.route("/upload", methods=["GET", "POST"])
    def upload_page():
        if request.method == "POST":
            f = request.files.get("csv_file")
            mode = request.form.get("mode") or web_config.get_settings()["tmdb_mode"]

            if not f or not f.filename:
                flash("Please select a CSV file.", "error")
                return redirect(url_for("upload_page"))

            # Prefix the saved filename with a unique token so concurrent
            # uploads of the same filename don't collide.
            filename = secure_filename(f.filename) or "upload.csv"
            unique_prefix = secrets.token_hex(6)
            saved_path = os.path.join(UPLOAD_DIR, f"{unique_prefix}_{filename}")
            f.save(saved_path)

            # Verify it's a CSV with the expected shape
            try:
                with open(saved_path, encoding="utf-8") as fh:
                    reader = csv.reader(fh)
                    header = next(reader, None)
                    if not header or len(header) < 2:
                        raise ValueError("CSV must have at least 2 columns (Title, Date)")
            except Exception as e:
                flash(f"Couldn't read CSV: {e}", "error")
                return redirect(url_for("upload_page"))

            # If real mode, copy CSV into the location the pipeline expects
            # (this is mostly cosmetic — web_pipeline.run_pipeline takes the
            # path directly). Just use saved_path.

            # Real mode requires TMDb key
            if mode == "real" and not web_config.has_tmdb_key():
                flash("Real mode requires a TMDb API key. Add it in Settings, or use Stub mode.", "error")
                return redirect(url_for("upload_page"))

            # Kick off background job
            job_id = web_pipeline.make_run_id()
            with jobs_lock:
                jobs[job_id] = {
                    "id": job_id,
                    "stage": "queued",
                    "progress_current": 0,
                    "progress_total": 1,
                    "message": "Starting...",
                    "result": None,
                    "error": None,
                }

            settings = web_config.get_settings()

            def worker():
                # Serialize pipeline runs — the CLI code writes shared root
                # CSV files, so concurrent runs would race.
                with pipeline_lock:
                    try:
                        result = web_pipeline.run_pipeline(
                            input_csv_path=saved_path,
                            mode=mode,
                            run_id=job_id,
                            progress_callback=make_progress_callback(job_id),
                            tmdb_api_key=None if mode == "stub" else settings["tmdb_api_key"],
                            tmdb_language=settings["tmdb_language"],
                        )
                        update_job(job_id, stage="done", result=result)
                    except Exception as e:
                        update_job(
                            job_id,
                            stage="error",
                            error=str(e),
                            message=f"Error: {e}",
                        )
                        app.logger.error(traceback.format_exc())

            thread = threading.Thread(target=worker, daemon=True)
            thread.start()

            return redirect(url_for("processing_page", job_id=job_id))

        return render_template(
            "upload.html",
            stub_mode=web_config.get_settings()["tmdb_mode"] == "stub",
        )

    @app.route("/processing/<job_id>")
    def processing_page(job_id):
        with jobs_lock:
            job = jobs.get(job_id)
        if not job:
            flash("Job not found.", "error")
            return redirect(url_for("upload_page"))
        return render_template("processing.html", job_id=job_id)

    @app.route("/api/jobs/<job_id>")
    def api_job_status(job_id):
        with jobs_lock:
            job = jobs.get(job_id)
        if not job:
            return jsonify({"error": "not found"}), 404
        return jsonify(job)

    def _resolve_run_base(run_id):
        """Return the directory to read run outputs from.

        If run_id given, must validate. If not, use latest run's snapshot
        directory (so we never depend on shared root CSVs being current).
        """
        if run_id:
            base = safe_run_dir(run_id)
            if not base:
                return None, None
            return base, run_id
        runs = web_pipeline.list_past_runs()
        if not runs:
            return None, None
        latest_id = runs[0]["run_id"]
        base = safe_run_dir(latest_id)
        return base, latest_id

    @app.route("/results")
    def results_page():
        run_id_arg = request.args.get("run_id")
        base, run_id = _resolve_run_base(run_id_arg)
        if base is None:
            if run_id_arg:
                flash("Run not found.", "error")
                return redirect(url_for("history_page"))
            flash("No runs yet — upload a CSV to start.", "info")
            return redirect(url_for("upload_page"))

        summary = ""
        summary_path = os.path.join(base, "run_summary.txt")
        if os.path.exists(summary_path):
            with open(summary_path, encoding="utf-8") as f:
                summary = f.read()

        def read_csv(name):
            p = os.path.join(base, name)
            if not os.path.exists(p):
                return []
            with open(p, encoding="utf-8") as f:
                return list(csv.DictReader(f))

        resolved = read_csv("resolved.csv")
        needs_review = read_csv("needs_review.csv")
        skipped = read_csv("skipped.csv")
        failures = read_csv("failures.csv")

        return render_template(
            "results.html",
            summary=summary,
            resolved=resolved,
            needs_review=needs_review,
            skipped=skipped,
            failures=failures,
            run_id=run_id,
        )

    @app.route("/review")
    def review_page():
        run_id_arg = request.args.get("run_id")
        base, run_id = _resolve_run_base(run_id_arg)
        if base is None:
            flash("No review queue found. Run an import first.", "error")
            return redirect(url_for("upload_page"))

        queue_path = os.path.join(base, "review_queue.csv")
        if not os.path.exists(queue_path):
            flash("No review queue found. Run an import first.", "error")
            return redirect(url_for("upload_page"))

        with open(queue_path, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        # Group rows by original_row_id
        groups = {}
        for r in rows:
            rid = r["original_row_id"]
            if rid not in groups:
                groups[rid] = {
                    "original_row_id": rid,
                    "input_title": r["input_title"],
                    "input_type": r["input_type"],
                    "review_reason": r["review_reason"],
                    "original_confidence": r.get("original_confidence", "0"),
                    "candidates": [],
                }
            if r.get("tmdb_id"):
                groups[rid]["candidates"].append(r)

        groups_list = list(groups.values())
        # Sort: ambiguous_candidates first, then no_match
        order = {"ambiguous_candidates": 0, "low_confidence_resolved": 1, "no_match": 2}
        groups_list.sort(key=lambda g: order.get(g["review_reason"], 9))

        picks = web_sync.load_picks(run_id)

        return render_template(
            "review.html",
            groups=groups_list,
            picks=picks,
            run_id=run_id,
            total=len(groups_list),
        )

    @app.route("/api/picks/bulk_accept_top", methods=["POST"])
    def api_bulk_accept_top():
        """For every ambiguous group in the run that hasn't been reviewed yet,
        accept the top-ranked candidate. Existing picks are left untouched."""
        data = request.get_json(silent=True) or {}
        run_id_arg = data.get("run_id") or request.args.get("run_id")
        base, run_id = _resolve_run_base(run_id_arg)
        if base is None:
            return jsonify({"error": "no run available"}), 400

        queue_path = os.path.join(base, "review_queue.csv")
        if not os.path.exists(queue_path):
            return jsonify({"error": "no review queue"}), 400

        with open(queue_path, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        # Pick the top candidate per row (highest candidate_confidence) from
        # ambiguous_candidates groups only — no_match has nothing to accept.
        top_by_row = {}
        for r in rows:
            if r.get("review_reason") != "ambiguous_candidates":
                continue
            if not r.get("tmdb_id"):
                continue
            rid = r["original_row_id"]
            try:
                conf = float(r.get("candidate_confidence") or 0)
            except ValueError:
                conf = 0.0
            cur = top_by_row.get(rid)
            if cur is None or conf > cur["conf"]:
                top_by_row[rid] = {
                    "conf": conf,
                    "tmdb_id": r["tmdb_id"],
                    "media_type": r.get("media_type") or "",
                }

        picks = web_sync.load_picks(run_id)
        added = 0
        for rid, top in top_by_row.items():
            if rid in picks:
                continue  # don't overwrite manual picks/skips
            picks[rid] = {
                "action": "accept",
                "tmdb_id": top["tmdb_id"],
                "media_type": top["media_type"],
            }
            added += 1

        web_sync.save_picks(run_id, picks)
        return jsonify(
            {
                "ok": True,
                "accepted": added,
                "skipped_existing": len(top_by_row) - added,
                "picks_count": len(picks),
                "run_id": run_id,
            }
        )

    @app.route("/api/picks", methods=["POST"])
    def api_save_pick():
        data = request.get_json(force=True)
        original_row_id = str(data.get("original_row_id", ""))
        action = data.get("action")  # "accept" | "skip" | "clear"
        tmdb_id = data.get("tmdb_id")
        media_type = data.get("media_type")
        run_id_arg = data.get("run_id")

        if not original_row_id:
            return jsonify({"error": "missing original_row_id"}), 400

        base, run_id = _resolve_run_base(run_id_arg)
        if base is None:
            return jsonify({"error": "no run available"}), 400

        picks = web_sync.load_picks(run_id)

        if action == "clear":
            picks.pop(original_row_id, None)
        else:
            picks[original_row_id] = {
                "action": action,
                "tmdb_id": tmdb_id,
                "media_type": media_type,
            }
        web_sync.save_picks(run_id, picks)

        return jsonify({"ok": True, "picks_count": len(picks), "run_id": run_id})

    @app.route("/sync", methods=["GET", "POST"])
    def sync_page():
        settings = web_config.get_settings()
        ts = trakt_status()

        run_id_arg = request.args.get("run_id") or request.form.get("run_id")
        base, run_id = _resolve_run_base(run_id_arg)
        if base is None:
            flash("No run available to sync. Run an import first.", "error")
            return redirect(url_for("upload_page"))

        if request.method == "POST":
            dry_run = request.form.get("dry_run") == "on"

            if not dry_run and not ts["connected"]:
                flash("Connect to Trakt before doing a real sync.", "error")
                return redirect(url_for("sync_page", run_id=run_id))

            try:
                result = web_sync.sync_to_trakt(
                    run_id=run_id,
                    run_dir=base,
                    client_id=settings["trakt_client_id"],
                    dry_run=dry_run,
                )
                # Stamp metadata.json on a successful real sync.
                if not dry_run:
                    try:
                        meta_path = os.path.join(base, "metadata.json")
                        meta = {}
                        if os.path.exists(meta_path):
                            with open(meta_path) as mf:
                                meta = json.load(mf)
                        meta["last_synced_at"] = datetime.datetime.now().isoformat(timespec="seconds")
                        meta["last_sync_added"] = result.get("added")
                        with open(meta_path, "w") as mf:
                            json.dump(meta, mf, indent=2)
                    except Exception as e:
                        logging.warning(f"Could not update metadata.json with last_synced_at: {e}")
                return render_template(
                    "sync_result.html", result=result, dry_run=dry_run, run_id=run_id,
                )
            except Exception as e:
                flash(f"Sync failed: {e}", "error")
                return redirect(url_for("sync_page", run_id=run_id))

        # GET - preview. If connected, also fetch the user's watched library
        # so the preview accurately reflects what dedup will skip.
        already_watched = None
        watched_fetch_error = None
        if ts["connected"]:
            try:
                already_watched = web_sync.fetch_already_watched(
                    client_id=settings["trakt_client_id"]
                )
            except Exception as e:
                watched_fetch_error = str(e)

        try:
            payload, summary = web_sync.build_sync_payload(
                run_id=run_id, run_dir=base, already_watched=already_watched
            )
        except Exception as e:
            flash(f"Couldn't build sync payload: {e}", "error")
            return redirect(url_for("results_page", run_id=run_id))

        picks = web_sync.load_picks(run_id)
        # Pull last_synced_at from metadata if present.
        last_synced_at = None
        try:
            meta_path = os.path.join(base, "metadata.json")
            if os.path.exists(meta_path):
                with open(meta_path) as mf:
                    last_synced_at = json.load(mf).get("last_synced_at")
        except Exception:
            pass

        return render_template(
            "sync.html",
            summary=summary,
            picks_count=sum(1 for p in picks.values() if p.get("action") == "accept"),
            trakt_status=ts,
            run_id=run_id,
            last_synced_at=last_synced_at,
            watched_fetch_error=watched_fetch_error,
        )

    @app.route("/history")
    def history_page():
        runs = web_pipeline.list_past_runs()
        return render_template("history.html", runs=runs)

    @app.route("/run/<run_id>/<filename>")
    def download_run_file(run_id, filename):
        run_dir = safe_run_dir(run_id)
        if not run_dir:
            abort(404)
        # Whitelist the files we expose (no path-traversal in filename either)
        allowed = {
            "resolved.csv", "needs_review.csv", "skipped.csv", "failures.csv",
            "review_queue.csv", "run_summary.txt", "metadata.json",
        }
        if filename not in allowed:
            abort(404)
        return send_from_directory(run_dir, filename, as_attachment=True)

    # ---- Trakt OAuth ----

    @app.route("/auth/connect")
    def auth_connect():
        s = web_config.get_settings()
        if not web_config.has_trakt_app():
            flash("Set Trakt Client ID and Secret in Settings first.", "error")
            return redirect(url_for("settings_page"))
        state = secrets.token_urlsafe(16)
        session["oauth_state"] = state
        redirect_uri = url_for("auth_callback", _external=True)
        url = web_oauth.build_authorize_url(s["trakt_client_id"], redirect_uri, state)
        return redirect(url)

    @app.route("/auth/callback")
    def auth_callback():
        code = request.args.get("code")
        state = request.args.get("state")
        if not code:
            flash("Trakt did not return an auth code.", "error")
            return redirect(url_for("settings_page"))
        if state != session.get("oauth_state"):
            flash("OAuth state mismatch — please try again.", "error")
            return redirect(url_for("settings_page"))

        s = web_config.get_settings()
        redirect_uri = url_for("auth_callback", _external=True)
        try:
            authorization = web_oauth.exchange_code_for_token(
                code, s["trakt_client_id"], s["trakt_client_secret"], redirect_uri,
            )
            web_oauth.save_authorization(authorization)
            flash("Connected to Trakt!", "success")
        except Exception as e:
            flash(f"Trakt auth failed: {e}", "error")
        return redirect(url_for("settings_page"))

    @app.route("/auth/disconnect", methods=["POST"])
    def auth_disconnect():
        web_oauth.clear_authorization()
        flash("Disconnected from Trakt.", "success")
        return redirect(url_for("settings_page"))

    return app


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    # Reloader breaks long-running background pipeline threads, so leave it off.
    create_app().run(host="0.0.0.0", port=5000, debug=debug, use_reloader=False)
