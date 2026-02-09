import csv
import logging
import os

REVIEW_THRESHOLD = 0.95

COLUMNS = [
    "source_file", "review_reason", "original_row_id", "original_confidence",
    "input_title", "input_type", "confidence", "status",
    "tmdb_id", "media_type", "tmdb_url",
    "year", "genres", "stars", "released_by", "vision_by_label", "vision_by", "poster_path",
    "candidate_rank", "candidate_ids",
]


def _parse_media_type(input_type):
    t = input_type.strip().lower()
    if t in ("movie",):
        return "movie"
    if t in ("tv_show", "tv", "show"):
        return "tv"
    return "unknown"


def _build_tmdb_url(media_type, tmdb_id):
    if media_type == "movie":
        return f"https://www.themoviedb.org/movie/{tmdb_id}"
    elif media_type == "tv":
        return f"https://www.themoviedb.org/tv/{tmdb_id}"
    return ""


def _enrich(client, media_type, tmdb_id, cache):
    cache_key = (media_type, int(tmdb_id))
    if cache_key in cache:
        return cache[cache_key]

    try:
        data = client.get_details_with_credits(media_type, int(tmdb_id))
    except Exception as e:
        logging.warning(f"TMDb enrichment failed for {media_type}/{tmdb_id}: {e}")
        data = {}

    if media_type == "movie":
        year = (data.get("release_date") or "")[:4]
        released_by = " | ".join(
            c.get("name", "") for c in (data.get("production_companies") or [])[:2]
        )
        vision_label = "Directed by"
        directors = [
            c.get("name", "")
            for c in (data.get("credits", {}).get("crew") or [])
            if c.get("job") == "Director"
        ]
        vision_by = ", ".join(directors[:2])
    else:
        year = (data.get("first_air_date") or "")[:4]
        released_by = " | ".join(
            n.get("name", "") for n in (data.get("networks") or [])[:1]
        )
        vision_label = "Created by"
        creators = [c.get("name", "") for c in (data.get("created_by") or [])]
        vision_by = ", ".join(creators[:3])

    genres = " | ".join(g.get("name", "") for g in (data.get("genres") or []))
    cast = data.get("credits", {}).get("cast") or []
    stars = " | ".join(c.get("name", "") for c in cast[:5])
    poster_path = data.get("poster_path", "")

    result = {
        "year": year,
        "genres": genres,
        "stars": stars,
        "released_by": released_by,
        "vision_by_label": vision_label,
        "vision_by": vision_by,
        "poster_path": poster_path or "",
    }
    cache[cache_key] = result
    return result


def generate_review_queue(client, output_dir="."):
    resolved_path = os.path.join(output_dir, "resolved.csv")
    needs_review_path = os.path.join(output_dir, "needs_review.csv")
    review_queue_path = os.path.join(output_dir, "review_queue.csv")

    rows = []
    cache = {}

    if os.path.exists(resolved_path):
        with open(resolved_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                conf = float(row.get("confidence", 0))
                if conf >= REVIEW_THRESHOLD:
                    continue

                input_type = row.get("type", "unknown")
                media_type = _parse_media_type(input_type)
                tmdb_id = row.get("tmdb_id", "")

                enrichment = _enrich(client, media_type, tmdb_id, cache)

                rows.append({
                    "source_file": "resolved.csv",
                    "review_reason": "low_confidence_resolved",
                    "original_row_id": row.get("original_row_id", ""),
                    "original_confidence": conf,
                    "input_title": row.get("title", ""),
                    "input_type": input_type,
                    "confidence": conf,
                    "status": "resolved_low_confidence",
                    "tmdb_id": tmdb_id,
                    "media_type": media_type,
                    "tmdb_url": _build_tmdb_url(media_type, tmdb_id),
                    "year": enrichment["year"],
                    "genres": enrichment["genres"],
                    "stars": enrichment["stars"],
                    "released_by": enrichment["released_by"],
                    "vision_by_label": enrichment["vision_by_label"],
                    "vision_by": enrichment["vision_by"],
                    "poster_path": enrichment["poster_path"],
                    "candidate_rank": 0,
                    "candidate_ids": "",
                })

    skipped_path = os.path.join(output_dir, "skipped.csv")
    if os.path.exists(skipped_path):
        with open(skipped_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                input_type = row.get("type", "unknown")
                media_type = _parse_media_type(input_type)

                rows.append({
                    "source_file": "skipped.csv",
                    "review_reason": "no_match",
                    "original_row_id": row.get("original_row_id", ""),
                    "original_confidence": 0,
                    "input_title": row.get("title", ""),
                    "input_type": input_type,
                    "confidence": 0,
                    "status": "no_match",
                    "tmdb_id": "",
                    "media_type": media_type,
                    "tmdb_url": "",
                    "year": "",
                    "genres": "",
                    "stars": "",
                    "released_by": "",
                    "vision_by_label": "",
                    "vision_by": "",
                    "poster_path": "",
                    "candidate_rank": 0,
                    "candidate_ids": "",
                })

    if os.path.exists(needs_review_path):
        with open(needs_review_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                input_type = row.get("type", "unknown")
                media_type = _parse_media_type(input_type)
                conf = float(row.get("confidence", 0))
                candidate_ids_str = row.get("candidate_ids", "")
                candidate_ids = [cid.strip() for cid in candidate_ids_str.split(";") if cid.strip()]

                for rank, cid in enumerate(candidate_ids, start=1):
                    enrichment = _enrich(client, media_type, cid, cache)

                    rows.append({
                        "source_file": "needs_review.csv",
                        "review_reason": "ambiguous_candidates",
                        "original_row_id": row.get("original_row_id", ""),
                        "original_confidence": conf,
                        "input_title": row.get("title", ""),
                        "input_type": input_type,
                        "confidence": conf,
                        "status": "candidate",
                        "tmdb_id": cid,
                        "media_type": media_type,
                        "tmdb_url": _build_tmdb_url(media_type, cid),
                        "year": enrichment["year"],
                        "genres": enrichment["genres"],
                        "stars": enrichment["stars"],
                        "released_by": enrichment["released_by"],
                        "vision_by_label": enrichment["vision_by_label"],
                        "vision_by": enrichment["vision_by"],
                        "poster_path": enrichment["poster_path"],
                        "candidate_rank": rank,
                        "candidate_ids": candidate_ids_str,
                    })

    with open(review_queue_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    logging.info(f"Wrote {len(rows)} rows to {review_queue_path}")
    return len(rows)
