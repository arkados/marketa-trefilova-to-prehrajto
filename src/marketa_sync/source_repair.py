"""An independent producer repairs missing sources without owning upload state."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import time

from .catalog import VariantSourceStore, source_candidate, stable_source
from .discovery import has_subtitles, allowed_audio
from .history import VARIANTS
from .media.models import MatchTier
from .media.ranking import quality_acceptable
from .media.subtitle_backfill import atomic_write

FEED_PATH = "state/source-repairs.json"
SEARCH_PATH = "state/source-repair-search.json"
POLICY = "verified-film-source-repair-v1"
LOCKED = ("upload", "prepared", "previous_target_id", "claim")


def load(path):
    return json.loads(path.read_text()) if path.exists() else {}


def fingerprint(record):
    """Public content checksum; contains no service authentication material."""
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def eligible(candidate, lane):
    if (candidate.match_tier not in {MatchTier.SOLID, MatchTier.STRONG}
            or not quality_acceptable(candidate) or (candidate.language_probability or 0) < 0.65):
        return False
    if lane == "cz":
        return candidate.audio_language == "cs"
    return allowed_audio(candidate) and has_subtitles(candidate) == (lane == "en")


def effective_source(row, manifest):
    selected = row.get("last_plan", {}).get("selected", {})
    if (row.get("source_repair", {}).get("replacement_source_id") == selected.get("source_id")
            and selected.get("url")):
        return {**selected, "source_url": selected["url"], "source_title": selected.get("title", "")}
    return manifest


def requests_for(histories, manifests, feed, progress, at=None):
    now = at or datetime.now(timezone.utc)
    for lane in VARIANTS:
        for film_id, row in histories[lane].get("films", {}).items():
            if any(row.get(k) for k in LOCKED):
                continue
            attempt = (row.get("attempts") or [{}])[-1]
            source = effective_source(row, manifests[lane].get(int(film_id)))
            if not source or attempt.get("status") != "source_refresh_failed":
                continue
            if str(attempt.get("source_id")) != str(source.get("source_id")):
                continue
            key = f"{lane}:{film_id}"
            previous = feed.get(key, {})
            if (previous.get("base_source_id") == str(source["source_id"])
                    and previous.get("base_source_url") == source["source_url"]):
                continue
            search = progress.get(key, {})
            if (search.get("base_source_id") == str(source["source_id"])
                    and search.get("retry_after", "") > now.isoformat()):
                continue
            yield lane, int(film_id), source


def apply_replacements(pipelines, feed):
    """The uploader alone accepts a replacement, under its checkpoint lock."""
    changed = 0
    for key, proof in feed.items():
        lane = proof.get("variant")
        film_id = proof.get("cr_film_id")
        if lane not in pipelines or key != f"{lane}:{film_id}" or proof.get("policy") != POLICY:
            continue
        source = proof.get("replacement", {})
        if not stable_source(source) or source.get("cr_film_id") != film_id:
            continue
        if any(k in source for k in ("download_url", "sample_url", "target_video_id")):
            continue
        try:
            candidate = source_candidate(source)
        except (ValueError, TypeError, KeyError):
            continue
        if not eligible(candidate, lane):
            continue
        pipeline = pipelines[lane]
        with pipeline.state._lock:
            row = pipeline.state.snapshot(film_id)
            current = effective_source(row, pipeline.selected_sources.get(film_id) or {})
            if any(row.get(k) for k in LOCKED):
                continue
            bound = (str(current.get("source_id")) == proof.get("base_source_id")
                     and current.get("source_url") == proof.get("base_source_url"))
            checksum = fingerprint(proof)
            if not bound and row.get("source_repair", {}).get("checksum") != checksum:
                continue
            if bound and (row.get("attempts") or [{}])[-1].get("status") != "source_refresh_failed":
                continue
            # Replace only the in-memory source. Producers exclusively own manifests.
            pipeline.selected_sources._rows[film_id] = dict(source)
            if row.get("source_repair", {}).get("checksum") != checksum:
                pipeline.state.film(film_id)["source_repair"] = {
                    "checksum": checksum, "base_source_id": proof["base_source_id"],
                    "replacement_source_id": candidate.source_id}
                pipeline.state.film(film_id).pop("source_repair_requested", None)
                pipeline.state.save("failure")
                changed += 1
    return changed


def repair_sources(root, films, names, providers, limit, runtime_minutes, persist=None,
                   poll_seconds=30, clock=time.monotonic, sleep=time.sleep):
    """Publish stable, fully verified alternatives; never write target histories."""
    feed = load(root / FEED_PATH)
    progress = load(root / SEARCH_PATH)
    film_map = {f.cr_film_id: f for f in films}
    deadline = clock() + runtime_minutes * 60
    checked = 0
    paths = [f"variants/{v}/{part}" for v in VARIANTS
             for part in ("state/sync.json", "manifests/selected-sources.jsonl")]
    while checked < limit and clock() < deadline:
        payloads = persist.read_remote_files(paths) if persist else {
            path: (root / path).read_text() for path in paths}
        histories = {v: json.loads(payloads[f"variants/{v}/state/sync.json"]) for v in VARIANTS}
        manifests = {v: {int(r["cr_film_id"]): r for line in
                        payloads[f"variants/{v}/manifests/selected-sources.jsonl"].splitlines()
                        if line.strip() and (r := json.loads(line))} for v in VARIANTS}
        pending = sorted(requests_for(histories, manifests, feed, progress),
                         key=lambda r: film_map[r[1]].priority_rank or 10**9
                         if r[1] in film_map else 10**9)
        if not pending:
            if not persist:
                break
            sleep(min(poll_seconds, max(0, deadline - clock())))
            continue
        for lane, film_id, source in pending:
            if checked >= limit or clock() >= deadline:
                break
            if film_id not in film_map:
                continue
            checked += 1
            provider = providers["cz" if lane == "cz" else "original"]
            provider.excluded_sources = {str(source["source_id"])}
            provider.discovery_timeout_seconds = min(600, max(1, deadline - clock()))
            key = f"{lane}:{film_id}"
            try:
                if lane == "cz":
                    result = provider.discover(film_map[film_id])
                    selected = result[0] if result else None
                else:
                    selected = provider.discover_variants(film_map[film_id]).get(lane)
                status = "verified" if selected and eligible(selected, lane) else "not_found"
                if status == "verified":
                    replacement = selected.to_dict()
                    replacement.update(cr_film_id=film_id, source_url=replacement.pop("url"),
                                       source_title=replacement.pop("title"),
                                       source_filename=replacement.pop("filename"),
                                       source_status="verified", upload_status="pending")
                    feed[key] = {"variant": lane, "cr_film_id": film_id, "policy": POLICY,
                                 "base_source_id": str(source["source_id"]),
                                 "base_source_url": source["source_url"], "replacement": replacement}
                    atomic_write(root / FEED_PATH, json.dumps(feed, ensure_ascii=False, sort_keys=True) + "\n")
            except Exception as error:
                from .media.sdilej import PremiumRequiredError
                if isinstance(error, PremiumRequiredError):
                    raise
                status = type(error).__name__
            now = datetime.now(timezone.utc)
            progress[key] = {"base_source_id": str(source["source_id"]), "status": status,
                             "checked_at": now.isoformat(),
                             "retry_after": (now + timedelta(hours=6 if status == "not_found" else 1)).isoformat()}
            atomic_write(root / SEARCH_PATH, json.dumps(progress, sort_keys=True) + "\n")
            if persist:
                persist(root / SEARCH_PATH, "flush")
            print(f"source_repair variant={lane} film={film_id} status={status}", flush=True)
    return checked
