"""Discover three independent film variants using only this project's catalog."""
from datetime import datetime, timedelta, timezone
import json
import time

from .catalog import EnglishTitles
from .discovery import EnglishProvider, display_name
from .history import VARIANTS
from .media.language import WhisperLanguageDetector
from .media.sdilej import PremiumRequiredError
from .media.subtitle_backfill import atomic_write


def providers_for(root, films, session, *, repair=False):
    detector = WhisperLanguageDetector()
    detector.audio_stream = "0:a:0"
    titles = EnglishTitles(root)
    names = {film.cr_film_id: titles.get(film.to_dict()) for film in films}
    providers = {
        "cz": EnglishProvider(session, detector, english_titles=names, czech_audio=True,
                              cache_path=root / "state/czech-candidate-scans.json"),
        "original": EnglishProvider(session, detector, english_titles=names,
                                    cache_path=root / "variants/en/state/candidate-scans.json",
                                    visual_report_path=root / "variants/en/state/visual-subtitles.json")}
    if repair:
        for provider in providers.values():
            provider.cache = {}
            provider.cache_path = None
            provider.visual_report_path = None
    return providers


def prepare_films(root, films, names, session, limit, runtime_minutes, persist=None,
                  providers=None, clock=time.monotonic):
    from .cli import make_pipeline
    providers = providers or providers_for(root, films, session)
    pipelines = {v: make_pipeline(root, v, names) for v in VARIANTS}
    progress_path = root / "state/film-discovery.json"
    progress = json.loads(progress_path.read_text()) if progress_path.exists() else {}
    deadline = clock() + runtime_minutes * 60
    ordered = sorted(films, key=lambda f: (progress.get(str(f.cr_film_id), {}).get("checked_at", ""),
                                          f.priority_rank or 10**9))
    attempted = 0
    for film in ordered:
        if attempted >= limit or clock() >= deadline:
            break
        key = str(film.cr_film_id)
        if progress.get(key, {}).get("retry_after", "") > datetime.now(timezone.utc).isoformat():
            continue
        paths = [f"variants/{v}/state/sync.json" for v in VARIANTS]
        histories = persist.read_remote_files(paths) if persist else {p: (root / p).read_text() for p in paths}
        wanted = []
        for lane, pipeline in pipelines.items():
            state = json.loads(histories[f"variants/{lane}/state/sync.json"])["films"].get(key, {})
            if (not pipeline.selected_sources.candidate(film.cr_film_id)
                    and not any(state.get(k) for k in ("upload", "prepared", "previous_target_id", "claim"))):
                wanted.append(lane)
        if not wanted:
            continue
        attempted += 1
        statuses = {}
        for kind, lanes in (("cz", [v for v in wanted if v == "cz"]),
                            ("original", [v for v in wanted if v != "cz"])):
            if not lanes or clock() >= deadline:
                continue
            provider = providers[kind]
            provider.discovery_timeout_seconds = min(600, max(1, deadline - clock()))
            try:
                if kind == "cz":
                    result = provider.discover(film)
                    selected = {"cz": result[0]} if result else {}
                else:
                    selected = provider.discover_variants(film)
                for lane in lanes:
                    candidate = selected.get(lane)
                    if candidate:
                        pipeline = pipelines[lane]
                        pipeline._record_verified_source(film, candidate, display_name(names[film.cr_film_id], lane, candidate))
                        pipeline.selected_sources.compact()
                        if persist:
                            persist(pipeline.selected_sources.path, "flush")
                    statuses[lane] = "verified" if candidate else "not_found"
            except PremiumRequiredError:
                raise
            except Exception as error:
                statuses.update({lane: type(error).__name__ for lane in lanes})
        now = datetime.now(timezone.utc)
        failed = any(s not in {"verified", "not_found"} for s in statuses.values())
        progress[key] = {"checked_at": now.isoformat(), "variants": statuses,
                         "retry_after": (now + timedelta(hours=1 if failed else 6)).isoformat()}
        atomic_write(progress_path, json.dumps(progress, sort_keys=True) + "\n")
        if persist:
            persist(progress_path, "flush")
        print(f"film_discovery film={key} variants={json.dumps(statuses, sort_keys=True)}", flush=True)
    return attempted
