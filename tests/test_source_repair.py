from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from marketa_sync.cli import make_pipeline, ordered_plans, checkpoint_paths
from marketa_sync.discovery import EnglishProvider, choose_english, display_name
from marketa_sync.history import VARIANTS, audit_history
from marketa_sync.media.models import Film
from marketa_sync.media.sdilej import SourceUnavailable
from marketa_sync.producer import prepare_films, providers_for
from marketa_sync.source_repair import (
    FEED_PATH, SEARCH_PATH, POLICY, apply_replacements, repair_sources, requests_for,
)
from test_marketa import candidate


def setup(root):
    film = Film(1, "film", "The Film", "The Film", 2000, 120, "en")
    pipelines = {v: make_pipeline(root, v, {1: "The Film"}) for v in VARIANTS}
    for p in pipelines.values():
        p.state.save("initial")
        p.selected_sources.compact()
    return film, pipelines


def failed(root):
    film, pipelines = setup(root)
    p = pipelines["cz"]
    p._record_verified_source(film, candidate("123", language="cs"), "The Film CZ Dabing")
    p.state.record_upload_failure(1, {"status": "source_refresh_failed", "source_id": "123"})
    return film, pipelines


def replacement(p, film, base="123", source="456"):
    c = candidate(source, language="cs")
    row = c.to_dict()
    row.update(cr_film_id=1, source_url=row.pop("url"), source_title=row.pop("title"),
               source_filename=row.pop("filename"), source_status="verified", upload_status="pending")
    return {"cz:1": {"variant": "cz", "cr_film_id": 1, "policy": POLICY,
                     "base_source_id": base, "base_source_url": f"https://sdilej.cz/{base}/film.mkv",
                     "replacement": row}}


def test_new_repair_bypasses_old_source_cooldown_but_repeated_failure_does_not(tmp_path):
    film, pipelines = failed(tmp_path)
    p = pipelines["cz"]
    assert ordered_plans(pipelines, [film], 3) == []
    feed = replacement(p, film)
    assert apply_replacements(pipelines, feed) == 1
    assert ordered_plans(pipelines, [film], 3)[0][1]["selected"]["source_id"] == "456"
    p.state.record_upload_failure(1, {"status": "source_refresh_failed", "source_id": "456"})
    assert apply_replacements(pipelines, feed) == 0
    assert ordered_plans(pipelines, [film], 3) == []
    histories = {v: x.state.data for v, x in pipelines.items()}
    manifests = {v: x.selected_sources._rows for v, x in pipelines.items()}
    assert list(requests_for(histories, manifests, feed, {}))[0][2]["source_id"] == "456"
    assert apply_replacements(pipelines, replacement(p, film, "456", "789")) == 1
    assert ordered_plans(pipelines, [film], 3)[0][1]["selected"]["source_id"] == "789"


@pytest.mark.parametrize("locked", ["upload", "prepared", "previous_target_id", "claim"])
def test_repair_never_changes_owned_or_inflight_source(tmp_path, locked):
    film, pipelines = failed(tmp_path)
    p = pipelines["cz"]
    p.state.film(1)[locked] = "777" if locked == "previous_target_id" else {"target_video_id": "777"}
    before = deepcopy(p.state.data)
    assert apply_replacements(pipelines, replacement(p, film)) == 0
    assert p.state.data == before
    assert p.selected_sources.candidate(1).source_id == "123"


@pytest.mark.parametrize("invalid", ["wrong_base", "wrong_film", "signed_url", "wrong_language"])
def test_invalid_feed_is_rejected(tmp_path, invalid):
    film, pipelines = failed(tmp_path)
    feed = replacement(pipelines["cz"], film)
    proof = feed["cz:1"]
    if invalid == "wrong_base":
        proof["base_source_id"] = "999"
    elif invalid == "wrong_film":
        proof["replacement"]["cr_film_id"] = 2
    elif invalid == "signed_url":
        proof["replacement"]["download_url"] = "https://invalid.test/?secret=private"
    else:
        proof["replacement"]["audio_language"] = "en"
    assert apply_replacements(pipelines, feed) == 0


def test_repair_worker_writes_only_its_feed_and_progress(tmp_path):
    film, pipelines = failed(tmp_path)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    c = candidate("456", language="cs")
    c.download_url = "https://invalid.test/?session=private"
    provider = SimpleNamespace(discover=lambda _: [c])
    assert repair_sources(tmp_path, [film], {1: "The Film"}, {"cz": provider}, 1, 1) == 1
    assert provider.excluded_sources == {"123"}
    assert all(p.read_bytes() == content for p, content in before.items())
    assert set(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*")
               if p.is_file() and p not in before) == {FEED_PATH, SEARCH_PATH}
    assert "private" not in (tmp_path / FEED_PATH).read_text()
    assert apply_replacements(pipelines, json.loads((tmp_path / FEED_PATH).read_text())) == 1


def test_three_variants_have_independent_quality_and_size_selection(monkeypatch):
    film = Film(1, "film", "The Film", "The Film", 2000, 120, "en")
    provider = EnglishProvider(None, None, english_titles={1: "The Film"})
    sub4k = candidate("123", width=3840, subtitles=True)
    plain4k = candidate("456", width=3840)
    plainlarge = candidate("789", width=3840)
    plainlarge.size_bytes *= 2
    subsmall = candidate("222", width=1920, subtitles=True)
    monkeypatch.setattr(provider, "search_by_quality", lambda _: [sub4k, plain4k, plainlarge, subsmall])
    monkeypatch.setattr(provider, "verify_english", lambda _, c: c)
    selected = provider.discover_variants(film)
    assert selected["en"].source_id == "123"
    assert selected["en_plain"].source_id == "456"
    assert display_name("The Film", "en", selected["en"]).endswith("CZ titulky")
    assert display_name("The Film", "en_plain", selected["en_plain"]) == "The Film 4K"


def test_removed_high_resolution_candidate_does_not_block_available_alternative(monkeypatch):
    film = Film(1, "film", "The Film", "The Film", 2000, 120, "en")
    provider = EnglishProvider(None, None, english_titles={1: "The Film"})
    old, new = candidate("123", width=3840), candidate("456")
    monkeypatch.setattr(provider, "search_by_quality", lambda _: [old, new])
    def verify(_, c):
        if c.source_id == "123":
            raise SourceUnavailable("Gone")
        return c
    monkeypatch.setattr(provider, "verify_english", verify)
    assert provider.discover_variants(film)["en_plain"].source_id == "456"


def test_standalone_producer_fills_all_lanes_without_writing_upload_histories(tmp_path):
    film, pipelines = setup(tmp_path)
    before = {v: p.state.path.read_bytes() for v, p in pipelines.items()}
    cz, sub, plain = candidate("123", language="cs"), candidate("456", subtitles=True), candidate("789")
    providers = {"cz": SimpleNamespace(discover=lambda _: [cz]),
                 "original": SimpleNamespace(discover_variants=lambda _: {"en": sub, "en_plain": plain})}
    assert prepare_films(tmp_path, [film], {1: "The Film"}, None, 1, 1, providers=providers) == 1
    for lane in VARIANTS:
        fresh = make_pipeline(tmp_path, lane, {1: "The Film"})
        assert fresh.selected_sources.candidate(1)
        assert fresh.state.path.read_bytes() == before[lane]
    plans = ordered_plans({v: make_pipeline(tmp_path, v, {1: "The Film"}) for v in VARIANTS}, [film], 3)
    assert len(plans) == 3
    assert next(row for lane, row in plans if lane == "en_plain")["needs_czech_subtitles"] is False


def test_repair_preparation_and_upload_checkpoint_ownership_do_not_overlap(tmp_path):
    sets = [set(checkpoint_paths(tmp_path, mode)) for mode in ("repair-sources", "prepare", "continuous")]
    assert not sets[0] & sets[1] and not sets[0] & sets[2] and not sets[1] & sets[2]


def test_failed_refresh_queues_repair_and_continues_next_transfer(tmp_path, monkeypatch):
    from dataclasses import replace
    from marketa_sync.cli import drain_queue
    film, pipelines = setup(tmp_path)
    pipeline = pipelines["cz"]
    second = replace(film, cr_film_id=2, priority_rank=2)
    pipeline.name_builder = lambda f, _: f"The Film {f.cr_film_id} CZ Dabing"
    for f, source in [(film, "123"), (second, "456")]:
        pipeline._record_verified_source(f, candidate(source, language="cs"), "Film")
    monkeypatch.setattr(pipeline, "_target_id_by_name", lambda *_: None)
    def refresh(c, **kwargs):
        if c.source_id == "123":
            raise SourceUnavailable("Removed")
        return c
    monkeypatch.setattr(pipeline.source_provider, "refresh_approved", refresh)
    def relay(_source, _target, c, name, description, on_prepared, **kwargs):
        on_prepared("777", c.size_bytes)
        return SimpleNamespace(video_id="777", size_bytes=c.size_bytes, completed=True)
    monkeypatch.setattr("marketa_sync.media.pipeline.relay_upload", relay)
    monkeypatch.setattr(pipeline, "_target_confirmed", lambda *_: True)
    counts = drain_queue(tmp_path, {"cz": pipeline}, [film, second], limit=2, runtime_minutes=1)
    assert counts["failed"] == 1 and counts["uploaded"] == 1
    assert pipeline.state.snapshot(1)["source_repair_requested"]["source_id"] == "123"
    assert pipeline.state.uploaded(2)


def test_missing_history_or_duplicate_target_fails_before_service_login(tmp_path):
    film, pipelines = setup(tmp_path)
    pipelines["cz"].state.record_success(1, {"target_video_id": "777"})
    pipelines["en"].state.record_success(1, {"target_video_id": "777"})
    with pytest.raises(ValueError, match="more than one"):
        audit_history(tmp_path)


def test_japanese_original_title_uses_japanese_romanization():
    from marketa_sync.catalog import combined_title
    title = combined_title("Princess Mononoke", "もののけ姫", "ja")
    assert title == "Princess Mononoke (mononoke hime)"
