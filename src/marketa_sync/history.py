"""Validate durable target ownership before contacting either service."""
from collections import Counter
import json
from pathlib import Path

VARIANTS = ("cz", "en", "en_plain")


def audit_history(root: Path):
    owners = {}
    counts = {}
    for lane in VARIANTS:
        data = json.loads((root / f"variants/{lane}/state/sync.json").read_text())
        films = data["films"]
        stats = Counter()
        for film_id, row in films.items():
            for key in ("upload", "prepared", "previous_target_id"):
                value = row.get(key)
                if isinstance(value, dict):
                    value = value.get("target_video_id")
                    if row.get(key) and not value:
                        raise ValueError("Target record has no durable ID")
                if value:
                    identity = (lane, film_id)
                    target = str(value)
                    if target in owners and owners[target] != identity:
                        raise ValueError("Target ID belongs to more than one film variant")
                    owners[target] = identity
                    stats[key] += 1
            for attempt in row.get("attempts", []):
                target = attempt.get("target_video_id")
                if target and (str(target) not in owners):
                    # Historical attempts must remain protected by a live ID guard.
                    if not any(row.get(k) for k in ("upload", "prepared", "previous_target_id")):
                        raise ValueError("Historical target has lost its duplicate guard")
        counts[lane] = dict(stats)
    return {"unique_target_ids": len(owners), "variants": counts}
