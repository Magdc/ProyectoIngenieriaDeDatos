"""Etapa 1 · ``generate``: envelopes del simulador → aceptación → anonimización → ``stage1/``.

Para cada fuente recorre los huecos de ``plan.slot_times()`` y, para cada uno, los
``seq`` del simulador hasta aceptar uno (``TrendModel.acceptance``). Cada evento
aceptado se valida contra ``envelope.v1`` (opcional), se anonimiza con
``enrich_envelope()`` (llave HMAC del stream) y se guarda **ya anonimizado** en
``stage1/{source}-{k:04d}.jsonl.gz``. Nada con ``user_id`` o ``handle`` toca el disco.

El *digest* (sha256 de ``source|seq|event_id|event_time`` en orden) identifica el
dataset: con la misma semilla, configuración y ``as_of`` sale el mismo digest (HU-024:
reejecutable sin duplicar).
"""

from __future__ import annotations

import collections
import datetime as dt
import hashlib
import time
from typing import Any, Callable, Mapping, Protocol

from historical import TOOL_ID
from historical.plan import (UTC, HistoricalConfig, TrendModel, accept_draw, format_ts,
                             plan_summary, slot_times, split_counts)
from historical.work import (HEX64_CHECK, WorkDir, atomic_write_bytes, atomic_write_json,
                             find_identity_keys, jsonl_gz_bytes, log_event, reset_dir,
                             sha256_bytes)


class EnvelopeSource(Protocol):
    kind: str

    def envelope(self, source: str, seq: int, now: dt.datetime) -> dict[str, Any]: ...

    def describe(self) -> dict[str, Any]: ...


class GenerateError(RuntimeError):
    pass


def _brand_label(env: Mapping[str, Any]) -> tuple[str | None, str | None]:
    brands = env.get("brands") or []
    sim = env.get("sim") or {}
    return (brands[0] if brands else None), sim.get("label_sentiment")


def accepted_events(cfg: HistoricalConfig, as_of: dt.datetime, envelopes: EnvelopeSource,
                    source: str, count: int, stats: dict[str, Any]):
    """Genera ``(seq, envelope)`` aceptados de una fuente, en orden cronológico."""
    model = TrendModel(cfg, as_of)
    seq = 0
    forced = 0
    candidates = 0
    for slot in slot_times(model, source, count):
        attempts = 0
        while True:
            env = envelopes.envelope(source, seq, slot)
            brand, label = _brand_label(env)
            this_seq = seq
            seq += 1
            attempts += 1
            candidates += 1
            if accept_draw(cfg.seed, source, this_seq) < model.acceptance(brand, label, slot):
                break
            if attempts >= cfg.max_candidates_per_slot:
                forced += 1
                break
        yield this_seq, env
    stats["candidates"] = candidates
    stats["forced"] = forced
    stats["last_seq"] = seq - 1


def generate(cfg: HistoricalConfig, as_of: dt.datetime, work: WorkDir, *,
             envelopes: EnvelopeSource,
             enrich: Callable[[Mapping[str, Any]], dict[str, Any]] | None,
             key_info: Mapping[str, Any] | None = None,
             validate_envelope: Callable[[Mapping[str, Any]], list[str]] | None = None,
             key_check: Callable[[list[str]], dict[str, Any]] | None = None,
             process_time: dt.datetime | None = None,
             digest_only: bool = False,
             progress_every: int = 5000) -> dict[str, Any]:
    """Corre la etapa 1. Con ``digest_only`` no anonimiza ni escribe: solo calcula el digest."""
    t0 = time.monotonic()
    process_time = process_time or dt.datetime.now(UTC)
    counts = split_counts(cfg.n, cfg.sources)
    digest = hashlib.sha256()
    per_source: dict[str, Any] = {}
    seqs: dict[str, list[int]] = {}
    hash_counter: collections.Counter[str] = collections.Counter()
    user_to_hashes: dict[str, set[str]] = collections.defaultdict(set)
    hash_to_users: dict[str, set[str]] = collections.defaultdict(set)
    invalid: list[dict[str, Any]] = []
    stage_files: list[dict[str, Any]] = []
    tmp_dir = work.dir / "stage1.tmp"
    if not digest_only:
        if enrich is None:
            raise GenerateError("falta el anonimizador (enrich)")
        reset_dir(tmp_dir)
    done = 0
    for source, count in counts.items():
        st: dict[str, Any] = {"target": count}
        brands: collections.Counter[str] = collections.Counter()
        labels: collections.Counter[str] = collections.Counter()
        kinds: collections.Counter[str] = collections.Counter()
        users: set[str] = set()
        buffer: list[dict[str, Any]] = []
        chunk = 0
        seq_list: list[int] = []
        first_ts = last_ts = None
        for seq, env in accepted_events(cfg, as_of, envelopes, source, count, st):
            seq_list.append(seq)
            digest.update(f"{source}|{seq}|{env['event_id']}|{env['event_time']}\n".encode())
            brand, label = _brand_label(env)
            brands[brand or "<sin marca>"] += 1
            labels[label or "null"] += 1
            kinds[str((env.get("payload") or {}).get("kind"))] += 1
            first_ts = first_ts or env["event_time"]
            last_ts = env["event_time"]
            if validate_envelope is not None:
                errors = validate_envelope(env)
                if errors and len(invalid) < 20:
                    invalid.append({"event_id": env["event_id"], "source": source,
                                    "seq": seq, "errors": errors})
                if errors:
                    st["invalid"] = st.get("invalid", 0) + 1
            if not digest_only:
                author = env.get("author") or {}
                user_id = author.get("user_id")
                enriched = enrich(env)
                leaks = find_identity_keys(enriched)
                if leaks:
                    raise GenerateError(f"identidad en claro tras anonimizar ({leaks[:3]}) en "
                                        f"{env['event_id']}: revise enrich_envelope()")
                uh = (enriched.get("author") or {}).get("user_hash")
                if not isinstance(uh, str) or not HEX64_CHECK.match(uh):
                    raise GenerateError(f"user_hash inválido en {env['event_id']}: {uh!r}")
                if enriched.get("origin") != "historical":
                    raise GenerateError(f"origin={enriched.get('origin')!r} tras anonimizar")
                hash_counter[uh] += 1
                if user_id is not None:
                    users.add(str(user_id))
                    user_to_hashes[str(user_id)].add(uh)
                    hash_to_users[uh].add(str(user_id))
                buffer.append(enriched)
                if len(buffer) >= cfg.chunk_size:
                    stage_files.append(_flush(tmp_dir, source, chunk, buffer))
                    chunk += 1
                    buffer = []
            done += 1
            if progress_every and done % progress_every == 0:
                log_event("hist_generate_progress", rows=done, total=cfg.n,
                          seconds=round(time.monotonic() - t0, 1))
        if buffer and not digest_only:
            stage_files.append(_flush(tmp_dir, source, chunk, buffer))
        seqs[source] = seq_list
        st.update({"accepted": len(seq_list),
                   "acceptance": round(len(seq_list) / st["candidates"], 4) if st.get("candidates") else None,
                   "brands": dict(brands.most_common()), "labels": dict(labels.most_common()),
                   "kinds": dict(kinds.most_common()), "first_event_time": first_ts,
                   "last_event_time": last_ts})
        if not digest_only:
            st["users"] = len(users)
            st["user_id_min"] = min(users) if users else None
            st["user_id_max"] = max(users) if users else None
        per_source[source] = st
    result: dict[str, Any] = {
        "tool": TOOL_ID,
        "lote": "3.1",
        "plan": plan_summary(cfg, as_of),
        "config": cfg.to_dict(),
        "digest": digest.hexdigest(),
        "sources": per_source,
        "rows": done,
        "invalid_examples": invalid,
        "invalid": sum(s.get("invalid", 0) for s in per_source.values()),
        "envelopes": envelopes.describe(),
        "seconds": round(time.monotonic() - t0, 1),
    }
    if digest_only:
        return result
    multi_hash = sum(1 for hs in user_to_hashes.values() if len(hs) > 1)
    multi_user = sum(1 for us in hash_to_users.values() if len(us) > 1)
    result["users"] = {"distinct_user_ids": len(user_to_hashes),
                       "distinct_user_hashes": len(hash_counter),
                       "user_ids_with_several_hashes": multi_hash,
                       "hashes_with_several_user_ids": multi_user}
    if multi_hash or multi_user:
        reset_dir(tmp_dir)
        raise GenerateError(f"user_hash no es 1 a 1 con user_id ({multi_hash} / {multi_user})")
    if result["invalid"]:
        result["status"] = "invalid"
        reset_dir(tmp_dir)
        tmp_dir.rmdir()
        raise GenerateError(f"{result['invalid']} envelopes no cumplen envelope.v1; ejemplos: "
                            f"{invalid[:2]}")
    if key_check is not None:
        top = [h for h, _ in hash_counter.most_common(500)]
        result["key_check"] = key_check(top)
        if result["key_check"].get("status") == "FALLA":
            reset_dir(tmp_dir)
            tmp_dir.rmdir()
            raise GenerateError("la llave HMAC no coincide con la del stream: "
                                f"{result['key_check'].get('detail')}")
    result["key"] = dict(key_info or {})
    result["process_time"] = format_ts(process_time)
    result["stage1"] = stage_files
    result["seqs"] = seqs
    result["generated_at"] = format_ts(dt.datetime.now(UTC))
    # Publica la etapa: stage1.tmp → stage1 y borra lo que dependía de la anterior.
    reset_dir(work.stage1)
    work.stage1.rmdir()
    tmp_dir.rename(work.stage1)
    for stale in (work.stage2, ):
        if stale.exists():
            reset_dir(stale)
    for path in (work.output_json, work.nlp_json):
        if path.exists():
            path.unlink()
    atomic_write_json(work.generate_json, result)
    log_event("hist_generate_done", rows=done, digest=result["digest"],
              seconds=result["seconds"], files=len(stage_files))
    return result


def _flush(tmp_dir, source: str, chunk: int, rows: list[dict[str, Any]]) -> dict[str, Any]:
    name = f"{source}-{chunk:04d}.jsonl.gz"
    data = jsonl_gz_bytes(rows)
    atomic_write_bytes(tmp_dir / name, data)
    return {"file": name, "rows": len(rows), "sha256": sha256_bytes(data)}


def same_plan(previous: Mapping[str, Any] | None, cfg: HistoricalConfig, as_of: dt.datetime) -> bool:
    if not previous:
        return False
    plan = previous.get("plan") or {}
    return (plan.get("config_sha256") == cfg.sha256() and plan.get("as_of") == format_ts(as_of))
