"""``doctor``: comprueba los adaptadores contra el código real, sin nube (parte A4).

Cada paso dice qué función del repo resolvió y con qué firma, para que un desajuste se
corrija en ``adapters.py`` antes de gastar tiempo en una corrida completa.
"""

from __future__ import annotations

import datetime as dt
import importlib
import json
import traceback
from typing import Any, Callable

from historical.plan import UTC

OK, AVISO, FALLA = "OK", "AVISO", "FALLA"

MODULES = ("yaml", "faker", "pyarrow", "jsonschema", "referencing", "simulator.factory",
           "simulator.sim_config", "simulator.generator.brands", "common.envelope",
           "common.enriched_row", "common.parquet_rows", "common.hmac_key", "common.hashing")


def _synthetic_samples() -> list[dict[str, Any]]:
    """Envelopes enriquecidos sintéticos (dentro de la imagen de NLP no está el simulador)."""
    from historical.testing import FakeEnvelopes, make_fake_enricher

    envs = FakeEnvelopes(3101)
    enrich = make_fake_enricher("doctor", dt.datetime.now(UTC))
    now = dt.datetime(2026, 6, 1, 15, 30, tzinfo=UTC)
    return [enrich(envs.envelope(s, i, now)) for s in ("reddit", "facebook", "instagram")
            for i in range(2)]


def bench_workers(spec: Any, cpus: int) -> list[int]:
    """``"2,4"`` → ``[2, 4]``: procesos de las pruebas en paralelo (al menos 2, como mucho
    los núcleos, sin repetir)."""
    out: list[int] = []
    for part in str(spec or "").replace(" ", "").split(","):
        if not part:
            continue
        k = max(2, min(int(part), max(2, cpus)))
        if k not in out:
            out.append(k)
    return out


def real_rows(root: Any, seed: int, n: int, to_row: Callable[[Any], dict[str, Any]]
              ) -> list[dict[str, Any]] | None:
    """Hasta ``n`` filas reales de ``stage1/`` (unas pocas de cada archivo, para mezclar fuentes
    y largos de texto), o ``None`` si todavía no hay etapa 1. El costo del NLP crece con el largo
    del texto, así que medir con textos sintéticos cortos sobreestima la velocidad (v2.28)."""
    from historical.work import WorkDir, read_jsonl_gz

    files = WorkDir(root, seed).stage1_files()
    if not files:
        return None
    per_file = max(1, -(-n // len(files)))
    rows: list[dict[str, Any]] = []
    for path in files:
        for env in read_jsonl_gz(path)[:per_file]:
            rows.append(to_row(env))
        if len(rows) >= n:
            break
    return rows[:n] or None


def run(args) -> dict[str, Any]:
    steps: list[dict[str, Any]] = []
    ctx: dict[str, Any] = {}

    def step(sid: str, name: str, fn: Callable[[], tuple[str, str, dict]]) -> None:
        try:
            status, detail, data = fn()
        except Exception as exc:  # noqa: BLE001 - el doctor nunca se cae
            status, detail = FALLA, f"{type(exc).__name__}: {exc}"[:600]
            data = {"traceback": traceback.format_exc()[-2000:]}
        steps.append({"id": sid, "name": name, "status": status, "detail": detail, "data": data})

    def imports():
        missing = {}
        for m in MODULES:
            try:
                importlib.import_module(m)
            except Exception as exc:  # noqa: BLE001
                missing[m] = f"{type(exc).__name__}: {exc}"[:200]
        if missing:
            return FALLA, f"no importan: {', '.join(missing)}", missing
        return OK, f"{len(MODULES)} módulos importan", {}

    def factory():
        from historical import adapters

        seed = args.seed or 3101
        env = adapters.SimulatorEnvelopes.build(seed)
        ctx["envelopes"] = env
        report = env.self_check()
        return OK, f"{env.how}; semilla {seed} aplicada; caos apagado; origin nativo " \
                   f"{env.native_origin!r}", {"describe": env.describe(), "self_check": report}

    def key():
        from historical import adapters

        info = adapters.load_key(args.key_source, gcp_project=args.gcp_project,
                                 secret_name=args.hmac_secret_name, env_var=args.key_env)
        ctx["key"] = info
        return OK, (f"{info.detail} (huella {info.fingerprint()}, "
                    f"{type(info.value).__name__} de {len(info.value)} caracteres)"
                    + (f"; intentos previos: {'; '.join(info.attempts)}" if info.attempts else "")), \
            {"attempts": list(info.attempts)}

    def enrich():
        from historical import adapters
        from historical.schema import SchemaUnavailable, load_validators
        from historical.work import find_identity_keys

        envs = ctx["envelopes"]
        now = dt.datetime(2026, 6, 1, 15, 30, tzinfo=UTC)
        enricher = adapters.make_enricher(ctx["key"].value, dt.datetime.now(UTC))
        try:
            validators = load_validators()
        except SchemaUnavailable as exc:
            validators = None
            note = f"sin validación ({exc})"
        else:
            note = "envelope.v1 y envelope.enriched.v1 OK"
        samples = []
        for source in ("reddit", "facebook", "instagram"):
            for seq in (0, 7, 31):
                env = envs.envelope(source, seq, now)
                if validators:
                    errs = validators["envelope"](env)
                    if errs:
                        return FALLA, f"{source}#{seq} no cumple envelope.v1: {errs[:2]}", {}
                enriched = enricher(env)
                leaks = find_identity_keys(enriched)
                if leaks:
                    return FALLA, f"identidad en claro tras enrich_envelope(): {leaks}", {}
                for f, v in (("sentiment_label", "positive"), ("sentiment_score", 0.9),
                             ("emotion_label", "joy"), ("emotion_score", 0.8),
                             ("nlp_model_version", "robertuito-sentiment@aaaaaaaaaaaa+"
                                                   "robertuito-emotion@bbbbbbbbbbbb")):
                    enriched[f] = v
                if validators:
                    errs = validators["enriched"](enriched)
                    if errs:
                        return FALLA, f"{source}#{seq} enriquecido no cumple el schema: {errs[:2]}", {}
                samples.append(enriched)
        ctx["samples"] = samples
        return OK, f"{len(samples)} eventos anonimizados con {enricher.describe}; {note}", \
            {"example": {k: samples[0].get(k) for k in ("event_id", "origin", "author", "likes",
                                                        "shares", "comments", "process_time")}}

    def parquet():
        import io

        import pyarrow.parquet as pq

        from historical import adapters

        rows = [adapters.to_row(e) for e in ctx["samples"]]
        data = adapters.rows_to_parquet(rows)
        table = pq.read_table(io.BytesIO(data))
        cols = adapters.posts_enriched_columns()
        if list(table.schema.names) != cols:
            return FALLA, f"columnas {table.schema.names} != POSTS_ENRICHED_COLUMNS {cols}", {}
        nlp_ok = all(r.get("sentiment_label") == "positive" for r in table.to_pylist())
        if not nlp_ok:
            return FALLA, "las columnas de NLP no pasan del envelope a la fila", {}
        types = {c: str(table.schema.field(c).type) for c in ("event_time", "brands", "payload")}
        return OK, f"{table.num_rows} filas, {len(cols)} columnas en orden; {types}", \
            {"row_example": json.loads(json.dumps(rows[0], default=str))}

    def nlp():
        from historical import adapters

        samples = ctx.get("samples") or _synthetic_samples()
        info = adapters.nlp_model_info()
        if not args.nlp_run:
            from apache_beam.options.pipeline_options import PipelineOptions

            t = adapters.nlp_transform(PipelineOptions(["--runner=BundleBasedDirectRunner"]))
            return OK, f"nlp_from_options() → {type(t).__name__}; {info}", info
        from historical.nlp_batch import BeamNlpEngine

        rows = [adapters.to_row(e) for e in samples[:3]]
        for r in rows:
            for f in adapters.NLP_FIELDS:
                r[f] = None
        out = BeamNlpEngine().annotate(rows)
        return OK, f"inferencia real: {out}", {"result": out, **info}

    def bench():
        import os
        import time

        from historical import adapters
        from historical.nlp_batch import BeamNlpEngine, batch_summary, parallel_bench
        from historical.testing import FakeEnvelopes, make_fake_enricher

        n = int(args.nlp_bench)
        seed = args.seed if getattr(args, "seed", None) is not None else \
            int(os.environ.get("HIST_SEED") or 3101)
        rows = None
        try:
            rows = real_rows(getattr(args, "root", None) or ".", seed, n, adapters.to_row)
        except Exception:  # noqa: BLE001 - si no se puede, textos sintéticos
            rows = None
        source = (f"filas reales de stage1 (semilla {seed})" if rows else
                  "textos sintéticos cortos: sobreestima la velocidad; corra `generate` antes "
                  "para medir con filas reales")
        if not rows:
            envs = FakeEnvelopes(3101)
            enrich = make_fake_enricher("bench", dt.datetime.now(UTC))
            base = dt.datetime(2026, 6, 1, 15, 30, tzinfo=UTC)
            rows = [adapters.to_row(enrich(envs.envelope(("reddit", "facebook", "instagram")[i % 3],
                                                        i, base))) for i in range(n)]
        for row in rows:
            for f in adapters.NLP_FIELDS:
                row[f] = None
        n = len(rows)
        lengths = sorted(len(str(r.get("text") or "")) for r in rows)
        env = {"rows": source, "text_chars_median": lengths[len(lengths) // 2] if lengths else None,
               "cpu_count": os.cpu_count(), "TA_NLP_THREADS": os.environ.get("TA_NLP_THREADS"),
               "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS"),
               "TA_NLP_BATCH_SECONDS": os.environ.get("TA_NLP_BATCH_SECONDS")}
        try:
            import torch

            env["torch_threads"] = torch.get_num_threads()
        except Exception as exc:  # noqa: BLE001
            env["torch_threads"] = f"{type(exc).__name__}"
        BeamNlpEngine().annotate(rows[:16])  # carga los modelos (caché del proceso)
        results = []
        for secs in (None, 0.0):
            label = f"1 proceso · espera {'por defecto' if secs is None else f'{secs:g} s'}"
            engine = BeamNlpEngine(batch_seconds=secs,
                                   batch_size=getattr(args, "batch_size", None))
            t0 = time.monotonic()
            try:
                out = engine.annotate(rows)
            except Exception as exc:  # noqa: BLE001
                results.append({"config": label, "error": f"{type(exc).__name__}: {exc}"[:300]})
                continue
            secs_taken = time.monotonic() - t0
            missing = sum(1 for r in rows if not (out.get(r["event_id"]) or {}).get("sentiment_label"))
            infer = engine.last_infer_seconds
            results.append({"config": label, "batch_seconds": secs, "workers": 1, "rows_per_s": round(n / secs_taken, 2),
                            "seconds": round(secs_taken, 1), "missing": missing,
                            "infer_share": round(infer / secs_taken, 2) if infer else None,
                            "batches": batch_summary(engine.last_batches)})
        for workers in bench_workers(getattr(args, "bench_workers", "2,4"), os.cpu_count() or 2):
            par = parallel_bench(rows, workers)
            label = f"{workers} procesos × {par.get('threads')} hilos"
            if "error" in par:
                results.append({"config": label, "error": par["error"]})
                continue
            results.append({"config": label, "batch_seconds": None,
                            "workers": workers, "rows_per_s": par["rows_per_s"],
                            "seconds": par["seconds"], "missing": n - par["rows"],
                            "infer_share": None, "batches": par["batches"]})
        ok = [r for r in results if "rows_per_s" in r and r["missing"] == 0]
        best = max(ok, key=lambda r: r["rows_per_s"]) if ok else None
        parts = [(f"{r['config']}: {r['rows_per_s']} filas/s (lote medio "
                  f"{r['batches'].get('mean_size')}"
                  + (f", inferencia {r['infer_share']:.0%} del tiempo" if r.get("infer_share") else "")
                  + (f", ¡{r['missing']} filas sin NLP!" if r.get("missing") else "")
                  + ")") if "rows_per_s" in r else
                 f"{r['config']}: ERROR {r['error']}" for r in results]
        if best:
            flags = (["--batch-seconds", f"{best['batch_seconds']:g}"]
                     if best["batch_seconds"] is not None else []) + \
                (["--workers", str(best["workers"])] if best.get("workers", 1) > 1 else [])
            parts.append("mejor: " + best["config"] + (f" → use {' '.join(flags)}" if flags else
                                                         " (valores por defecto)"))
        return (OK if best else FALLA), "; ".join(parts) + f"; entorno {env}", \
            {"results": results, "env": env, "best": best}

    nlp_only = getattr(args, "nlp_only", False)
    if not nlp_only:
        step("D1", "Importaciones", imports)
        step("D2", "Simulador (EventFactory)", factory)
        step("D3", "Llave HMAC", key)
        if "envelopes" in ctx and "key" in ctx:
            step("D4", "Anonimización y contratos", enrich)
        if "samples" in ctx:
            step("D5", "Fila y Parquet", parquet)
    if args.nlp or args.nlp_run or nlp_only:
        step("D6", "NLP (streaming.nlp)", nlp)
    if getattr(args, "nlp_bench", 0):
        step("D7", f"Velocidad del NLP ({int(args.nlp_bench)} filas por configuración)", bench)
    status = OK
    if any(s["status"] == FALLA for s in steps):
        status = FALLA
    elif any(s["status"] == AVISO for s in steps):
        status = AVISO
    expected = ((0 if nlp_only else 5) + (1 if (args.nlp or args.nlp_run or nlp_only) else 0)
                + (1 if getattr(args, "nlp_bench", 0) else 0))
    if len(steps) < expected:
        status = FALLA
    return {"status": status, "steps": steps}
