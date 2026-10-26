"""Etapa 2 · ``nlp``: sentimiento y emoción en lote con el NLP del pipeline (§3.15).

Corre **dentro de la imagen** ``ta-dataflow-nlp-local:local`` (modelos horneados, torch,
pysentimiento), lanzada por ``scripts/historical_nlp.sh``; no necesita credenciales.

Motor ``beam`` (el de verdad): por cada archivo de ``stage1/`` arma un pipeline batch
con el ``BundleBasedDirectRunner`` (el mismo runner local del Lote 2.3) y el PTransform
que devuelve ``streaming.nlp.nlp_from_options()``, es decir, **el mismo** RunInference,
preprocesado, mapeo de etiquetas y ``nlp_model_version`` que el stream. Las filas de
entrada son las de ``posts_enriched`` (``to_posts_enriched_row``), como en el pipeline.

Reanudable: un archivo de ``stage2/`` con su resumen ``.json`` (mismo sha256 de
``stage1`` y sin filas sin NLP) no se repite. Con ``--retry-failed`` (por defecto) las
filas que salieron sin NLP se reintentan una vez.
"""

from __future__ import annotations

import collections
import functools
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from historical.adapters import NLP_FIELDS, row_of
from historical.work import (WorkDir, atomic_write_bytes, atomic_write_json, jsonl_gz_bytes,
                             log_event, read_jsonl_gz, sha256_bytes, sha256_file)


class NlpEngine(Protocol):
    name: str

    def annotate(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        """``{event_id: {campo de NLP: valor}}`` para las filas dadas."""


class NlpStageError(RuntimeError):
    pass


# Resultados por corrida. El BundleBasedDirectRunner ejecuta en este mismo proceso, así que un Map puede dejar aquí cada fila sin pasar por un
# GroupByKey: con lotes por tiempo, el último lote incompleto sale con el temporizador después
# de que la marca de agua llegó al final, y un GroupByKey posterior (p. ej. el de WriteToText)
# lo rechaza en el BundleBasedDirectRunner (AssertionError en _GroupByKeyOnlyEvaluator).
_RESULTS: dict[str, dict[str, dict[str, Any]]] = {}


def _collect(element: Any, run_id: str) -> None:
    row = row_of(element)
    _RESULTS[run_id][str(row.get("event_id"))] = {f: _plain(row.get(f)) for f in NLP_FIELDS}


def _plain(value: Any) -> Any:
    """Valores JSON (los scores pueden llegar como tipos de numpy)."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return json.loads(json.dumps(value, default=str))


BATCH_SIZES: "collections.Counter[int]" = collections.Counter()
INFER_SECONDS = [0.0]
_PATCHED = {"done": False, "ok": False}


def count_batches() -> bool:
    """Cuenta el tamaño de cada lote que llega a ``RunInference`` (diagnóstico).

    Envuelve ``_RunInferenceDoFn.process`` de Beam en este proceso; si la clase no existe o
    cambia, no hace nada y el conteo queda vacío.
    """
    if _PATCHED["done"]:
        return _PATCHED["ok"]
    _PATCHED["done"] = True
    try:
        from apache_beam.ml.inference import base

        cls = base._RunInferenceDoFn
        original = cls.process

        @functools.wraps(original)
        def process(self, batch, *args, **kwargs):
            try:
                BATCH_SIZES[len(batch)] += 1
            except TypeError:
                pass
            t0 = time.monotonic()
            try:
                return original(self, batch, *args, **kwargs)
            finally:
                INFER_SECONDS[0] += time.monotonic() - t0

        cls.process = process
        _PATCHED["ok"] = True
    except Exception:  # noqa: BLE001 - solo diagnóstico
        _PATCHED["ok"] = False
    return _PATCHED["ok"]


RUNNERS = ("bundle",)

FNAPI_UNSAFE = (
    "el FnApiRunner no sirve con el NLP del stream: agrupa en lotes con estado y temporizador "
    "(BatchElements con max_batch_duration_secs) en dos pasos encadenados, y el FnApiRunner "
    "toma como hora de disparo del temporizador la marca de tiempo del elemento; las filas del "
    "último lote parcial, que salen con el temporizador de ventana, llevan la marca máxima, así "
    "que el segundo lote parcial queda 'time-pending' para siempre: o el pipeline falla "
    "(AssertionError: time-pending bundles did not execute) o pierde esas filas sin avisar. "
    "Use el runner por defecto (bundle) y, para ir más rápido, --workers")


class BeamNlpEngine:
    """RunInference del Lote 2.3 en un pipeline batch local.

    Siempre con el ``BundleBasedDirectRunner`` (el del modo local del Lote 2.3); el
    ``FnApiRunner`` se descartó (ver ``FNAPI_UNSAFE``). ``batch_seconds`` = ``None`` deja la espera por
    defecto del pipeline (``TA_NLP_BATCH_SECONDS``/``--nlp_batch_seconds``, 5 s): en un
    pipeline batch los lotes se llenan enseguida, y forzar 0 puede hacer que el pipeline
    no espere a llenar el lote y lo deje en 1.
    """

    name = "beam"

    def __init__(self, batch_size: int | None = None, batch_seconds: float | None = None,
                 tmp_dir: str | None = None, runner: str = "bundle"):
        if runner == "fnapi":
            raise ValueError(FNAPI_UNSAFE)
        if runner not in RUNNERS:
            raise ValueError(f"runner desconocido {runner!r} ({', '.join(RUNNERS)})")
        self.batch_size = batch_size
        self.batch_seconds = batch_seconds
        self.tmp_dir = tmp_dir
        self.runner = runner
        self._described: dict[str, Any] | None = None
        self.last_batches: dict[int, int] = {}
        self.last_infer_seconds: float | None = None

    def _options(self):
        from apache_beam.options.pipeline_options import PipelineOptions

        argv = ["--runner=BundleBasedDirectRunner", "--nlp_enabled=auto"]
        if self.batch_seconds is not None:
            argv.append(f"--nlp_batch_seconds={self.batch_seconds:g}")
        if self.batch_size:
            argv.append(f"--nlp_batch_size={int(self.batch_size)}")
        return PipelineOptions(argv)

    def _runner(self):
        from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner

        return BundleBasedDirectRunner()

    def annotate(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        previous_env = os.environ.get("TA_NLP_BATCH_SECONDS")
        if self.batch_seconds is not None:
            os.environ["TA_NLP_BATCH_SECONDS"] = f"{self.batch_seconds:g}"
        try:
            return self._annotate(rows)
        finally:
            if previous_env is None:
                os.environ.pop("TA_NLP_BATCH_SECONDS", None)
            else:
                os.environ["TA_NLP_BATCH_SECONDS"] = previous_env

    def _annotate(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        import apache_beam as beam

        from historical.adapters import nlp_transform

        counting = count_batches()
        before = collections.Counter(BATCH_SIZES)
        infer_before = INFER_SECONDS[0]
        options = self._options()
        transform = nlp_transform(options)
        if self._described is None:
            self._described = {"transform": type(transform).__name__}
        run_id = uuid.uuid4().hex
        _RESULTS[run_id] = {}
        try:
            with beam.Pipeline(runner=self._runner(), options=options) as p:
                (p
                 | "Filas" >> beam.Create([dict(r) for r in rows], reshuffle=False)
                 | "NLP" >> transform
                 | "Recoger" >> beam.Map(_collect, run_id))
            out = dict(_RESULTS[run_id])
        finally:
            _RESULTS.pop(run_id, None)
        if counting:
            diff = collections.Counter(BATCH_SIZES)
            diff.subtract(before)
            self.last_batches = {k: v for k, v in sorted(diff.items()) if v > 0}
            self.last_infer_seconds = round(INFER_SECONDS[0] - infer_before, 2)
        return out

    def describe(self) -> dict[str, Any]:
        info = {"engine": self.name, "runner": self.runner, "batch_size": self.batch_size,
                "batch_seconds": self.batch_seconds}
        info.update(self._described or {})
        return info


def batch_summary(batches: Mapping[int, int]) -> dict[str, Any]:
    """Lotes por tamaño → número de lotes, tamaño medio y los tamaños más comunes."""
    n = sum(batches.values())
    rows = sum(int(k) * v for k, v in batches.items())
    top = sorted(batches.items(), key=lambda kv: -kv[1])[:3]
    return {"batches": n, "mean_size": round(rows / n, 2) if n else None,
            "top": {int(k): v for k, v in top}}


def _missing(rec: Mapping[str, Any] | None) -> bool:
    return not rec or rec.get("sentiment_label") is None or rec.get("emotion_label") is None


def pending_files(work: WorkDir, *, force: bool = False) -> tuple[list[Path], int]:
    """Archivos de ``stage1/`` sin un ``stage2/`` completo y cuántos ya estaban."""
    pending, skipped = [], 0
    for path in work.stage1_files():
        side = work.stage2 / f"{path.name}.json"
        out = work.stage2 / path.name
        if not force and side.exists() and out.exists():
            meta = json.loads(side.read_text(encoding="utf-8"))
            if meta.get("stage1_sha256") == sha256_file(path) and meta.get("nlp_missing") == 0:
                skipped += 1
                continue
        pending.append(path)
    return pending, skipped


def process_file(work: WorkDir, engine: NlpEngine, path: Path, *,
                 to_row: Callable[[Mapping[str, Any]], dict[str, Any]],
                 retry_failed: bool = True, worker: int | None = None) -> dict[str, Any]:
    """NLP de un archivo de ``stage1/`` → ``stage2/`` (con su resumen ``.json``)."""
    t1 = time.monotonic()
    stage1_sha = sha256_file(path)
    enriched = read_jsonl_gz(path)
    rows = [to_row(e) for e in enriched]
    result = engine.annotate(rows)
    batches_raw = collections.Counter(getattr(engine, "last_batches", {}) or {})
    infer_s = getattr(engine, "last_infer_seconds", None) or 0.0
    missing = [r for r in rows if _missing(result.get(str(r.get("event_id"))))]
    retried = 0
    if missing and retry_failed:
        retried = len(missing)
        again = engine.annotate(missing)
        batches_raw.update(getattr(engine, "last_batches", {}) or {})
        infer_s += getattr(engine, "last_infer_seconds", None) or 0.0
        for eid, rec in again.items():
            if not _missing(rec):
                result[eid] = rec
    versions: dict[str, int] = {}
    n_missing = 0
    for e in enriched:
        rec = result.get(str(e["event_id"])) or {}
        for f in NLP_FIELDS:
            e[f] = rec.get(f)
        if _missing(rec):
            n_missing += 1
        v = e.get("nlp_model_version")
        versions[str(v)] = versions.get(str(v), 0) + 1
    data = jsonl_gz_bytes(enriched)
    atomic_write_bytes(work.stage2 / path.name, data)
    seconds = time.monotonic() - t1
    batches = batch_summary(batches_raw)
    batches["infer_seconds"] = round(infer_s, 1) if batches_raw else None
    meta = {"file": path.name, "rows": len(enriched), "stage1_sha256": stage1_sha,
            "nlp_batches": batches, "worker": worker,
            "sha256": sha256_bytes(data), "nlp_missing": n_missing, "retried": retried,
            "versions": versions,
            "engine": getattr(engine, "describe", lambda: {"engine": engine.name})(),
            "seconds": round(seconds, 1),
            "rows_per_s": round(len(enriched) / seconds, 2) if seconds > 0 else None}
    atomic_write_json(work.stage2 / f"{path.name}.json", meta)
    return meta


def worker_threads(workers: int) -> int:
    """Hilos de torch por proceso: ``TA_NLP_THREADS`` si está, si no núcleos / procesos."""
    explicit = os.environ.get("TA_NLP_THREADS")
    if explicit:
        return max(1, int(explicit))
    return max(1, (os.cpu_count() or 1) // max(1, workers))


def _make_engine(spec: Mapping[str, Any]) -> NlpEngine:
    if spec.get("kind") == "fake":
        from historical.testing import FakeNlpEngine

        return FakeNlpEngine(**spec.get("kwargs", {}))
    return BeamNlpEngine(**spec.get("kwargs", {}))


def _to_row_fn(name: str) -> Callable[[Mapping[str, Any]], dict[str, Any]]:
    if name == "fake":
        from historical.testing import fake_to_row

        return fake_to_row
    from historical.adapters import to_row

    return to_row


def _worker_main(root: str, seed: int, names: list[str], engine_spec: dict[str, Any],
                 to_row_name: str, threads: int, retry_failed: bool, worker: int) -> None:
    """Proceso hijo: fija sus hilos de torch y procesa sus archivos (spawn, sin estado heredado)."""
    os.environ["TA_NLP_THREADS"] = str(threads)
    try:
        import torch  # noqa: F401 - solo si está (imagen de NLP)

        torch.set_num_threads(threads)
    except Exception:  # noqa: BLE001
        pass
    work = WorkDir(root, seed)
    engine = _make_engine(engine_spec)
    to_row = _to_row_fn(to_row_name)
    t0 = time.monotonic()
    done_rows = 0
    for i, name in enumerate(names):
        meta = process_file(work, engine, work.stage1 / name, to_row=to_row,
                            retry_failed=retry_failed, worker=worker)
        done_rows += meta["rows"]
        elapsed = time.monotonic() - t0
        log_event("hist_nlp_file_done", worker=worker, file=name, rows=meta["rows"],
                  nlp_missing=meta["nlp_missing"], seconds=meta["seconds"],
                  rows_per_s=meta["rows_per_s"], batch_mean=meta["nlp_batches"].get("mean_size"),
                  infer_s=meta["nlp_batches"].get("infer_seconds"),
                  worker_files_left=len(names) - i - 1,
                  worker_rows_per_s=round(done_rows / elapsed, 2) if elapsed > 0 else None)


def _bench_worker(rows: list[dict[str, Any]], runner: str, batch_seconds: float | None,
                  threads: int, barrier: Any, queue: Any) -> None:
    """Proceso hijo del benchmark: carga los modelos, espera a los demás y mide."""
    try:
        os.environ["TA_NLP_THREADS"] = str(threads)
        try:
            import torch

            torch.set_num_threads(threads)
        except Exception:  # noqa: BLE001
            pass
        engine = BeamNlpEngine(runner=runner, batch_seconds=batch_seconds)
        engine.annotate(rows[:4])
        barrier.wait(timeout=900)
        start = time.time()
        out = engine.annotate(rows)
        end = time.time()
        queue.put({"n": len(out), "start": start, "end": end,
                   "infer_seconds": engine.last_infer_seconds, "batches": engine.last_batches})
    except Exception as exc:  # noqa: BLE001
        try:
            barrier.abort()
        except Exception:  # noqa: BLE001
            pass
        queue.put({"error": f"{type(exc).__name__}: {exc}"[:300]})


def _gather(procs: Sequence[Any], queue: Any, timeout: float) -> tuple[list[dict[str, Any]], str | None]:
    """Un resultado por proceso; si uno muere sin responder (p. ej. falta de memoria) no se
    queda esperando: termina los demás y lo explica."""
    import queue as queue_mod

    results: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout
    while len(results) < len(procs):
        try:
            results.append(queue.get(timeout=2))
            continue
        except queue_mod.Empty:
            pass
        dead = [p for p in procs if p.exitcode not in (None, 0)]
        if dead or time.monotonic() > deadline:
            for p in procs:
                if p.is_alive():
                    p.terminate()
            if dead:
                code = dead[0].exitcode
                hint = (" (-9: el sistema lo mató, casi siempre por falta de memoria; cada proceso "
                        "carga sus propios modelos)" if code == -9 else "")
                return results, f"un proceso terminó con código {code} sin responder{hint}"
            return results, f"sin respuesta en {timeout:g} s"
    return results, None


def parallel_bench(rows: Sequence[Mapping[str, Any]], workers: int, *, runner: str = "bundle",
                   batch_seconds: float | None = None, timeout: float = 1800.0) -> dict[str, Any]:
    """Filas/s en total con ``workers`` procesos a la vez (sin contar la carga de modelos)."""
    import multiprocessing

    ctx = multiprocessing.get_context("spawn")
    threads = worker_threads(workers)
    barrier = ctx.Barrier(workers)
    queue = ctx.Queue()
    chunks = [[dict(r) for r in rows[i::workers]] for i in range(workers)]
    procs = [ctx.Process(target=_bench_worker,
                         args=(chunk, runner, batch_seconds, threads, barrier, queue))
             for chunk in chunks]
    for proc in procs:
        proc.start()
    results, error = _gather(procs, queue, timeout)
    for proc in procs:
        proc.join(timeout=60)
    errors = ([error] if error else []) + [r["error"] for r in results if "error" in r]
    if errors:
        return {"workers": workers, "threads": threads, "error": errors[0]}
    t0 = min(r["start"] for r in results)
    t1 = max(r["end"] for r in results)
    total = sum(r["n"] for r in results)
    batches: collections.Counter[int] = collections.Counter()
    for r in results:
        batches.update(r.get("batches") or {})
    return {"workers": workers, "threads": threads, "rows": total,
            "seconds": round(t1 - t0, 1),
            "rows_per_s": round(total / (t1 - t0), 2) if t1 > t0 else None,
            "batches": batch_summary(batches)}


def run_nlp_stage(work: WorkDir, engine: NlpEngine | None = None, *,
                  to_row: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
                  max_files: int | None = None, retry_failed: bool = True,
                  force: bool = False, workers: int = 1,
                  engine_spec: Mapping[str, Any] | None = None,
                  to_row_name: str = "adapters") -> dict[str, Any]:
    """Etapa 2. Con ``workers > 1`` reparte los archivos entre procesos hijos (``spawn``),
    cada uno con sus modelos y ``núcleos / workers`` hilos de torch; necesita ``engine_spec``
    (``{"kind": "beam"|"fake", "kwargs": {...}}``) porque el motor se crea en cada hijo."""
    if not work.stage1_files():
        raise NlpStageError(f"no hay archivos en {work.stage1}: corra `generate` primero")
    work.stage2.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    pending, skipped = pending_files(work, force=force)
    if max_files is not None:
        pending = pending[:max_files]
    workers = max(1, min(int(workers or 1), len(pending) or 1))
    engine_name = (engine.name if engine is not None
                   else str((engine_spec or {}).get("kind", "beam")))
    log_event("hist_nlp_start", files=len(work.stage1_files()), pending=len(pending),
              skipped=skipped, engine=engine_name, workers=workers,
              threads_per_worker=worker_threads(workers) if workers > 1 else None)
    if workers == 1:
        if engine is None:
            engine = _make_engine(engine_spec or {"kind": "beam"})
        to_row = to_row or _to_row_fn(to_row_name)
        done_rows = 0
        for i, path in enumerate(pending):
            meta = process_file(work, engine, path, to_row=to_row, retry_failed=retry_failed)
            done_rows += meta["rows"]
            elapsed = time.monotonic() - t0
            rate = done_rows / elapsed if elapsed > 0 else None
            remaining = len(pending) - i - 1
            log_event("hist_nlp_file_done", file=path.name, rows=meta["rows"],
                      nlp_missing=meta["nlp_missing"], seconds=meta["seconds"],
                      batch_mean=meta["nlp_batches"].get("mean_size"),
                      infer_s=meta["nlp_batches"].get("infer_seconds"),
                      rows_per_s=meta["rows_per_s"], files_left=remaining,
                      eta_min=round(remaining * meta["rows"] / rate / 60, 1) if rate else None)
        described = getattr(engine, "describe", lambda: {"engine": engine.name})()
    else:
        import multiprocessing

        if engine_spec is None:
            raise NlpStageError("con varios procesos hace falta engine_spec")
        groups = [[p.name for p in pending[i::workers]] for i in range(workers)]
        threads = worker_threads(workers)
        ctx = multiprocessing.get_context("spawn")
        procs = [ctx.Process(target=_worker_main, name=f"ta-hist-nlp-{i}",
                             args=(str(work.root), work.seed, group, dict(engine_spec),
                                   to_row_name, threads, retry_failed, i))
                 for i, group in enumerate(groups) if group]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
        failed = {p.name: p.exitcode for p in procs if p.exitcode != 0}
        if failed:
            log_event("hist_nlp_worker_failed", workers=failed,
                      hint=("-9 = el sistema mató el proceso (casi siempre memoria): use menos "
                            "--workers o dé más memoria a Docker/WSL; lo hecho se conserva"
                            if -9 in failed.values() else None))
        described = {"engine": engine_name, "workers": workers, "threads_per_worker": threads,
                     **dict((engine_spec or {}).get("kwargs", {}))}
    summary = summarize_stage2(work)
    summary.update({"processed_files": len(pending) - summary["pending_files"]
                    if workers > 1 else len(pending),
                    "skipped_files": skipped, "workers": workers,
                    "seconds": round(time.monotonic() - t0, 1), "engine": described})
    atomic_write_json(work.nlp_json, summary)
    log_event("hist_nlp_done", **{k: summary[k] for k in ("files", "rows", "nlp_missing",
                                                          "pending_files", "seconds")})
    return summary


def summarize_stage2(work: WorkDir) -> dict[str, Any]:
    stage1 = {p.name: p for p in work.stage1_files()}
    rows = missing = 0
    versions: dict[str, int] = {}
    pending = []
    engines = set()
    rates = []
    for name, path in stage1.items():
        side = work.stage2 / f"{name}.json"
        if not side.exists() or not (work.stage2 / name).exists():
            pending.append(name)
            continue
        meta = json.loads(side.read_text(encoding="utf-8"))
        if meta.get("stage1_sha256") != sha256_file(path):
            pending.append(name)
            continue
        rows += int(meta.get("rows", 0))
        missing += int(meta.get("nlp_missing", 0))
        for v, c in (meta.get("versions") or {}).items():
            versions[v] = versions.get(v, 0) + int(c)
        engines.add(str((meta.get("engine") or {}).get("engine")))
        if meta.get("rows_per_s"):
            rates.append(float(meta["rows_per_s"]))
    return {"files": len(stage1), "pending_files": len(pending), "pending": pending[:10],
            "rows": rows, "nlp_missing": missing, "versions": versions,
            "engines": sorted(engines),
            "rows_per_s_median": sorted(rates)[len(rates) // 2] if rates else None}
