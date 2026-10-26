"""Adaptadores al código de lotes anteriores que el loader reutiliza tal cual.

El loader **no reimplementa** el simulador, la anonimización, la fila de
``posts_enriched``, el Parquet ni el NLP: llama al código de los lotes 1.1, 1.2, 2.2 y
2.3 (HU-024: "el mismo generador… el mismo código de anonimización y NLP"). Este
módulo concentra esas llamadas. Las firmas se descubren con ``inspect`` a partir de lo
que documenta ARCHITECTURE.md (§3.2.2, §3.10, §3.14, §3.15), con variantes razonables
de nombres; si algo no encaja, el error dice qué función, qué firma y qué faltó.
``load_historical.py doctor`` muestra lo que resolvió cada adaptador.

Nada de aquí se importa al cargar el paquete: el simulador (Faker), pyarrow y Beam se
importan al usarse, para que el mismo paquete corra en el host y en la imagen de NLP.
"""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import datetime as dt
import enum
import hashlib
import hmac
import inspect
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

UTC = dt.timezone.utc
NLP_FIELDS = ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score",
              "nlp_model_version")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


class AdapterError(RuntimeError):
    """Una pieza de un lote anterior no tiene la forma que el loader esperaba."""


class Lazy:
    """Valor que solo se calcula si una firma lo pide."""

    def __init__(self, fn: Callable[[], Any]):
        self.fn = fn
        self._done = False
        self._value: Any = None

    def get(self) -> Any:
        if not self._done:
            self._value = self.fn()
            self._done = True
        return self._value


def describe_callable(fn: Any) -> str:
    name = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or repr(fn)
    module = getattr(fn, "__module__", "")
    try:
        sig = str(inspect.signature(fn))
    except (TypeError, ValueError):
        sig = "(…)"
    return f"{module}.{name}{sig}" if module else f"{name}{sig}"


def call_flexible(fn: Callable[..., Any], *, positional: Sequence[Any] = (),
                  by_name: Mapping[str, Any] | None = None,
                  only_if_required: Iterable[str] = ()) -> Any:
    """Llama a ``fn`` llenando sus parámetros por nombre.

    ``positional`` se consume en orden por los primeros parámetros posicionales.
    ``by_name`` da valores por nombre de parámetro (varios alias pueden apuntar al mismo
    valor). Los nombres de ``only_if_required`` solo se pasan si el parámetro no tiene
    valor por defecto. Los valores ``Lazy`` se calculan solo si se usan.
    """
    by_name = dict(by_name or {})
    only = set(only_if_required)
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return fn(*positional)
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    pending = list(positional)
    missing: list[str] = []
    for p in sig.parameters.values():
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and pending:
            args.append(pending.pop(0))
            continue
        if p.name in by_name and not (p.name in only and p.default is not p.empty):
            value = by_name[p.name]
            value = value.get() if isinstance(value, Lazy) else value
            if p.kind is p.POSITIONAL_ONLY:
                args.append(value)
            else:
                kwargs[p.name] = value
            continue
        if p.default is p.empty:
            missing.append(p.name)
    if missing:
        raise AdapterError(
            f"{describe_callable(fn)}: no sé qué pasar a {', '.join(missing)}. "
            "Ajuste el adaptador en src/historical/adapters.py (ver HISTORICAL.md §6).")
    return fn(*args, **kwargs)


def _aliases(value: Any, *names: str) -> dict[str, Any]:
    return {n: value for n in names}


def _annotation_text(fn: Callable[..., Any], names: Iterable[str]) -> str:
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return ""
    for n in names:
        if n in params:
            ann = params[n].annotation
            return "" if ann is inspect.Parameter.empty else str(ann)
    return ""


@contextlib.contextmanager
def patched_env(values: Mapping[str, str]) -> Iterator[None]:
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def plain_json(value: Any) -> Any:
    """Copia con tipos JSON (el envelope es JSON por contrato, §5.2)."""
    return json.loads(json.dumps(value, ensure_ascii=False, default=_json_default))


def _json_default(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        from historical.plan import format_ts

        return format_ts(value if value.tzinfo else value.replace(tzinfo=UTC))
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if isinstance(value, enum.Enum):
        return value.value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    raise TypeError(f"no serializable: {type(value).__name__}")


# ======================================================================= simulador

CONFIG_NAMES = ("config", "cfg", "sim_config", "settings", "conf", "sim_cfg")
SEED_NAMES = ("seed", "sim_seed")
ORIGIN_NAMES = ("origin",)
CHAOS_NAMES = ("chaos_enabled", "chaos")
SOURCE_NAMES = ("source", "fuente", "src")
SEQ_NAMES = ("seq", "sequence", "index", "n")
NOW_NAMES = ("now", "ts", "at", "when", "clock_now", "instant", "t")


def _get(obj: Any, *names: str) -> Any:
    for n in names:
        if isinstance(obj, Mapping) and n in obj:
            return obj[n]
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def load_sim_config(seed: int) -> Any:
    """``simulator.sim_config.load_config()`` con ``SIM_SEED`` = semilla histórica y caos apagado."""
    try:
        import simulator  # noqa: F401
        from simulator import sim_config
    except ImportError as exc:
        raise AdapterError(f"no se pudo importar el simulador (Lote 1.2): {exc}. "
                           "Instale src/simulator/requirements.txt con constraints.txt.") from exc
    sim_yaml = Path(simulator.__file__).resolve().parent / "config" / "sim.yaml"
    overrides = {"SIM_SEED": str(seed), "SIM_CHAOS_ENABLED": "false"}
    if not os.environ.get("SIM_CONFIG") and sim_yaml.exists():
        overrides["SIM_CONFIG"] = str(sim_yaml)
    with patched_env(overrides):
        env_copy = dict(os.environ)
        by_name = {**_aliases(env_copy, "env", "environ", "environment"),
                   **_aliases(str(sim_yaml), "path", "config_path", "file", "filename", "yaml_path")}
        cfg = call_flexible(sim_config.load_config, by_name=by_name,
                            only_if_required=("path", "config_path", "file", "filename", "yaml_path"))
    got_seed = _get(cfg, "seed", "sim_seed")
    if got_seed is not None and int(got_seed) != int(seed):
        raise AdapterError(f"sim_config.load_config() devolvió seed={got_seed}, se pidió {seed}")
    chaos = _get(cfg, "chaos")
    enabled = _get(chaos, "enabled") if chaos is not None and not isinstance(chaos, bool) else chaos
    if enabled is None:
        enabled = _get(cfg, "chaos_enabled")
    if enabled is True:
        raise AdapterError("el caos quedó encendido en la configuración del simulador")
    return cfg


def build_factory(cfg: Any, seed: int) -> tuple[Any, str]:
    try:
        from simulator import factory as fmod
    except ImportError as exc:
        raise AdapterError(f"no se pudo importar simulator.factory (Lote 1.2): {exc}") from exc

    def _brands() -> Any:
        from simulator.generator.brands import load_brands

        return load_brands()

    by_name = {**_aliases(cfg, *CONFIG_NAMES), **_aliases(int(seed), *SEED_NAMES),
               **_aliases("historical", *ORIGIN_NAMES), **_aliases(False, *CHAOS_NAMES),
               **_aliases(Lazy(_brands), "brands")}
    for name in ("build_factory", "make_factory", "create_factory", "factory_from_config",
                 "build_event_factory"):
        fn = getattr(fmod, name, None)
        if callable(fn):
            return call_flexible(fn, by_name=by_name), f"simulator.factory.{name}"
    cls = getattr(fmod, "EventFactory", None)
    if cls is None:
        raise AdapterError("simulator.factory no define EventFactory (§3.10)")
    for name in ("from_config", "from_settings", "create", "build"):
        fn = getattr(cls, name, None)
        if callable(fn):
            return call_flexible(fn, by_name=by_name), f"EventFactory.{name}"
    return call_flexible(cls, by_name=by_name), "EventFactory(...)"


def extract_envelope(obj: Any) -> dict[str, Any]:
    """Envelope (§5.2) de lo que devuelve ``EventFactory.make`` (``Emission``)."""
    if isinstance(obj, Mapping) and "event_id" in obj:
        return plain_json(dict(obj))
    for attr in ("envelope", "event", "record", "body_json", "parsed"):
        val = obj.get(attr) if isinstance(obj, Mapping) else getattr(obj, attr, None)
        if isinstance(val, Mapping) and "event_id" in val:
            return plain_json(dict(val))
    for attr in ("messages", "message", "pubsub_messages", "data", "body"):
        val = obj.get(attr) if isinstance(obj, Mapping) else getattr(obj, attr, None)
        if val is None:
            continue
        items = val if isinstance(val, (list, tuple)) else [val]
        for item in items:
            for raw in _message_bodies(item):
                try:
                    parsed = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                if isinstance(parsed, dict) and "event_id" in parsed:
                    return parsed
    if isinstance(obj, (list, tuple)):
        for item in obj:
            with contextlib.suppress(AdapterError):
                return extract_envelope(item)
    raise AdapterError(f"no encuentro el envelope en {type(obj).__name__} "
                       "(se buscó envelope/event/messages[].data)")


def _message_bodies(item: Any) -> list[Any]:
    out = []
    if isinstance(item, (bytes, str)):
        out.append(item)
    elif isinstance(item, tuple) and item:
        out.append(item[0])
    elif isinstance(item, Mapping):
        out.extend(item.get(k) for k in ("data", "body") if item.get(k) is not None)
    else:
        for attr in ("data", "body"):
            if getattr(item, attr, None) is not None:
                out.append(getattr(item, attr))
    return out


class SimulatorEnvelopes:
    """Envelopes del simulador real (Lote 1.2) convertidos a ``origin = historical``."""

    kind = "simulator"

    def __init__(self, seed: int, factory: Any, how: str):
        self.seed = int(seed)
        self.factory = factory
        self.how = how
        self._make = getattr(factory, "make", None)
        if not callable(self._make):
            raise AdapterError(f"{how} no tiene un método make() (§3.10)")
        ann = _annotation_text(self._make, NOW_NAMES).lower()
        self.now_as_epoch = ("float" in ann or "int" in ann) and "datetime" not in ann
        from common.envelope import event_id

        self._event_id = event_id
        self.native_origin: str | None = None

    @classmethod
    def build(cls, seed: int) -> "SimulatorEnvelopes":
        cfg = load_sim_config(seed)
        factory, how = build_factory(cfg, seed)
        self = cls(seed, factory, how)
        self.self_check()
        return self

    def _call_make(self, source: str, seq: int, now: dt.datetime) -> Any:
        now_value: Any = now.timestamp() if self.now_as_epoch else now
        by_name = {**_aliases(source, *SOURCE_NAMES), **_aliases(seq, *SEQ_NAMES),
                   **_aliases(now_value, *NOW_NAMES), "scenario": None}
        try:
            return call_flexible(self._make, by_name=by_name)
        except AdapterError:
            return self._make(source, seq, now_value)

    def raw(self, source: str, seq: int, now: dt.datetime) -> dict[str, Any]:
        return extract_envelope(self._call_make(source, seq, now))

    def envelope(self, source: str, seq: int, now: dt.datetime) -> dict[str, Any]:
        env = self.raw(source, seq, now)
        if self.native_origin is None:
            self.native_origin = env.get("origin")
        env["origin"] = "historical"
        env["event_id"] = self._event_id("historical", source, self.seed, seq)
        return env

    def self_check(self) -> dict[str, Any]:
        """Comprueba semilla, caos, ``seq`` y marca de tiempo con un evento por fuente."""
        from historical.plan import parse_ts

        now = dt.datetime(2026, 1, 15, 12, 0, 0, 123000, tzinfo=UTC)
        report: dict[str, Any] = {}
        for source in ("reddit", "facebook", "instagram"):
            env = self.raw(source, 3, now)
            origin = env.get("origin")
            if env.get("source") != source:
                raise AdapterError(f"make({source!r}, 3, …) devolvió source={env.get('source')!r}")
            if env.get("seq") != 3:
                raise AdapterError(f"make({source!r}, 3, …) devolvió seq={env.get('seq')!r}")
            expected = self._event_id(origin or "stream", source, self.seed, 3)
            if env.get("event_id") != expected:
                raise AdapterError(
                    f"el simulador no usa la semilla {self.seed}: event_id {env.get('event_id')} "
                    f"!= {expected} (¿load_config() ignora SIM_SEED?)")
            chaos = (env.get("sim") or {}).get("chaos")
            if chaos:
                raise AdapterError(f"evento con caos {chaos}: el caos debe ir apagado")
            delta = (now - parse_ts(env["event_time"])).total_seconds()
            if not -1 <= delta <= 6:
                raise AdapterError(f"event_time {env['event_time']} no sigue a now={now.isoformat()} "
                                   "(¿make() ignora el instante?)")
            report[source] = {"origin": origin, "event_id_ok": True, "lag_s": round(delta, 3)}
        self.native_origin = report["reddit"]["origin"]
        return report

    def describe(self) -> dict[str, Any]:
        return {"how": self.how, "make": describe_callable(self._make),
                "now_as_epoch": self.now_as_epoch, "native_origin": self.native_origin}


# ============================================================= llave y anonimización

KEY_SOURCES = ("secret", "env", "local_dev")


@dataclasses.dataclass
class KeyInfo:
    value: Any
    source: str
    detail: str
    alternatives: tuple[Any, ...] = ()
    attempts: tuple[str, ...] = ()

    def fingerprint(self, value: Any | None = None) -> str:
        raw = self.value if value is None else value
        data = raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8")
        return hmac.new(bytes(data), b"ta-historical-fingerprint", hashlib.sha256).hexdigest()[:16]


def _key_source_obj(hk: Any, source: str, project: str | None, secret_name: str,
                    env_var: str) -> Any:
    ks = getattr(hk, "KeySource", None)
    if ks is None:
        return source
    if isinstance(ks, type) and issubclass(ks, enum.Enum):
        for member in ks:
            if str(member.value).lower() == source or member.name.lower() == source:
                return member
        raise AdapterError(f"KeySource no tiene el modo {source!r}: {[m.name for m in ks]}")
    if dataclasses.is_dataclass(ks) or inspect.isclass(ks):
        by_name = {**_aliases(source, "kind", "mode", "type", "source", "name_kind"),
                   **_aliases(secret_name, "secret_name", "secret", "name"),
                   **_aliases(project, "project", "project_id", "secret_project", "gcp_project"),
                   **_aliases(env_var, "env_var", "env", "env_name", "variable", "var")}
        try:
            return call_flexible(ks, by_name=by_name)
        except (AdapterError, TypeError, ValueError):
            for attr in (source, source.upper(), f"from_{source}"):
                maker = getattr(ks, attr, None)
                if maker is not None:
                    return call_flexible(maker, by_name=by_name) if callable(maker) else maker
            raise
    return source


def _first_key_value(result: Any) -> Any:
    if isinstance(result, (str, bytes, bytearray)):
        return result
    if isinstance(result, (tuple, list)):
        for item in result:
            if isinstance(item, (str, bytes, bytearray)):
                return item
    for attr in ("key", "value", "secret"):
        val = getattr(result, attr, None)
        if isinstance(val, (str, bytes, bytearray)):
            return val
    raise AdapterError(f"load_hmac_key() devolvió {type(result).__name__}, no una llave")


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (bytes, bytearray)):
        return len(bytes(value).strip()) == 0
    return len(str(value).strip()) == 0


def _describe_value(value: Any) -> str:
    """Tipo y largo de una llave, sin mostrarla."""
    if value is None:
        return "None"
    return f"{type(value).__name__} de {len(value)} caracteres" if isinstance(
        value, (str, bytes, bytearray)) else type(value).__name__


def load_key(source: str, *, gcp_project: str | None = None, secret_name: str = "ta-hmac-key",
             env_var: str = "TA_HMAC_KEY") -> KeyInfo:
    """Llave HMAC con el mismo código del stream (``common.hmac_key``, §3.14).

    Prueba, en orden, ``load_hmac_key()``, ``access_secret()`` y ``gcloud secrets versions
    access``; un resultado vacío cuenta como fallo y se pasa al siguiente. El error final
    dice qué devolvió cada intento (tipo y largo, nunca el valor).
    """
    if source not in KEY_SOURCES:
        raise AdapterError(f"origen de llave desconocido {source!r} ({', '.join(KEY_SOURCES)})")
    if source == "env":
        value = os.environ.get(env_var)
        if _is_empty(value):
            raise AdapterError(f"la variable {env_var} está vacía")
        return KeyInfo(value, "env", f"variable {env_var}")
    try:
        from common import hmac_key as hk
    except ImportError as exc:
        raise AdapterError(f"no se pudo importar common.hmac_key (Lote 2.2): {exc}") from exc
    attempts: list[str] = []
    loader = getattr(hk, "load_hmac_key", None)
    if callable(loader):
        try:
            ks = _key_source_obj(hk, source, gcp_project, secret_name, env_var)
            by_name = {**_aliases(ks, "source", "key_source", "ks", "spec", "mode", "kind"),
                       **_aliases(gcp_project, "project", "project_id", "secret_project",
                                  "gcp_project"),
                       **_aliases(secret_name, "secret_name", "name", "secret"),
                       **_aliases(env_var, "env_var", "env_name")}
            raw = call_flexible(loader, positional=[ks], by_name=by_name)
            value = _first_key_value(raw)
            if not _is_empty(value):
                return KeyInfo(value, source, f"common.hmac_key.load_hmac_key({source})",
                               attempts=tuple(attempts))
            attempts.append(f"{describe_callable(loader)} con {ks!r}: devolvió "
                            f"{_describe_value(value)} (vacía)")
        except Exception as exc:  # noqa: BLE001 - se informa y se prueba la alternativa
            attempts.append(f"load_hmac_key: {type(exc).__name__}: {exc}"[:400])
    else:
        attempts.append("common.hmac_key no define load_hmac_key")
    if source == "local_dev":
        for name in dir(hk):
            val = getattr(hk, name)
            if "DEV" in name.upper() and "KEY" in name.upper() and isinstance(val, (str, bytes)) \
                    and not _is_empty(val):
                return KeyInfo(val, source, f"common.hmac_key.{name}", attempts=tuple(attempts))
        raise AdapterError("no encontré la llave local de desarrollo: " + "; ".join(attempts))
    access = getattr(hk, "access_secret", None)
    if callable(access):
        try:
            by_name = {**_aliases(gcp_project, "project", "project_id", "secret_project"),
                       **_aliases(secret_name, "secret_name", "name", "secret", "secret_id"),
                       "version": "latest"}
            value = _first_key_value(call_flexible(access, by_name=by_name,
                                                   only_if_required=("version",)))
            if not _is_empty(value):
                return KeyInfo(value, source, "common.hmac_key.access_secret()",
                               attempts=tuple(attempts))
            attempts.append(f"{describe_callable(access)}: devolvió {_describe_value(value)} (vacía)")
        except Exception as exc:  # noqa: BLE001
            attempts.append(f"access_secret: {type(exc).__name__}: {exc}"[:400])
    if not gcp_project:
        raise AdapterError("no se pudo leer la llave: " + "; ".join(attempts + ["falta GCP_PROJECT"]))
    proc = subprocess.run(
        ["gcloud", "secrets", "versions", "access", "latest", f"--secret={secret_name}",
         f"--project={gcp_project}"], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if proc.returncode != 0 or _is_empty(proc.stdout):
        detail = proc.stderr.strip()[:300] if proc.returncode != 0 else "salida vacía"
        raise AdapterError("no se pudo leer la llave: " + "; ".join(
            attempts + [f"gcloud secrets versions access: {detail}"]))
    value = proc.stdout
    stripped = value.strip()
    alternatives = (stripped,) if stripped != value else ()
    return KeyInfo(value, source, "gcloud secrets versions access (alternativa)",
                   alternatives=alternatives, attempts=tuple(attempts))


KEY_PARAM_NAMES = ("key", "hmac_key", "secret", "hmac_secret", "llave", "hmac", "secret_key")
PROCESS_PARAM_NAMES = ("process_time", "now", "processed_at", "at", "process_ts", "ts",
                       "process_dt")


KEY_NAME_PARTS = ("key", "secret", "hmac", "llave")
PROCESS_NAME_PARTS = ("process", "now")


def _matching_params(fn: Callable[..., Any], parts: Sequence[str], skip_first: bool) -> list[str]:
    try:
        params = list(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):
        return []
    if skip_first and params and params[0].kind in (params[0].POSITIONAL_ONLY,
                                                    params[0].POSITIONAL_OR_KEYWORD):
        params = params[1:]
    return [p.name for p in params
            if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
            and any(part in p.name.lower() for part in parts)]


def make_enricher(key: Any, process_time: dt.datetime) -> Callable[[Mapping[str, Any]], dict]:
    """``common.enriched_row.enrich_envelope()`` (§3.14) con la llave y la hora de proceso.

    La llave va al parámetro cuyo nombre contiene ``key``, ``secret``, ``hmac`` o ``llave``
    (además de los nombres de ``KEY_PARAM_NAMES``); si no hay ninguno, falla mostrando la
    firma en vez de dejar que se use un valor por defecto vacío.
    """
    try:
        from common import enriched_row as er
    except ImportError as exc:
        raise AdapterError(f"no se pudo importar common.enriched_row (Lote 2.2): {exc}") from exc
    if _is_empty(key):
        raise AdapterError("la llave HMAC llegó vacía al anonimizador (revise load_key)")
    fn = er.enrich_envelope
    key_params = sorted(set(_matching_params(fn, KEY_NAME_PARTS, skip_first=True))
                        | set(n for n in _matching_params(fn, KEY_PARAM_NAMES, skip_first=True)))
    if not key_params:
        raise AdapterError(f"{describe_callable(fn)}: no encuentro el parámetro de la llave "
                           "(se buscan nombres con key/secret/hmac/llave)")
    proc_params = [n for n in _matching_params(fn, PROCESS_NAME_PARTS + PROCESS_PARAM_NAMES,
                                               skip_first=True) if n not in key_params]
    ann = _annotation_text(fn, proc_params or PROCESS_PARAM_NAMES).lower()
    from historical.plan import format_ts

    pt: Any = process_time
    if ann and "datetime" not in ann and "str" in ann:
        pt = format_ts(process_time)
    by_name = {**_aliases(key, *KEY_PARAM_NAMES, *key_params),
               **_aliases(pt, *PROCESS_PARAM_NAMES, *proc_params)}

    def enrich(envelope: Mapping[str, Any]) -> dict[str, Any]:
        out = call_flexible(fn, positional=[copy.deepcopy(dict(envelope))], by_name=by_name)
        return plain_json(out)

    enrich.describe = describe_callable(fn)  # type: ignore[attr-defined]
    enrich.key_params = key_params  # type: ignore[attr-defined]
    return enrich


def to_row(enriched: Mapping[str, Any]) -> dict[str, Any]:
    """``common.enriched_row.to_posts_enriched_row()`` y las columnas de NLP del envelope."""
    from common import enriched_row as er

    row = call_flexible(er.to_posts_enriched_row, positional=[dict(enriched)])
    row = dict(row)
    for f in NLP_FIELDS:
        if f in enriched:
            row[f] = enriched[f]
    return row


def posts_enriched_columns() -> list[str]:
    from common import enriched_row as er

    cols = getattr(er, "POSTS_ENRICHED_COLUMNS", None)
    if cols is None:
        raise AdapterError("common.enriched_row no define POSTS_ENRICHED_COLUMNS (§3.14)")
    names = []
    for c in cols:
        if isinstance(c, str):
            names.append(c)
        elif isinstance(c, Mapping):
            names.append(str(c.get("name")))
        elif isinstance(c, (tuple, list)):
            names.append(str(c[0]))
        else:
            names.append(str(getattr(c, "name")))
    return names


def rows_to_parquet(rows: Sequence[Mapping[str, Any]]) -> bytes:
    """``common.parquet_rows.rows_to_parquet_bytes()`` (mismo formato que ``enriched/``, §5.4)."""
    from common import parquet_rows as pr

    data = call_flexible(pr.rows_to_parquet_bytes, positional=[list(rows)])
    if not isinstance(data, (bytes, bytearray)):
        raise AdapterError(f"rows_to_parquet_bytes devolvió {type(data).__name__}")
    return bytes(data)


# ================================================================== NLP (Lote 2.3)

def nlp_transform(options: Any) -> Any:
    """El PTransform de NLP del pipeline (``streaming.nlp.nlp_from_options``, §3.15)."""
    import apache_beam as beam

    try:
        from streaming import nlp
    except ImportError as exc:
        raise AdapterError(f"no se pudo importar streaming.nlp (Lote 2.3): {exc}") from exc
    fn = getattr(nlp, "nlp_from_options", None)
    if not callable(fn):
        raise AdapterError("streaming.nlp no define nlp_from_options() (§3.15)")
    result = call_flexible(fn, positional=[options],
                           by_name=_aliases(options, "options", "pipeline_options", "opts"))
    candidates = result if isinstance(result, (tuple, list)) else [result]
    for item in candidates:
        if isinstance(item, beam.PTransform):
            return item
    if result is None:
        raise AdapterError(
            "nlp_from_options() devolvió None: el NLP quedó apagado. En local se activa solo si "
            "existe $TA_NLP_MODELS_DIR/models.json (corra la etapa dentro de la imagen "
            "ta-dataflow-nlp-local:local con scripts/historical_nlp.sh).")
    raise AdapterError(f"nlp_from_options() devolvió {type(result).__name__}, no un PTransform")


def nlp_model_info() -> dict[str, Any]:
    """Versión de modelos según ``streaming.nlp`` (solo diagnóstico)."""
    from streaming import nlp

    info: dict[str, Any] = {}
    for name in ("read_manifest", "nlp_model_version"):
        fn = getattr(nlp, name, None)
        if callable(fn):
            try:
                info[name] = plain_json(call_flexible(fn))
            except Exception as exc:  # noqa: BLE001
                info[name] = f"{type(exc).__name__}: {exc}"
    return info


def row_of(element: Any) -> Mapping[str, Any]:
    """Fila de un elemento de salida del NLP (dict, o tupla/objeto que la contiene)."""
    if isinstance(element, Mapping):
        return element
    if isinstance(element, tuple):
        for item in element:
            if isinstance(item, Mapping) and "event_id" in item:
                return item
    for attr in ("row", "value", "element"):
        val = getattr(element, attr, None)
        if isinstance(val, Mapping):
            return val
    raise AdapterError(f"salida del NLP no reconocida: {type(element).__name__}")
