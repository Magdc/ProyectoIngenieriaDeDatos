"""Great Expectations (GX 1.x): un contexto efímero, una suite por DataFrame y sus resultados sin datos sensibles.

GX no manda analítica (``GX_ANALYTICS_ENABLED=False``) ni dibuja barras de progreso. Los resultados se piden en
formato ``SUMMARY`` con 0 valores de ejemplo (``partial_unexpected_count = 0``): el JSON no lleva ``user_hash``,
textos ni ``event_id``; las ``value_set`` grandes (DQ-70: los ``user_hash`` de ``dim_user``) se reemplazan por
su tamaño antes de guardarlo.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from dq.catalog import Spec

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")

RESULT_FORMAT = {"result_format": "SUMMARY", "partial_unexpected_count": 0}
MAX_VALUE_SET = 20


def resolve(kwargs: dict, value_sets: dict[str, list] | None) -> dict:
    """``value_set = "@dim.llave"`` → los valores de esa dimensión (DQ-70)."""
    out = dict(kwargs)
    ref = out.get("value_set")
    if isinstance(ref, str) and ref.startswith("@"):
        out["value_set"] = list((value_sets or {}).get(ref[1:], []))
    return out


def sanitize(result: dict) -> dict:
    """Quita lo que puede traer datos de filas y achica las ``value_set`` grandes."""
    result = dict(result)
    result.pop("id", None)  # los id de GX son UUID: fuera, para que un UUID en la salida solo pueda ser un event_id
    meta = dict(result.get("meta") or {})
    meta.pop("batch_spec", None)
    result["meta"] = meta
    clean = []
    for r in result.get("results") or []:
        r = dict(r)
        cfg = dict(r.get("expectation_config") or {})
        cfg.pop("id", None)
        kw = dict(cfg.get("kwargs") or {})
        if isinstance(kw.get("value_set"), list) and (len(kw["value_set"]) > MAX_VALUE_SET or kw.get("column") == "user_hash"):
            kw["value_set"] = f"<{len(kw['value_set'])} valores de la dimensión>"
        kw.pop("batch_id", None)
        cfg["kwargs"] = kw
        r["expectation_config"] = cfg
        r["result"] = {k: v for k, v in (r.get("result") or {}).items() if not k.startswith(("partial_unexpected", "unexpected_list",
                                                                                                "unexpected_index"))}
        clean.append(r)
    result["results"] = clean
    return result


class Validator:
    """Un contexto de GX efímero (nada en disco) con una fuente pandas; ``validate`` corre una suite sobre un DataFrame."""

    def __init__(self) -> None:
        import great_expectations as gx
        from great_expectations.data_context.types.base import ProgressBarsConfig

        self.gx = gx
        self.version = gx.__version__
        self.ctx = gx.get_context(mode="ephemeral")
        self.ctx.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
        self.source = self.ctx.data_sources.add_pandas("ta_dq")

    def validate(self, name: str, df: pd.DataFrame, specs: list[Spec],
                 value_sets: dict[str, list] | None = None) -> tuple[dict, list[tuple[Spec, dict | None]]]:
        import great_expectations.expectations as gxe

        expectations = []
        for i, spec in enumerate(specs):
            cls = getattr(gxe, spec.ge)
            expectations.append(cls(**resolve(spec.kwargs, value_sets), meta={"rule": spec.rule, "label": spec.label, "i": i}))
        suite = self.gx.ExpectationSuite(name=name, expectations=expectations)
        batch = self.source.add_dataframe_asset(name).add_batch_definition_whole_dataframe("all").get_batch(
            batch_parameters={"dataframe": df})
        raw = batch.validate(suite, result_format=RESULT_FORMAT).to_json_dict()
        by_i: dict[int, Any] = {}
        for r in raw.get("results") or []:
            i = ((r.get("expectation_config") or {}).get("meta") or {}).get("i")
            if i is not None:
                by_i[int(i)] = r
        return sanitize(raw), [(spec, by_i.get(i)) for i, spec in enumerate(specs)]
