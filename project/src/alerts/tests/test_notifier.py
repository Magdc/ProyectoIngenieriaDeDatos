"""Notificador y contrato ``ta-alert`` (Lote 6.2). Solo biblioteca estándar: también corre dentro de la imagen."""

from __future__ import annotations

import base64
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from alerts import notifier
from common import alert_message as am

T0 = 1_790_000_000_000 - (1_790_000_000_000 % 60_000)  # inicio de minuto, en UTC


def _alert(metric="volume", z=8.5, value=120, mean=40.0, origin="dataflow", **kw):
    args = dict(metric=metric, brand="postobon", source="instagram", window_start_ms=T0, window_end_ms=T0 + 300_000,
                value=value, baseline_mean=mean, baseline_std=6.2, zscore=z, z_threshold=3.0, baseline_windows=60,
                n_posts=120, avg_sentiment=-0.41, n_negative=70, n_positive=10, detected_ms=T0 + 345_000,
                cooldown_seconds=900, origin=origin)
    args.update(kw)
    return am.build_alert(**args)


def _push(alert=None, data: bytes | None = None, **message):
    if data is None:
        data, attrs = am.encode(alert)
        message.setdefault("attributes", attrs)
    body = {"message": {"data": base64.b64encode(data).decode(), "messageId": "m-1",
                        "publishTime": am.format_ts(T0 + 346_000), **message},
            "subscription": "projects/p/subscriptions/ta-alerts-notifier"}
    return json.dumps(body).encode()


def test_contrato_ida_y_vuelta_y_atributos():
    alert = _alert()
    assert am.alert_problems(alert) == []
    assert list(alert) == list(am.FIELDS)
    data, attrs = am.encode(alert)
    assert am.decode(data) == alert
    assert set(attrs) == set(am.ATTRIBUTE_KEYS) and attrs["alert_type"] == "volume_up"
    assert alert["change"] == 80 and alert["change_pct"] == 200.0
    # Determinista: misma marca, fuente, tipo y ventana ⇒ mismo id.
    assert _alert(detected_ms=T0 + 999_000)["alert_id"] == alert["alert_id"]
    assert _alert(z=-4.0, value=10)["alert_type"] == "volume_down"
    sentiment = _alert(metric="sentiment", z=-6.0, value=-0.62, mean=0.11)
    assert sentiment["alert_type"] == "sentiment_down" and sentiment["change_pct"] is None
    assert am.alert_problems(sentiment) == []


@pytest.mark.parametrize("change, problem", [
    ({"zscore": -8.5}, "signo de zscore"),
    ({"zscore": 2.0}, "z_threshold"),
    ({"window_end": "2026-10-27 10:05"}, "window_end"),
    ({"alert_type": "volume_down", "direction": "down", "zscore": -8.5}, "alert_id no coincide"),
    ({"extra": 1}, "campos no previstos"),
    ({"source": "tiktok"}, "source"),
])
def test_contrato_rechaza(change, problem):
    alert = {**_alert(), **change}
    assert any(problem in p for p in am.alert_problems(alert)), am.alert_problems(alert)
    with pytest.raises(ValueError):
        am.encode(alert)


def test_texto_con_marca_fuente_ventana_y_magnitud_en_utc_y_bogota():
    alert = _alert()
    text = am.alert_text(alert)
    start = am.parse_ts(alert["window_start"])
    assert "Volumen al alza de postobon en instagram" in text
    assert "120 posts frente a una media de 40,0 (+200 %, z = 8,5)" in text
    utc = f"{start:%H:%M}"
    bogota = f"{(start.hour - 5) % 24:02d}:{start:%M}"
    assert f"{utc}–" in text and f"({bogota}–" in text and "en Bogotá" in text
    sent = am.alert_text(_alert(metric="sentiment", z=-6.0, value=-0.62, mean=0.11, origin="check"))
    assert sent.startswith("[prueba] Sentimiento a la baja de postobon")
    assert "−0,62 frente a 0,11 (−0,73, z = −6,0; 120 posts)" in sent
    aviso = am.notification_fields(alert)
    assert aviso == {"marca": "postobon", "fuente": "instagram", "tipo": "volume_up", "ventana": am.window_text(alert),
                     "valor": "120", "media": "40,0", "cambio": "+200 %", "z": "8,5", "origen": "dataflow",
                     "alerta_id": alert["alert_id"]}


def test_push_valido_escribe_el_log_de_la_alerta():
    written = []
    alert = _alert()
    code, record = notifier.handle_push(_push(alert), T0 + 347_500, emit=written.append)
    assert code == 204 and written == [record]
    assert record["severity"] == "WARNING" and record["alert"] == "trend"
    assert record["message"] == am.alert_text(alert)
    assert record["aviso"]["marca"] == "postobon" and record["alert_id"] == alert["alert_id"]
    assert record["message_id"] == "m-1" and record["publish_time"] == am.format_ts(T0 + 346_000)
    assert record["detection_s"] == 47.5 and record["notify_delay_s"] == 2.5
    assert "schema" not in record and json.dumps(record)


def test_mensaje_invalido_se_confirma_con_error_y_peticion_mala_da_400():
    written = []
    code, record = notifier.handle_push(_push(data=b'{"no": "es una alerta"}'), T0, emit=written.append)
    assert code == 204 and record["severity"] == "ERROR" and record["alert"] == "invalid"
    code, record = notifier.handle_push(_push(data=b"\xff\xfe"), T0, emit=written.append)
    assert code == 204 and "UTF-8" in record["message"]
    body = json.dumps({"message": {"data": "%%%"}}).encode()
    code, record = notifier.handle_push(body, T0, emit=written.append)
    assert code == 204 and record["alert"] == "invalid"
    for bad in (b"no json", b'{"sin": "message"}', b'{"message": "texto"}'):
        code, record = notifier.handle_push(bad, T0, emit=written.append)
        assert code == 400 and record["alert"] == "bad_request"
    assert all(r["alert"] != "trend" for r in written)


def test_servidor_http(capsys):
    server = ThreadingHTTPServer(("127.0.0.1", 0), notifier.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with urllib.request.urlopen(base + "/healthz", timeout=5) as resp:
            assert resp.status == 200 and resp.read() == b"ok"
        req = urllib.request.Request(base + "/", data=_push(_alert()), method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 204
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(urllib.request.Request(base + "/", data=b"x", method="POST"), timeout=5)
        assert exc.value.code == 400
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(base + "/otra", timeout=5)
        assert exc.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
    lines = [json.loads(ln) for ln in capsys.readouterr().out.splitlines() if ln.startswith("{")]
    assert [ln["alert"] for ln in lines] == ["trend", "bad_request"]
