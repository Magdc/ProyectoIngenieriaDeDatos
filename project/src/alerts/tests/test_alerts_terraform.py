"""Terraform del Lote 6.2 frente al código: nombres, filtro del log, etiquetas del correo y permisos.

Leen archivos del repositorio (``infra/gcp``), así que no van en la imagen del notificador.
"""

from __future__ import annotations

import re
from pathlib import Path

from common import alert_message as am
from streaming.alerts import ALERTS_TOPIC, DEFAULT_COOLDOWN_SECONDS

PROJECT = Path(__file__).resolve().parents[3]
MODULE = PROJECT / "infra" / "gcp" / "modules" / "alert_notifier" / "main.tf"
ENV = PROJECT / "infra" / "gcp" / "environments" / "dev"


def _default(text: str, name: str) -> str:
    m = re.search(rf'variable "{name}" \{{.*?default\s*=\s*"?([^"\n]*)"?\n', text, flags=re.S)
    assert m, name
    return m.group(1).strip()


def test_nombres_del_modulo_y_parametros_de_5_12():
    text = MODULE.read_text(encoding="utf-8")
    assert _default(text, "service_name") == "ta-alert-notifier"
    assert _default(text, "alerts_topic") == ALERTS_TOPIC
    assert _default(text, "push_subscription") == "ta-alerts-notifier"
    assert _default(text, "incidents_topic") == "ta-alerts-incidents"
    assert _default(text, "policy_name") == "ta-alerta-tendencia"
    assert _default(text, "rate_limit_seconds") == "300" and _default(text, "auto_close_seconds") == "1800"
    assert "min_instance_count = 0" in text and "max_instance_count = 1" in text
    assert "allUsers" not in text and "allAuthenticatedUsers" not in text
    assert DEFAULT_COOLDOWN_SECONDS == 900


def test_filtro_y_etiquetas_coinciden_con_el_log_del_notificador():
    text = MODULE.read_text(encoding="utf-8")
    sample = am.build_alert(metric="volume", brand="pepsi", source="reddit", window_start_ms=0, window_end_ms=300_000,
                            value=30, baseline_mean=10.0, baseline_std=2.0, zscore=10.0, z_threshold=3.0,
                            baseline_windows=60, n_posts=30, avg_sentiment=0.1, n_negative=1, n_positive=5,
                            detected_ms=350_000, cooldown_seconds=900)
    record = am.log_record(sample, received_ms=351_000)
    assert 'severity="WARNING"' in text and f'jsonPayload.alert="{record["alert"]}"' in text
    assert record["severity"] == "WARNING"
    m = re.search(r"extracted\s*=\s*\[([^\]]*)\]", text)
    keys = re.findall(r'"([a-z_]+)"', m.group(1))
    assert keys == list(record["aviso"]), "las etiquetas de la política deben ser los campos de jsonPayload.aviso"
    for key in keys:
        assert f"$${{log.extracted_label.{key}}}" in text, f"la documentación del correo no usa {key}"


def test_entorno_dev_permisos_y_correos_sin_versionar():
    alerts = (ENV / "alerts.tf").read_text(encoding="utf-8")
    assert 'account_id   = "sa-alerts"' in alerts
    assert 'resource "google_pubsub_topic_iam_member" "dataflow_alerts_publisher"' in alerts
    assert "module.sa_dataflow.member" in alerts and '"roles/pubsub.publisher"' in alerts
    variables = (ENV / "variables_alerts.tf").read_text(encoding="utf-8")
    assert re.search(r'variable "alert_emails" \{[^}]*sensitive\s*=\s*true', variables, flags=re.S)
    outputs = (ENV / "outputs_alerts.tf").read_text(encoding="utf-8")
    for name in ("alerts_service_account", "alerts_notifier_url", "alerts_notifier_image", "alerts_push_subscription",
                 "alerts_incidents_topic", "alert_policy_name", "alert_policy_id", "alert_email_channels"):
        assert f'output "{name}"' in outputs
    # Ningún archivo versionado del entorno lleva una dirección de correo real.
    for path in [*ENV.glob("*.tf"), ENV / "terraform.tfvars.example", MODULE]:
        found = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", path.read_text(encoding="utf-8"))
        assert all(f.endswith(("example.com", "gserviceaccount.com")) or f.startswith("$") for f in found), (path.name, found)
