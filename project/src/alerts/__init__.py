"""Alertas de tendencia (Lote 6.2, HU-039): notificador de Cloud Run y comprobaciones. Ver ARCHITECTURE.md §3.28.

``alerts.notifier`` es lo único que va en la imagen ``alert-notifier`` (junto con ``common.alert_message``);
``alerts.check`` y ``alerts.cli`` los usa el validador desde la máquina del equipo.
"""
