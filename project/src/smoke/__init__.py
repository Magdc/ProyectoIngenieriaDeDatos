"""Prueba de humo de extremo a extremo (Lote 5.4, HU-048; ARCHITECTURE.md §3.26).

``e2e`` sigue un comentario centinela desde Pub/Sub hasta ``ta_curated.fact_posts`` con puertos inyectables;
``clouds`` implementa esos puertos con los clientes reales de GCP y AWS; ``cli`` es ``scripts/smoke_e2e.py``.
"""
