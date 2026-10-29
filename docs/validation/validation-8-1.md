# Validación del Lote 8.1 · HU-044 y HU-045

> **Informe técnico, sustentación y runbook** (ADR-37).
> - [`INFORME.md`](../INFORME.md): arquitectura, decisiones, contratos, por qué las fuentes son simuladas, KPI medidos
>   frente al objetivo, limitaciones y modelos de NLP con su licencia y sus citas.
> - [`SUSTENTACION.md`](../SUSTENTACION.md): guion de ~20 min de punta a punta con el pico y su alerta, el plan B y el
>   registro del ensayo.
> - [`RUNBOOK.md`](../RUNBOOK.md): encendido y apagado, reinicio del simulador, reproceso desde `raw/`, DLQ y cada
>   alerta.
>
> La tabla de KPI del informe **no se escribe a mano**: `project/scripts/kpi_report.py` la arma con la evidencia de las
> validaciones. La latencia por etapa sale de la prueba de humo de HU-048; la calidad, de Great Expectations; el
> tablero, del 6.1; la detección, del 6.2 y el 6.3. Contrato en `docs/ARCHITECTURE.md` §3.33.
>
> **Automático:** `python project/scripts/validate_8_1.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming`. Evidencia en `project/evidencia/validacion_8_1.json`.
>
> **Cambia la nube solo `ensayo`** (con el OK del usuario). Enciende el simulador y Dataflow ~1,5 h (≈ 0,6 USD) y lanza
> la ola de demo, con su alerta y su correo. Corre la prueba de humo, que publica un centinela, ejecuta el exportador y
> arranca el workflow. Refresca SPICE y apaga.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-044 · Arquitectura, ADR, contratos y justificación de las fuentes simuladas | L1 |
| HU-044 · KPI medidos frente al objetivo: latencia < 5 min, completitud ≥ 95 %, unicidad ≥ 99 %, duplicados < 1 %, tablero < 3 s, anonimización 100 % | L1, L2 (la tabla = la evidencia; todos cumplen) |
| HU-044 · Limitaciones declaradas | L1 (las cuatro de la HU y otras) |
| HU-044 · Modelos de NLP (RoBERTuito y pysentimiento, corpus TASS) y su licencia | L1 |
| HU-044 · Cada KPI con consulta o comando reproducible; la tabla se alimenta de HU-048 | L2 (columna «Cómo volver a medirlo»; latencias de `smoke_e2e_5_4.json`) |
| HU-045 · Guion simulador → Pub/Sub → Dataflow → BigQuery → S3 → Athena → QuickSight, con un pico y su alerta | L3 |
| HU-045 · Runbook: encendido y apagado, reinicio del simulador, reproceso desde `raw/`, DLQ | L4 |
| HU-045 · Plan B: capturas, ejecución grabada, simulador local | L3 |
| HU-045 · Ensayo completo realizado | E1 (y L6, el registro en `SUSTENTACION.md`) |

## Cómo ejecutarlo

```bash
cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
source ~/.venvs/ta-streaming/bin/activate
python project/scripts/kpi_report.py --check
python project/scripts/validate_8_1.py preflight
python project/scripts/validate_8_1.py tests
python project/scripts/validate_8_1.py local
python project/scripts/validate_8_1.py ensayo --yes   # con el OK del usuario; ~1,5 h
python project/scripts/validate_8_1.py status
```

## Partes

| Paso | Esperado |
|---|---|
| A1 | Los 10 archivos del lote |
| B1 | `src/report/tests/test_kpis.py`: **5** passed (la evidencia real cumple todos los KPI; la tabla del informe está al día; una medición fuera de objetivo se marca ❌) |
| B2 | `test_ops_validate81.py`: **5** passed |
| L1 | `INFORME.md` con los 18 puntos de HU-044 |
| L2 | `kpi_report.py --check`: la tabla del informe = la evidencia; 8 KPI cumplen y 1 es informativo (precisión del NLP) |
| L3 | `SUSTENTACION.md`: las 7 etapas, `demo_scenario.py start`, la alerta, el plan B (capturas, ejecución grabada, simulador local) y el registro del ensayo |
| L4 | `RUNBOOK.md`: las 5 secciones |
| L5 | Cada ruta citada en los tres documentos existe |
| L6 | El ensayo está registrado (AVISO mientras no se haga) |
| E1 | Ensayo: P y F del 6.3 (simulador y Dataflow, baseline, ola ×3 y su alerta), `smoke_e2e.py run`, `quicksight.py refresh` y O; el resultado queda en «Registro del ensayo» |

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| L2 «la tabla del informe no coincide» | Una validación cambió su evidencia: `python project/scripts/kpi_report.py --write` y commit |
| L2 «no cumplen: …» | Una medición nueva quedó fuera del objetivo: el informe debe decirlo; revisar la etapa con su validador |
| E1 falla en la alerta | El baseline tarda ≈ 66 min con un job nuevo; `validate_6_3.py flow` tiene su propia guía (problemas frecuentes del 6.3) |
