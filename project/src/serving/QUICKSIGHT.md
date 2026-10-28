# Tablero de QuickSight · guía de uso (Lote 6.1)

> Contrato e interfaces en `docs/ARCHITECTURE.md` §3.27 y §3.27.1; validación en `docs/validation/validation-6-1.md`.
> Todo lo que se despliega sale de `quicksight_spec.py` y de las consultas de `sql/`: para cambiar el tablero se cambia
> el código y se corre `python project/scripts/quicksight.py deploy --yes` (nunca a mano en la consola, porque el
> siguiente `deploy` lo pisaría).

## Qué hay

| Pieza | Id | Detalle |
|---|---|---|
| Fuente | `ta-athena-curated` | Athena, workgroup `ta-analytics`, rol de servicio `ta-quicksight-role` (solo lee `ta_curated` y `curated/`) |
| Dataset | `ta-ds-publicaciones` | Un comentario por fila (`fact_posts` con el nombre y el papel de la marca y la red), horas en `America/Bogota`, NLP en español |
| Dataset | `ta-ds-coocurrencia-hashtags` | Pares de hashtags que aparecen juntos y en cuántos comentarios |
| Dataset | `ta-ds-marcas-compartidas` | Pares de marcas y cuántos autores mencionan las dos |
| Dataset | `ta-ds-autores-por-marcas` | Cuántos autores mencionan cada combinación de marcas |
| Refresco | `ta-cada-hora` | Importación completa a SPICE cada hora (el lakehouse también corre cada hora) |
| Tablero | `ta-tablero-tendencias` | Hojas «Tendencias» y «Relaciones» (abajo) |
| Grupos | `ta-autores`, `ta-lectores` | Autores: editan datasets, análisis y tablero. Lectores: solo ven el tablero |

Ningún dataset tiene `user_hash` ni otra columna de autor: las relaciones se cuentan por marca o combinación de
marcas dentro del SQL.

## Las hojas

**Tendencias (HU-037).** Filtros arriba: marca, red social, origen del dato (`stream` o `historical`) y periodo (día
de Bogotá).
- Indicadores: comentarios, sentimiento medio (−1 a +1), % de comentarios negativos (sobre los que tienen NLP) e
  interacciones (me gusta + compartidos + respuestas).
- «Comentarios por día y marca» en **escala logarítmica**: el stream trae ~40.000 comentarios por día y el histórico
  ~280; con «Origen del dato» se ve cada uno por separado.
- «Sentimiento medio por día y marca», «Marca propia frente a competidores» (barras por marca coloreadas por papel y
  línea de sentimiento), «Comentarios por red social y marca», «Reparto del sentimiento por marca» y «Emociones por
  marca» (estos dos, solo comentarios con NLP) y «Comentarios por hora del día» (mapa de calor, hora de Bogotá).

**Relaciones (HU-038).**
- «Autores que mencionan varias marcas» (%), «Autores por combinación de marcas», sankey «Autores que comparten las
  dos marcas», mapa «Coocurrencia de hashtags» y tabla «Pares de hashtags más frecuentes».

## Abrir el tablero

- **Consola:** entrar a la consola de AWS con una identidad de la cuenta y abrir QuickSight en `us-east-1` →
  Dashboards → «TrendAnalyzer · Tendencias y relaciones».
- **Sin consola (prueba o demo):** `python project/scripts/quicksight.py embed-url --user 648617469176` (administrador)
  o `--user ta-quicksight-lector/demo` (lector) imprime una URL de un solo uso; ábrala antes de 5 minutos. No la
  comparta: abre una sesión de ese usuario.

## Dar acceso a una persona del equipo

1. Su identidad IAM debe poder asumir `ta-quicksight-lector` (solo ver) o `ta-quicksight-autor` (editar): una
   política con `sts:AssumeRole` sobre el rol. La cuenta raíz no puede asumir roles.
2. Al entrar a QuickSight con el rol asumido (nombre de sesión = su usuario), QuickSight la registra sola como
   lector o autor (la política del rol solo permite eso).
3. Agregarla al grupo: `aws quicksight create-group-membership --aws-account-id 648617469176 --namespace default
   --group-name ta-lectores --member-name ta-quicksight-lector/<sesión> --region us-east-1`. Desde ahí ve el tablero.

Cada usuario cuesta al terminar la prueba gratis (≈ 26-nov-2026): 24 USD/mes un autor o administrador, 3 un lector.
Nunca dar papeles Pro (suman 250 USD/mes a la cuenta).

## Refrescar, revisar y borrar

```bash
python project/scripts/quicksight.py refresh     # importar ya, sin esperar la hora
python project/scripts/quicksight.py status      # usuarios, última importación, versión del tablero
python project/scripts/validate_6_1.py status    # además, días de prueba que quedan y costo después
python project/scripts/validate_6_1.py unsubscribe --confirm-unsubscribe   # al terminar el uso (irreversible)
```
