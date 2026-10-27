# Precisión del NLP (HU-033) · corrida `dq-20261027T131811Z-4e48c2`

Datos: `ta_curated.fact_posts` (221.629 filas) al 2026-10-27T13:18:11.000Z. Evaluadas: **109.365** filas con `label_sentiment` y `sentiment_label`; sin etiqueta de referencia 66.605 (`nostalgia` y `adiccion`, ADR-13); con etiqueta pero sin predicción 45.659 (filas sin NLP).

**Accuracy: 92,0 %** · F1 macro: 91,7 %

## Matriz de confusión (filas = `label_sentiment`, columnas = `sentiment_label`)

| | negative | neutral | positive |
|---|---|---|---|
| **negative** | 36.236 | 1.380 | 76 |
| **neutral** | 414 | 26.758 | 791 |
| **positive** | 211 | 5.911 | 37.588 |

## Por clase

| Clase | Precisión | Recall | F1 | Soporte |
|---|---|---|---|---|
| negative | 98,3 % | 96,1 % | 97,2 % | 37.692 |
| neutral | 78,6 % | 95,7 % | 86,3 % | 27.963 |
| positive | 97,7 % | 86,0 % | 91,5 % | 43.710 |

## Por origen

| Origen | Filas evaluadas | Accuracy |
|---|---|---|
| historical | 35.045 | 91,8 % |
| stream | 74.320 | 92,0 % |

## Emoción (sin verdad de referencia: solo distribución, 156.366 filas)

| Emoción | Filas | % |
|---|---|---|
| joy | 44.415 | 28,4 % |
| sadness | 14.185 | 9,1 % |
| anger | 17.653 | 11,3 % |
| fear | 1.097 | 0,7 % |
| disgust | 937 | 0,6 % |
| surprise | 1.097 | 0,7 % |
| neutral | 76.982 | 49,2 % |

## Limitación

Las métricas son optimistas: los comentarios salen de plantillas del generador (§3.2) y los modelos se entrenaron con tuits en español; `label_sentiment` es la etiqueta de la categoría de la plantilla, no una anotación humana. No hay verdad de referencia para la emoción.
