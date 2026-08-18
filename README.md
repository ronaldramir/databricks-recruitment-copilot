# Recruitment Copilot

Proyecto final — Técnico en Ingeniería de Datos con Databricks (Universidad Latina de Costa Rica), Módulo 2.

Pipeline medallion end-to-end sobre currículums en PDF (dataset [Resume Dataset, Kaggle](https://www.kaggle.com/datasets/snehaanbhawal/resume-dataset)), orquestado en Databricks y consumido por un agente conversacional (Agent Bricks + MCP server) respaldado por Lakebase.

## Pregunta de negocio

¿Qué categorías de currículums muestran más señales de experiencia y liderazgo, y qué candidatos destacan dentro de cada una?

## Arquitectura

| Capa | Contenido |
|---|---|
| [`bronze/`](bronze/) | Ingesta cruda de los PDFs con Auto Loader (`cloudFiles`, `binaryFile`). |
| [`silver/`](silver/) | Limpieza, deduplicación, cuarentena y columnas derivadas (`word_count`, `leadership_score`). |
| [`gold/`](gold/) | Métricas de negocio: `gold_category_stats` y `gold_top_candidates`. |
| [`mcp_server/`](mcp_server/) | Agente MCP: tools de lectura sobre Gold + `shortlist_candidate` (escritura en Lakebase). |
| [`dashboard/`](dashboard/) | Vista humana del shortlist / red de seguridad si falta tiempo para el agente. |

Orquestación: Job de Databricks o Lakeflow Declarative Pipeline encadenando las tres capas, con idempotencia verificada (correr dos veces no duplica Gold).

## Human-in-the-loop

El agente **sugiere y anota, nunca decide**. `shortlist_candidate` marca un currículum para revisión humana — no aprueba ni descarta candidatos. Ver el detalle en [`PLAN.md`](PLAN.md).

## Plan completo

El detalle de decisiones, cronograma y accesos está en [`PLAN.md`](PLAN.md).

## Docente

Repositorio público — acceso de lectura garantizado para `AndresACV` sin necesidad de invitación explícita.
