# Recruitment Copilot

Proyecto final — Técnico en Ingeniería de Datos con Databricks (Universidad Latina de Costa Rica), Módulo 2.

Pipeline medallion end-to-end sobre currículums en PDF (dataset [Resume Dataset, Kaggle](https://www.kaggle.com/datasets/snehaanbhawal/resume-dataset)), orquestado en Databricks y consumido por un agente conversacional (Agent Bricks + MCP server) respaldado por Lakebase, con matching semántico currículum↔vacante vía Databricks Vector Search.

## Pregunta de negocio

¿Qué categorías de currículums muestran más señales de experiencia y liderazgo, y qué candidatos destacan dentro de cada una?

## Arquitectura

**Pipeline (Bronze → Silver → Gold):**

```
Kaggle: snehaanbhawal/resume-dataset
      │  kaggle datasets download (setup_secrets.py + bronze/01_bronze_ingest.py)
      ▼
Volume: recruitment_copilot.bronze.landing          (PDFs crudos, por categoría)
      │  Auto Loader (cloudFiles, binaryFile) + pypdf   [bronze/01_bronze_ingest.py]
      ▼
recruitment_copilot.bronze.bronze_resumes            (raw_text tal cual, checkpoint idempotente)
      │  normalizar, cuarentena, dedupe, word_count/leadership_score  [silver/02_silver_transform.py]
      ▼
recruitment_copilot.silver.silver_resumes            (mode overwrite)
      │  groupBy category  /  window ranking por leadership_score      [gold/03_gold_metrics.py]
      ▼
recruitment_copilot.gold.gold_category_stats         (responde la pregunta de negocio)
recruitment_copilot.gold.gold_top_candidates          (ranking, opcional)
```

Orquestación: Job de Databricks con 3 tareas encadenadas (`bronze → silver → gold`), `trigger(availableNow=True)` + checkpoint en Bronze, `mode("overwrite")` idempotente en Silver/Gold. Idempotencia demostrada corriendo el Job dos veces seguidas.

**Capa de consumo:**

```
 Agente (Databricks Agent Bricks / Playground)
           │  MCP tool calls: get_category_stats, search_resumes,
           │  find_matching_resumes, shortlist_candidate, get_shortlist
           ▼
 mcp_server/recruiter_mcp_server.py   (FastMCP, streamable-HTTP, Databricks App)
           │
           ├──► resume_broker.py  ──► Gold (SQL Warehouse, Statement Execution API)
           ├──► vector_search.py  ──► Vector Search Index sobre silver_resumes.clean_text
           └──► lakebase.py       ──► Lakebase Postgres: candidate_shortlist

 dashboard/app.py   (Flask, Databricks App SEPARADA — nunca escribe)
           └──► copias propias de lakebase.py / resume_broker.py / vector_search.py
```

- `mcp_server/recruiter_mcp_server.py` — el MCP server. Expone las tools vía `@mcp.tool`, transporte streamable-HTTP. Toda la lógica vive en los módulos de abajo.
- `mcp_server/resume_broker.py` — lee `gold_category_stats`/`gold_top_candidates` vía la Statement Execution API del SDK de Databricks (sin Spark — una Databricks App no tiene sesión de Spark).
- `mcp_server/vector_search.py` — matching semántico vía `VectorSearchClient` sobre un Delta Sync Index (Databricks calcula y sincroniza los embeddings solo; no hay modelo local que cargar, a diferencia del patrón pgvector + `sentence-transformers` de `databricks-lakebase-app-day-3`).
- `mcp_server/lakebase.py` — conexión a Lakebase (Postgres administrado por Databricks), mismo patrón que el proyecto de referencia.
- `dashboard/app.py` — dashboard de solo lectura (su propia Databricks App), con copias propias de los tres módulos de arriba — cada App se despliega de forma independiente desde su propia carpeta, así que la duplicación es deliberada, no un descuido.

## Tools del agente

| Tool | Qué hace |
|---|---|
| `get_category_stats()` | Lee `gold_category_stats` — conteo, `avg_word_count`, `avg_leadership_score` por categoría. |
| `search_resumes(category?, limit?)` | Lee `gold_top_candidates` — ranking por `leadership_score`, opcionalmente filtrado por categoría. |
| `find_matching_resumes(job_description, limit?)` | Vector Search semántico sobre `silver_resumes.clean_text` — currículums más parecidos a una descripción de vacante en lenguaje libre. |
| `get_shortlist(limit?)` | Lee `candidate_shortlist` de Lakebase — quién ya fue marcado para revisión. |
| `shortlist_candidate(resume_id, category, note)` | **Única escritura que existe.** Inserta/actualiza en `candidate_shortlist` (Lakebase). |

**División de responsabilidad:** las tools exponen datos (Gold, Vector Search) y una sola escritura determinística (`shortlist_candidate`); el agente decide qué currículum recomendar y por qué, pero nunca puede aprobar, rechazar ni "descartar" a nadie — esa tool no existe.

## Human-in-the-loop

El agente **sugiere y anota, nunca decide**. `shortlist_candidate` marca un currículum para revisión humana — no aprueba ni descarta candidatos. El hiring algorítmico tiene escrutinio legal real (NYC Local Law 144, guías EEOC); esta restricción está en el docstring de la tool y debe reforzarse en el system prompt del agente (ver abajo).

## System prompt del agente

```
Sos un copiloto de reclutamiento. Tu trabajo es sugerir candidatos para revisión humana,
nunca decidir por tu cuenta — no aprobás, no rechazás, no "descartás" a nadie. Esa decisión
es exclusivamente de un reclutador humano.

Usá siempre las tools disponibles para responder — nunca inventes un currículum, una
categoría, un leadership_score o un match_score que no haya salido de una llamada real. Si
una tool devuelve {"status": "error", ...}, contale al usuario el problema puntual en vez
de adivinar o rellenar con datos de tu propio conocimiento.

Para preguntas generales sobre categorías ("¿qué áreas muestran más liderazgo?"), usá
get_category_stats. Para pedir candidatos dentro de una categoría ya conocida (por nombre
exacto, ej. "INFORMATION-TECHNOLOGY"), usá search_resumes. Cuando el usuario describe una
vacante en lenguaje libre ("busco alguien con experiencia liderando equipos de datos"), usá
find_matching_resumes en vez de search_resumes — es búsqueda semántica, no por categoría
exacta, y suele encontrar candidatos que una búsqueda por palabra clave se perdería.

Cuando recomiendes revisar a alguien, llamá a shortlist_candidate y explicá siempre en
`note` por qué ese currículum merece revisión — qué leadership_score o qué match_score lo
hizo destacar, citando el número real devuelto por la tool. Revisá get_shortlist antes si
no estás seguro de si ya fue marcado.

Nunca redondees los números a ojo ni los inventes — citá siempre los valores reales
(leadership_score, match_score, resume_count) que te devuelven las tools.
```

## Setup

### Fase A — ya hecho (este repo)

- Bronze/Silver/Gold corridos de punta a punta en Databricks, con idempotencia demostrada (Job con 3 tareas, corrido dos veces, mismos conteos).
- Código completo del MCP server, el dashboard y Vector Search — falta desplegarlo (Fase B).

### Fase B — pasos manuales en tu workspace de Databricks (Free Edition)

**Checkpoint 2026-08-19:** pasos 1–7 completos. Vector Search endpoint (`recruitment_copilot_vs`) e índice (`silver_resumes_index`) creados, en estado **"Waiting for initial sync"** — ya aprovisionó recursos, ahora sincronizando ~2483 filas; puede tardar varios minutos más en pasar a Online. **Retomar por el paso 8** (desplegar `mcp_server/`) — no hace falta esperar a que el índice esté Online para desplegar la App, solo para que `find_matching_resumes` funcione en runtime.

1. [x] **Provisionar Lakebase**: Compute → OLTP Database → Create. Copiar la connection URL.
2. [x] **Correr `mcp_server/schema_shortlist.sql`** contra esa instancia — desde el **SQL Editor de Databricks** o `psql` local. **No** lo corras con `psycopg2` desde un notebook serverless: Free Edition rompe `psycopg2` ahí con `FATAL FIPS SELFTEST FAILURE`.
3. [x] **Correr `setup_secrets.py`** (ya soporta Kaggle + Lakebase) para guardar la URL de Lakebase como secret.
4. [x] **Habilitar Change Data Feed en Silver** (requisito para un Delta Sync Index):
   ```sql
   ALTER TABLE recruitment_copilot.silver.silver_resumes
   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
   ```
5. [x] **Crear un Vector Search endpoint** (`recruitment_copilot_vs`).
6. [x] **Crear el Delta Sync Index** sobre `silver_resumes` — columna de embedding `clean_text`, PK `resume_id`, columnas indexadas: todas (en blanco) — nombrado `recruitment_copilot.silver.silver_resumes_index`. *Verificar que el status haya pasado de "Provisioning" a "Online" antes de probar `find_matching_resumes`.*
7. [x] **Conseguir el ID de tu SQL Warehouse** y reemplazar `REPLACE_WITH_YOUR_WAREHOUSE_ID` en `mcp_server/app.yaml` y `dashboard/app.yaml` (ya hecho: `f6d25ff69fb4c394`).
8. [x] **Desplegar `mcp_server/`** como Databricks App (`recruiter-mcp`), source = `mcp_server/`. Crasheó una vez con `ModuleNotFoundError: databricks.vectorsearch` — el paquete `databricks-vectorsearch` expone el módulo como `databricks.vector_search` (guion bajo); corregido en `mcp_server/vector_search.py` y `dashboard/vector_search.py` (commit `fc4b37e`). Redesplegada, status **Running**.
9. [ ] **← PRÓXIMO PASO. Desplegar `dashboard/`** como una **segunda** Databricks App, source = `dashboard/`. Free Edition permite hasta 3 Apps — esto usa 2.
10. [ ] **Registrar el MCP server**: AI Gateway → MCPs → Add MCP, pegar la URL streamable-HTTP de la App del MCP server (copiarla de la pantalla de `recruiter-mcp`).
11. [ ] **Construir y validar el agente en el Playground**: Tools → seleccionar el MCP recién registrado → pegar el system prompt de arriba → probar con preguntas reales → pegar los transcripts en "Demo Q&A" abajo. Una vez validado: Get Code → Export to Databricks Apps.
12. [ ] Abrir la App del dashboard y confirmar que el shortlist armado por el agente se ve ahí.
13. [ ] **Free Edition auto-detiene las Apps a las 24h de inactividad** — reiniciar `mcp_server`, `dashboard` y el agente exportado antes del check-in (24 ago) y la presentación (26 ago).

## Demo Q&A

_Pegar acá al menos 3 preguntas reales y la respuesta del agente (con sus tool calls), después de probarlo en el Playground/Agent Bricks._

1. **Q:** _(ej. "¿qué categorías muestran más señales de liderazgo?")_
   **A:**

2. **Q:**
   **A:**

3. **Q:**
   **A:**

## Known limitations

_Errores encontrados durante las pruebas y cómo se resolvieron (o por qué no) — sección 9 de la rúbrica pide documentar esto explícitamente._

- **`ModuleNotFoundError: databricks.vectorsearch` al desplegar `recruiter-mcp`:** el paquete `databricks-vectorsearch` (`requirements.txt`) expone el módulo como `databricks.vector_search` (con guion bajo), no `databricks.vectorsearch`. Afectaba tanto a `mcp_server/vector_search.py` como a `dashboard/vector_search.py`. Resuelto cambiando el import a `from databricks.vector_search.client import VectorSearchClient`.

## Plan completo

El detalle de decisiones, cronograma y accesos está en [`PLAN.md`](PLAN.md).

## Docente

Repositorio público — acceso de lectura garantizado para `AndresACV` sin necesidad de invitación explícita.
