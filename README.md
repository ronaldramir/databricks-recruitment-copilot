# Recruitment Copilot

Proyecto final — Técnico en Ingeniería de Datos con Databricks (Universidad Latina de Costa Rica), Módulo 2.

Pipeline medallion end-to-end sobre currículums en PDF (dataset [Resume Dataset, Kaggle](https://www.kaggle.com/datasets/snehaanbhawal/resume-dataset)), orquestado en Databricks y consumido por un agente conversacional (Agent Bricks + MCP server) respaldado por Lakebase, con matching semántico currículum↔vacante vía Databricks Vector Search.

## Pregunta de negocio

¿Qué categorías de currículums muestran más señales de experiencia y liderazgo, y qué candidatos destacan dentro de cada una? *(principal, respondida por `gold_category_stats` + `gold_top_candidates`)*

Gold responde además cuatro preguntas extra para el equipo de reclutamiento — ver [`gold/03_gold_metrics.py`](gold/03_gold_metrics.py):

1. ¿La seniority declarada en el CV (títulos "senior"/"director"/"vp") coincide con el lenguaje de liderazgo, o son señales distintas? (`gold_category_stats`, columna `avg_seniority_score`)
2. ¿Qué categorías tienen currículums listos para contactar de inmediato (traen email/teléfono detectables)? (`gold_contact_quality`)
3. ¿Un currículum más largo realmente comunica más liderazgo, o `word_count` solo mide verborragia? (`gold_length_vs_leadership`)
4. ¿En qué categorías el pipeline pierde más candidatos por PDFs no legibles? (`gold_category_health`, cruza Bronze con la cuarentena de Silver)

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
      │  groupBy category  /  window ranking / terciles / joins con Bronze  [gold/03_gold_metrics.py]
      ▼
recruitment_copilot.gold.gold_category_stats          (pregunta de negocio principal)
recruitment_copilot.gold.gold_top_candidates          (ranking por categoría, extra)
recruitment_copilot.gold.gold_contact_quality         (% listos para contactar, extra)
recruitment_copilot.gold.gold_length_vs_leadership    (¿largo del CV correlaciona con liderazgo?, extra)
recruitment_copilot.gold.gold_category_health         (tasa de cuarentena por categoría, extra)
```

Orquestación: Job de Databricks con 3 tareas encadenadas (`bronze → silver → gold`), `trigger(availableNow=True)` + checkpoint en Bronze, `mode("overwrite")` idempotente en Silver/Gold. Idempotencia demostrada corriendo el Job dos veces seguidas.

**Capa de consumo:**

```
 Agente (Databricks Agent Bricks / Playground)
           │  MCP tool calls: get_category_stats, search_resumes,
           │  find_matching_resumes, shortlist_candidate, get_shortlist
           ▼
 mcp_server/recruiter_mcp_server.py   (FastMCP, streamable-HTTP, Databricks App con prefijo mcp-)
           │
           ├──► resume_broker.py  ──► Gold (SQL Warehouse, Statement Execution API)
           ├──► vector_search.py  ──► Vector Search Index sobre silver_resumes.clean_text
           └──► lakebase.py       ──► Lakebase Postgres: candidate_shortlist

 dashboard/app.py   (Flask, Databricks App SEPARADA — nunca escribe)
           └──► copia propia de lakebase.py — solo candidate_shortlist, nada de Gold
```

- `mcp_server/recruiter_mcp_server.py` — el MCP server. Expone las tools vía `@mcp.tool`, transporte streamable-HTTP. Toda la lógica vive en los módulos de abajo.
- `mcp_server/resume_broker.py` — lee `gold_category_stats`/`gold_top_candidates` vía la Statement Execution API del SDK de Databricks (sin Spark — una Databricks App no tiene sesión de Spark).
- `mcp_server/vector_search.py` — matching semántico vía `VectorSearchClient` sobre un Delta Sync Index (Databricks calcula y sincroniza los embeddings solo; no hay modelo local que cargar, a diferencia del patrón pgvector + `sentence-transformers` de `databricks-lakebase-app-day-3`).
- `mcp_server/lakebase.py` — conexión a Lakebase (Postgres administrado por Databricks), mismo patrón que el proyecto de referencia.
- `dashboard/app.py` — dashboard de solo lectura (su propia Databricks App), con una copia propia de `lakebase.py` — cada App se despliega de forma independiente desde su propia carpeta. **Deliberadamente no expone `find_matching_resumes` ni `gold_category_stats`**: el matching semántico vive solo como tool del agente (un único lugar para buscar candidatos, no dos interfaces compitiendo), y el reporting de categorías es trabajo de una Genie Space apuntada a Gold, no de código a medida. El dashboard existe únicamente para mostrar el shortlist que arma el agente — para qué currículum, para qué puesto, y por qué.

## Tools del agente

| Tool | Qué hace |
|---|---|
| `get_category_stats()` | Lee `gold_category_stats` — conteo, `avg_word_count`, `avg_leadership_score` por categoría. |
| `search_resumes(category?, limit?)` | Lee `gold_top_candidates` — ranking por `leadership_score`, opcionalmente filtrado por categoría. |
| `find_matching_resumes(job_description, limit?)` | Vector Search semántico sobre `silver_resumes.clean_text` — currículums más parecidos a una descripción de vacante en lenguaje libre. |
| `get_shortlist(limit?, job_title?)` | Lee `candidate_shortlist` de Lakebase — quién ya fue marcado para revisión, opcionalmente filtrado por puesto. |
| `shortlist_candidate(resume_id, category, note, job_title, job_description?)` | **Única escritura que existe.** Inserta/actualiza en `candidate_shortlist` (Lakebase), atado a un puesto concreto — el mismo currículum puede quedar shortlisteado para más de un puesto sin pisar la nota anterior. |

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

Llamá a shortlist_candidate SOLO cuando el usuario te lo pida explícitamente (ej. "marcalo",
"agregalo al shortlist", "quiero revisar a este"). Nunca lo hagas de forma proactiva solo
porque encontraste un buen match - encontrar candidatos y decidir a quién escalarle al
reclutador son dos pasos distintos, y el segundo lo decide el humano, no vos.

Cuando shortlistees a alguien, explicá siempre en `note` por qué ese currículum merece
revisión, citando el número real devuelto por la tool (leadership_score o match_score) -
nunca lo califiques de "alto"/"bajo"/"buen match" con tus propias palabras, dejá que el
número hable solo y que el reclutador lo juzgue. Pasá siempre un `job_title` corto (el
puesto que se está buscando; si no hay uno específico, usá el nombre de la categoría).
`job_description` es SIEMPRE obligatorio en cada llamada: si el candidato salió de un
find_matching_resumes anterior en esta conversación, pasá el texto exacto que usaste en esa
llamada, palabra por palabra; si salió de search_resumes o get_category_stats, pasá un
string vacío "" - nunca inventes una descripción. Revisá get_shortlist antes si no estás
seguro de si ya fue marcado para ese mismo puesto.

Nunca redondees los números a ojo ni los inventes — citá siempre los valores reales
(leadership_score, match_score, resume_count) que te devuelven las tools.
```

## Setup

### Fase A — ya hecho (este repo)

- Bronze/Silver/Gold corridos de punta a punta en Databricks, con idempotencia demostrada (Job con 3 tareas, corrido dos veces, mismos conteos).
- Código completo del MCP server, el dashboard y Vector Search — falta desplegarlo (Fase B).
- **Pendiente (2026-08-20):** Silver ganó tres columnas derivadas (`seniority_score`, `has_email`/`has_phone`/`contact_complete`) y Gold ganó tres tablas nuevas (`gold_contact_quality`, `gold_length_vs_leadership`, `gold_category_health`) — código listo, **falta correr el Job de punta a punta en Databricks** para materializarlas y completar los párrafos de respuesta en `gold/03_gold_metrics.py` con los números reales. `resume_broker.py`/el MCP server siguen leyendo solo `gold_category_stats`/`gold_top_candidates`, sin cambios — exponer las tablas nuevas como tools es opcional, no bloqueante para la entrega.

### Fase B — pasos manuales en tu workspace de Databricks (Free Edition)

**Checkpoint 2026-08-19 (noche):** pasos 1–9 completos, `dashboard` validado de punta a punta en producción (categorías, shortlist y match semántico los tres funcionando). **Retomar por el paso 10** (registrar el MCP en AI Gateway).

**Checkpoint 2026-08-19 (más tarde):** se sacó la caja de match semántico del `dashboard` (`/api/match_resumes`, `dashboard/vector_search.py`) — quedaba como una segunda interfaz de búsqueda que competía con la tool `find_matching_resumes` del agente, en vez de reforzar el patrón "el agente actúa, el dashboard solo muestra lo que hizo". El dashboard ahora solo lee categorías (Gold, como contexto) y el shortlist (Lakebase, para ver en vivo lo que el agente marcó) — el matching semántico existe únicamente como tool del agente. Falta redesplegar la App del dashboard con este cambio antes de la demo.

**Checkpoint 2026-08-19 (paso 10, pivote):** el registro manual vía **AI Gateway → MCPs → Create MCP Service** se abandonó — pedía armar un client OAuth (Token endpoint/Client ID/Client secret) o un Bearer token que no terminó de andar, sobre-ingeniería para un MCP que ya es una Databricks App propia dentro del mismo workspace. En su lugar: **una Databricks App cuyo nombre empieza con `mcp-` se auto-descubre en el dropdown de Tools del Playground**, sin registro aparte ni configuración de auth manual. Se redesplegó el MCP server como una App nueva (`mcp-recruiter-copilot`, mismo código de `mcp_server/`) y quedó **funcionando** — confirmado en Playground con "Tools (1)" y las 5 tools respondiendo. Pendiente: borrar la App vieja `recruiter-mcp` (nunca llegó a conectar) y el MCP Service a medio crear en Catalog, para no gastar el cupo de 3 Apps de Free Edition.

**Checkpoint 2026-08-19 (bug de identidad):** `_get_end_user_email()` en `recruiter_mcp_server.py` leía `x-forwarded-user` (un UUID interno) en vez de `x-forwarded-email` (el email real) — el shortlist quedaba con `added_by` = UUID. Corregido para priorizar `x-forwarded-email`.

**Checkpoint 2026-08-19 (shortlist con contexto de puesto):** `candidate_shortlist` ganó `job_title`/`job_description` — cada flag queda atado al puesto por el que se lo shortlisteó, no solo a la categoría. Requiere correr la migración al final de `mcp_server/schema_shortlist.sql` contra Lakebase (la tabla ya existía) antes de redesplegar. El dashboard sacó la tabla de categorías (ese reporting es trabajo de una Genie Space apuntada a Gold, no del dashboard) y ahora muestra puesto + JD en el shortlist.

1. [x] **Provisionar Lakebase**: Compute → OLTP Database → Create. Copiar la connection URL.
2. [x] **Correr `mcp_server/schema_shortlist.sql`** contra esa instancia — desde el **SQL Editor de Databricks** o `psql` local. **No** lo corras con `psycopg2` desde un notebook serverless: Free Edition rompe `psycopg2` ahí con `FATAL FIPS SELFTEST FAILURE`. *(Si la tabla ya existía antes de `job_title`/`job_description`, correr la migración comentada al final del archivo en vez del `CREATE TABLE`.)*
3. [x] **Correr `setup_secrets.py`** (ya soporta Kaggle + Lakebase) para guardar la URL de Lakebase como secret.
4. [x] **Habilitar Change Data Feed en Silver** (requisito para un Delta Sync Index):
   ```sql
   ALTER TABLE recruitment_copilot.silver.silver_resumes
   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
   ```
5. [x] **Crear un Vector Search endpoint** (`recruitment_copilot_vs`).
6. [x] **Crear el Delta Sync Index** sobre `silver_resumes` — columna de embedding `clean_text`, PK `resume_id`, columnas indexadas: todas (en blanco) — nombrado `recruitment_copilot.silver.silver_resumes_index`. *Verificar que el status haya pasado de "Provisioning" a "Online" antes de probar `find_matching_resumes`.*
7. [x] **Conseguir el ID de tu SQL Warehouse** y reemplazar `REPLACE_WITH_YOUR_WAREHOUSE_ID` en `mcp_server/app.yaml` y `dashboard/app.yaml` (ya hecho: `f6d25ff69fb4c394`).
8. [x] **Desplegar `mcp_server/`** como Databricks App con nombre `mcp-recruiter-copilot` (el prefijo `mcp-` es lo que permite el auto-descubrimiento en Playground — ver checkpoint del paso 10), source = `mcp_server/`. Otorgar los mismos grants de Unity Catalog (`USE CATALOG`/`USE SCHEMA`/`SELECT` sobre `gold`/`silver`) al service principal de esta App nueva.
9. [x] **Desplegar `dashboard/`** como una **segunda** Databricks App, source = `dashboard/`. Free Edition permite hasta 3 Apps.
10. [x] **MCP conectado a Playground** — sin registro manual en AI Gateway, por el auto-descubrimiento vía prefijo `mcp-` (ver checkpoint arriba).
11. [ ] **← PRÓXIMO PASO. Construir y validar el agente en el Playground**: pegar el system prompt de arriba → probar con preguntas reales, incluida la del guardrail (pedirle que "rechace" a alguien - debe negarse) → pegar los transcripts en "Demo Q&A" abajo. Una vez validado: Get Code → Export to Databricks Apps.
12. [ ] Abrir la App del dashboard y confirmar que el shortlist armado por el agente se ve ahí, con puesto y nota.
13. [ ] **Free Edition auto-detiene las Apps a las 24h de inactividad** — reiniciar `mcp-recruiter-copilot`, `dashboard` y el agente exportado antes del check-in (24 ago) y la presentación (26 ago).

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
- **`INSUFFICIENT_PRIVILEGES` / `USE SCHEMA` al leer Gold desde el dashboard:** el service principal de cada Databricks App necesita grants explícitos de Unity Catalog (`USE CATALOG`, `USE SCHEMA`, `SELECT`) sobre `recruitment_copilot.gold`/`.silver` — declarar el SQL Warehouse o el índice como "Resource" de la App solo le da acceso al *cómputo*, no a los datos. Resuelto otorgando los grants a los service principals de `recruiter-mcp` y `dashboard` vía Catalog Explorer.
- **`'NoneType' object has no attribute 'schema'` en `resume_broker._query`:** `execute_statement(..., wait_timeout="30s")` puede volver con el statement todavía en `PENDING`/`RUNNING` si el warehouse serverless está arrancando en frío, dejando `response.manifest` en `None`. Resuelto pollendo con `get_statement` hasta un estado terminal antes de leer `manifest`/`result`.
- **`Please specify either personal access token or service principal client ID and secret` en Vector Search:** `VectorSearchClient()` sin argumentos solo auto-detecta credenciales dentro de un notebook de Databricks, no dentro de una Databricks App. Resuelto pasándole explícitamente `service_principal_client_id`/`_secret` desde las env vars que Databricks Apps ya inyecta (`DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET`), y el `workspace_url` resuelto vía `WorkspaceClient().config.host`.
- **Registrar el MCP vía AI Gateway pedía un client OAuth completo (o un Bearer token) que no llegó a conectar:** para un MCP que ya es una Databricks App del mismo workspace, ese registro manual es para MCPs externos, no la ruta esperada. Resuelto desplegando el MCP server como una App con nombre prefijado `mcp-` (`mcp-recruiter-copilot`) — Playground la auto-descubre en el dropdown de Tools sin configurar auth a mano.
- **`candidate_shortlist.email` guardaba un UUID en vez del email real:** `_get_end_user_email()` leía el header `x-forwarded-user` (un ID interno) en vez de `x-forwarded-email` (el email). Ambos headers llegan de Databricks Apps pero significan cosas distintas. Resuelto priorizando `x-forwarded-email`.
- **El agente omitía `job_description` al shortlistear, aunque el system prompt y el docstring del tool decían "obligatorio":** con `job_description: str | None = None` en la firma de `shortlist_candidate`, el parámetro seguía siendo opcional a nivel de schema — reforzar la instrucción en prosa (dos veces, incluso con "REQUIRED"/"OBLIGATORIO") no cambió el comportamiento del modelo, que lo siguió omitiendo. Resuelto sacando el default (pasa a `job_description: str`, sin `None`) para que el schema del tool exponga el argumento como realmente requerido; el modelo pasa `""` cuando no aplica en vez de inventar una descripción. Lección: para un LLM tool-calling, la forma del schema pesa más que la redacción del prompt.

## Plan completo

El detalle de decisiones, cronograma y accesos está en [`PLAN.md`](PLAN.md).

## Docente

Repositorio público — acceso de lectura garantizado para `AndresACV` sin necesidad de invitación explícita.
