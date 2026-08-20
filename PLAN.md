# Plan: Copiloto de Reclutamiento (Proyecto Final)

> Documento de traspaso. Leelo antes de empezar a construir — tiene todas las decisiones ya tomadas para no repetir la conversación. El plan visual con diagramas está publicado aparte (ver enlace al final).

## 1. Resumen

- **Fuente:** PDFs crudos de currículums, organizados en carpetas por categoría laboral.
- **Pregunta de negocio:** ¿qué categorías de currículums muestran más señales de experiencia y liderazgo, y qué candidatos destacan dentro de cada una?
- **Quién lo consumiría:** un equipo de reclutamiento.
- **Capas:** Bronze → Silver → Gold, orquestadas con un Job o Lakeflow Declarative Pipeline.
- **Capa de consumo:** agente conversacional (Agent Bricks) + MCP server, siguiendo el patrón de `databricks-lakebase-app-day-3` (agente que lee contexto y ejecuta una acción real, respaldada por Lakebase). Dashboard AI/BI simple como red de seguridad si falta tiempo.
- **Vector Search (opcional, se va a intentar):** matching semántico currículum↔vacante, como última pieza si sobra tiempo.

## 2. Por qué esta fuente y no otras

Se consideraron y descartaron:

- **Reseñas de producto (Datafiniti Consumer Reviews, Google Play Store Apps+Reviews):** ambas son tablas con una columna de texto libre — más "estructurado con texto adentro" que datos no estructurados de origen. No calzaba con lo que ya quedó anotado como el tipo de fuente en el documento del profesor.
- **Se eligió:** [Resume Dataset (Kaggle, snehaanbhawal)](https://www.kaggle.com/datasets/snehaanbhawal/resume-dataset) — ~2 400 currículums reales en PDF, en carpetas por categoría (HR, IT, Healthcare, Sales, Finance, Engineering, y otras — 24 categorías en total). El dataset también trae un CSV con el texto ya extraído (`Resume_str`) — **ignorarlo a propósito** y leer los PDFs directamente desde las carpetas, para que la ingesta sea extracción real de un formato no estructurado.

## 3. Arquitectura de datos

### Bronze — `bronze_resumes`

- Ingesta con **Auto Loader** (`cloudFiles`, formato `binaryFile`) leyendo los PDFs crudos.
- Columnas: `resume_id`, `category` (de la carpeta), `raw_text` (extraído del PDF, sin normalizar), `file_path`, `_ingested_at`, `_source`.
- Nada de limpieza acá — tal cual sale de la extracción, con sus artefactos de espacios/saltos de línea.
- Escritura Delta idempotente (que reingesta no duplique).

### Silver — `silver_resumes`

Leyendo de Bronze:

- Normalizar espacios y artefactos de extracción del PDF.
- Deduplicar.
- Poner en cuarentena los PDFs que no se pudieron leer (texto vacío/nulo).
- **Columnas derivadas:**
  - `word_count`: longitud del currículum.
  - `leadership_score`: heurística sobre palabras clave — cuenta términos como "led", "managed", "architected", "founded" vs. términos como "assisted", "supported", "helped". Esta es la columna que cuenta como "columna derivada útil" del rúbrico.
- Columnas finales: `resume_id`, `category`, `clean_text`, `word_count`, `leadership_score`.

### Gold

- **`gold_category_stats`** (obligatoria): `category`, `resume_count`, `avg_word_count`, `avg_leadership_score`. Responde la pregunta de negocio.
- **`gold_top_candidates`** (opcional, puntos extra): `category`, `resume_id`, `leadership_score`, `rank`. Ranking de candidatos dentro de cada categoría — es lo que el agente usa para recomendar a quién revisar.

### Orquestación

- Job de Databricks (una tarea por capa, encadenadas) o Lakeflow Declarative Pipeline.
- Demostrar idempotencia: correr dos veces, verificar que los conteos de Gold no cambian.

## 4. Capa de consumo — patrón `day-3`, adaptado

Mismo esqueleto que `databricks-lakebase-app-day-3`: un agente con herramientas MCP que además puede **escribir una decisión real**, respaldada por Lakebase, con un dashboard que la refleja en vivo.

| Pieza | Nombre | Rol |
|---|---|---|
| MCP tool (lectura) | `get_category_stats`, `search_resumes` | Contexto para que el agente razone, sobre las tablas Gold. |
| MCP tool (escritura) | `shortlist_candidate` | Paralelo directo de `place_trade` en day-3. Agrega el currículum a una lista para revisión humana. |
| Lakebase | `candidate_shortlist` | Misma forma que `schema_watchlist.sql` de day-2/day-3: `email`, `resume_id`, `category`, `note`, `added_at`. |
| Dashboard | `dashboard/app.py` | Lee la misma tabla `candidate_shortlist` — vista humana del shortlist en tiempo casi real. |

### Human-in-the-loop (importante para el system prompt del agente)

El agente **sugiere y anota, nunca decide**: `shortlist_candidate` marca un currículum para que un reclutador humano lo revise — no aprueba ni descarta a nadie. Dejarlo explícito en el system prompt del agente y en el README. El hiring algorítmico tiene escrutinio real por sesgo (leyes tipo NYC Local Law 144, guías de EEOC) — esta frase de más es lo que hace que el proyecto se lea maduro en vez de ingenuo, especialmente si termina en un portafolio.

Ejemplo de línea para el system prompt (ajustar cuando se construya):

> Sos un copiloto de reclutamiento. Tu trabajo es *sugerir* candidatos para revisión, nunca decidir por tu cuenta. Cuando uses `shortlist_candidate`, explicá siempre por qué ese currículum merece revisión humana. Nunca rechaces ni "descartes" candidatos — esa decisión es exclusivamente humana.

## 5. Vector Search (implementado — Databricks Vector Search nativo)

- **Qué es:** búsqueda por similitud semántica (embeddings) en vez de por palabras exactas. Un *Delta Sync Index* apunta a `silver_resumes.clean_text`; Databricks calcula los embeddings (modelo hosteado, sin `sentence-transformers` local) y mantiene el índice sincronizado automáticamente cada vez que Silver cambia.
- **Decisión frente al patrón de `day-3`:** ese proyecto usa `sentence-transformers` + pgvector en Lakebase (embeddings calculados y guardados a mano). Acá se optó por Vector Search nativo en su lugar — cero dependencias pesadas en la App, sync automático, y es la feature que el módulo del curso ya documenta. Lakebase sigue siendo solo para `candidate_shortlist`, sin ningún cambio.
- **Cómo quedó armado:** `mcp_server/vector_search.py` y `dashboard/vector_search.py` (idénticos, uno por App) consultan el índice `recruitment_copilot.silver.silver_resumes_index` vía `VectorSearchClient`. Expuesto como tool `find_matching_resumes(job_description, limit)` en el MCP server (para el agente) **y** como caja de búsqueda en el dashboard (`/api/match_resumes`) — esto último es un extra que `day-3` no tenía: ahí el score de similitud solo lo veía el agente, nunca se mostraba en una UI.
- **Setup pendiente (una sola vez, en Databricks):**
  1. `ALTER TABLE recruitment_copilot.silver.silver_resumes SET TBLPROPERTIES (delta.enableChangeDataFeed = true);` (requisito para un Delta Sync Index).
  2. Crear un Vector Search endpoint.
  3. Crear el Delta Sync Index sobre `silver_resumes`, columna de embedding `clean_text`, PK `resume_id`, columna extra `category`.
- Referencia: [Databricks Vector Search — documentación oficial](https://docs.databricks.com/aws/en/vector-search/vector-search)

## 6. Plan de trabajo (14–26 de agosto de 2026)

- [ ] **Hasta el 16 ago (preparación):** descargar el Resume Dataset, confirmar estructura de carpetas, crear repo GitHub (`AndresACV` como colaborador de lectura), redactar la pregunta de negocio en Markdown, probar extracción de un PDF de muestra (`pypdf`/`pdfplumber`).
- [ ] **17 ago — Clase 13 (kick-off, obligatorio):** confirmar fuente y pregunta con el docente. Empezar notebook Bronze: Auto Loader `binaryFile` + extracción + metadata. Primer commit.
- [ ] **18 ago:** terminar Bronze (idempotente, validar que reingesta no duplique). Empezar Silver: normalizar texto, dedupe, cuarentena.
- [ ] **19 ago — Clase 14 (taller):** terminar Silver. Calcular `word_count` y `leadership_score`. Commit.
- [ ] **20–21 ago:** construir Gold (`gold_category_stats`, opcional `gold_top_candidates`). Responder la pregunta de negocio en Markdown. Commit.
- [ ] **22–23 ago:** Job o Lakeflow Pipeline encadenando las tres capas; correr dos veces y confirmar que Gold no duplica. Levantar Lakebase y la tabla `candidate_shortlist`. Construir `recruiter_mcp_server.py` (tools de lectura + `shortlist_candidate`) y el agente en Agent Bricks. Respaldo si falta tiempo: Dashboard AI/BI simple sobre Gold, sin agente.
- [ ] **24 ago — Clase 15 (check-in obligatorio):** mostrar al docente fuente ingerida, Silver y Gold corriendo, repo con historial de commits.
- [ ] **25 ago:** terminar la capa de consumo. Si sobra tiempo: Vector Search + `find_matching_resumes`. Otorgar accesos finales. Ensayar la presentación (10–15 min).
- [ ] **26 ago — Clase 16 (presentación y entrega):** demo en vivo — ingesta, capas, orquestación e idempotencia, capa de consumo.

## 7. Accesos obligatorios para el docente

1. **GitHub:** repo público — ya cumple el requisito de acceso de lectura para `AndresACV` sin necesidad de invitación explícita.
2. **Gold / capa de consumo:** compartir el agente, dashboard o Genie Space con `andres.calvo5@ulatina.net` en modo lectura, o:
   ```sql
   GRANT SELECT ON TABLE recruitment_copilot.gold.gold_category_stats
   TO `andres.calvo5@ulatina.net`;
   ```
   Catalog propio del proyecto (`recruitment_copilot`, schemas `bronze`/`silver`/`gold`) en vez de `workspace.default`, para no chocar con otros estudiantes en el catalog compartido de la clase.

## 8. Referencias

- Requisitos completos del proyecto: `clase-13-proyecto-final.md` (esta misma carpeta).
- Patrón de referencia reutilizado: `databricks-lakebase-app-day-3/` (esta misma carpeta) — especialmente `mcp_server/alpaca_mcp_server.py`, `mcp_server/alpaca_broker.py` y `mcp_server/schema_watchlist.sql` como plantilla para `recruiter_mcp_server.py`, `resume_broker.py` y el esquema de `candidate_shortlist`.
- Dataset: [kaggle.com/datasets/snehaanbhawal/resume-dataset](https://www.kaggle.com/datasets/snehaanbhawal/resume-dataset)
- Plan visual con diagramas (arquitectura, flujo de datos, timeline): https://claude.ai/code/artifact/f82ab419-353b-4014-bf9f-7130138aeca1
