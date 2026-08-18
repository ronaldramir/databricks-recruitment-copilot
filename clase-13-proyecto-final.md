# Universidad Latina de Costa Rica
### Técnico en Ingeniería de Datos con Databricks

**MÓDULO 2: APACHE SPARK - INGESTA DE DATOS CON LAKEFLOW CONNECT**

> **PROYECTO FINAL**

# Proyecto Final: De la Ingesta al Producto de Datos

*Pipeline medallion end-to-end sobre la fuente de datos que usted elija, orquestado con Job o Lakeflow Declarative Pipelines, versionado en GitHub y consumido desde un dashboard, un Genie Space o un agente*

| | |
|---|---|
| **Clase** | Clase 13 |
| **Semana** | Semana 07 - Proyecto Final |
| **Docente** | Andrés Calvo Vargas |

---

## Información del Proyecto

| | |
|---|---|
| **Entregable** | Pipeline medallion end-to-end (ingesta, Bronze, Silver, Gold) orquestado, versionado en GitHub y consumido desde una capa de visualización o de agentes |
| **Valor** | 30% de la nota final |
| **Fecha de asignación** | Lunes 17 de agosto de 2026 (Clase 13, kick-off) |
| **Check-in obligatorio** | Lunes 24 de agosto de 2026 (Clase 15), revisión 1:1 |
| **Presentación y entrega** | Miércoles 26 de agosto de 2026 (Clase 16), 10 a 15 min por estudiante |
| **Modalidad** | Individual |
| **Formato de entrega** | No hay documento que redactar. Se entrega: (1) el repositorio de GitHub con todo el código del pipeline y acceso de lectura para AndresACV, (2) el acceso a su capa de consumo (dashboard, Genie Space, Power BI o agente) para que el docente pueda ver la capa Gold, y (3) la presentación en vivo del proyecto funcionando. |

---

## Descripción General

El proyecto final lo lleva del dato crudo al producto de datos. Usted elige la fuente, elige el caso de negocio y elige cómo se consume el resultado. Lo que no cambia es el camino: ingesta a una capa Bronze, refinamiento por arquitectura medallion (Silver y Gold), orquestación repetible, versionado en GitHub y una capa de consumo a la que el docente tenga acceso.

El proyecto es individual.

**Qué demuestra este proyecto:** que usted sabe traer datos de una fuente real, refinarlos por capas con criterios de calidad, automatizar el flujo como en producción, versionar su trabajo en Git y entregar un resultado que alguien más pueda consumir sin pedirle ayuda. Esa es la cadena completa de valor de un data engineer.

---

## Arquitectura del Proyecto

| Componente | Rol |
|---|---|
| **Fuente** | La que usted quiera: Databricks Marketplace, Kaggle, una API, archivos propios, logs, un conector (por ejemplo Monday CRM), datos no estructurados, etc. |
| **Bronze** | Ingesta de la fuente hacia tablas Delta con la data cruda, tal cual llega, con metadata de ingesta. |
| **Silver** | Limpia, valida y enriquece: tipos correctos, sin nulls problemáticos, sin duplicados, columnas derivadas. |
| **Gold** | Calcula las métricas de negocio que alimentan la capa de consumo. |
| **Orquestación** | Un Job de Databricks o un Lakeflow Spark Declarative Pipeline que corre las capas en orden, de forma repetible e idempotente. |
| **Capa de consumo** | Dashboard AI/BI, Genie Space, Power BI, un agente sobre sus datos, o la combinación que prefiera. |
| **GitHub** | Todo el código del pipeline versionado, con acceso de lectura para el docente. |

---

## Requisitos del Proyecto

### 1. Fuente de datos y caso de negocio

La fuente es libre: Databricks Marketplace, Kaggle, una API pública, archivos CSV o JSON propios, logs de una aplicación, un conector a un sistema (CRM, ERP), datos no estructurados (texto, PDFs, imágenes). Lo único obligatorio es que usted la ingiera al lakehouse.

- El dataset debe tener suficiente riqueza para analizar: al menos una dimensión para agrupar (categoría, región, fecha, usuario) y al menos una métrica para agregar (monto, cantidad, duración, conteo).
- Defina una pregunta de negocio clara que su capa Gold y su capa de consumo respondan.

**Ejemplos ya conversados con el grupo**

| Estudiante | Datos | Capa de consumo |
|---|---|---|
| Mario | Datos financieros | Genie Agents |
| Andrea (Pao) | Logs de Guild Wars 2 con arcdps, o mapeo de accidentes de tránsito (por qué se da un accidente) | Databricks Apps o Dashboard |
| Adriana | Spotify o Netflix | Dashboard de Databricks |
| Ronald | Datos no estructurados | Por definir |
| José | Monday CRM (conector de Databricks) | Agents |
| Sebastián | Entretenimiento (películas, público) | Genie Agents o dashboard en Power BI |

Estos son los casos que ya mapeamos en clase. Si quiere cambiar de fuente o de capa de consumo, puede hacerlo: solo confírmelo en el check-in.

### 2. Capa Bronze: ingesta (20%)

Aquí es donde los datos entran al lakehouse. La forma de ingerir depende de su fuente, y todas son válidas:

- Si la data ya vive en Databricks (Marketplace, catálogo existente), su ingesta es la lectura de ese catálogo hacia sus propias tablas Delta crudas.
- Si la data viene de afuera (Kaggle, API, archivos, logs), puede usar un notebook o un archivo `.py` que la extraiga y la deje disponible, y otro que cree las tablas Delta con la data cruda. También puede hacerlo todo en un solo archivo si el caso es simple.
- Si aplica a su fuente, puede reutilizar Auto Loader (`cloudFiles`) para la ingesta incremental, como en el L4.

Requisitos:

- La capa Bronze guarda los datos tal cual llegan: sin limpiar, sin deduplicar, sin castear.
- Agregue metadata de ingesta (por ejemplo `_ingested_at` y `_source`).
- Escriba al menos una tabla Delta Bronze de forma idempotente.
- Documente en una celda Markdown de qué fuente parte, qué columnas trae y qué pregunta de negocio va a responder.

### 3. Capa Silver: limpieza y transformación (20%)

Leyendo de Bronze:

- Seleccionar y tipar las columnas que necesita; castear a los tipos correctos.
- Limpiar: manejar nulls, normalizar strings, filtrar o apartar filas inválidas (quarantine), quitar duplicados.
- Enriquecer: crear al menos una columna derivada útil para el análisis (extraer el mes de una fecha, una categoría calculada, un margen, un sentimiento, lo que aplique a su caso).
- Escribir al menos una tabla Delta Silver de forma idempotente.

### 4. Capa Gold: métricas de negocio (20%)

Leyendo de Silver:

- Calcular las agregaciones de negocio que responden su pregunta (agrupar por dimensión, sumar, promediar, contar, rankings) con DataFrames o Spark SQL.
- Escribir al menos una tabla Delta Gold de forma idempotente.
- Responder, en una celda Markdown, la pregunta de negocio con el resultado obtenido.

**Mínimos y máximos:** mínimo un archivo por capa (bronze, silver, gold) y una tabla por capa. De ahí para arriba, tenga los archivos y las tablas que su caso necesite.

### 5. Orquestación (15%)

Su pipeline tiene que poder correr solo, en orden y sin duplicar datos. Dos caminos válidos:

- **Job de Databricks (Workflows):** una tarea por capa, encadenadas por dependencia (bronze, luego silver, luego gold). Ejecutarlo y verificar que todas las tareas terminan en verde.
- **Lakeflow Spark Declarative Pipelines:** declarar las capas con decoradores (`@dlt.table` o `@dp.table`) y dejar que el motor resuelva las dependencias y la calidad con expectations.

Demostrar idempotencia: correr el pipeline una segunda vez no debe duplicar datos (los conteos de Gold se mantienen).

### 6. Capa de consumo sobre Gold (15%)

La capa Gold tiene que poder consumirse sin abrir un notebook. Elija la forma que mejor le calce a su caso:

- Dashboard AI/BI de Databricks con al menos dos visualizaciones que respondan su pregunta.
- Genie Space configurado sobre sus tablas Gold, capaz de responder preguntas en lenguaje natural del caso de negocio.
- Power BI u otra herramienta de BI conectada a Gold.
- Un agente que use sus datos Gold en un flujo tipo Agent Bricks.
- Una Databricks App o la combinación que usted quiera.

Sea cual sea, tiene que tener un título y quedar claro qué muestra y qué pregunta responde.

### 7. Accesos para el docente (obligatorio)

Dos accesos, sin los cuales el proyecto no se puede evaluar:

- **Capa de consumo y Gold:** el docente tiene que poder ver el resultado. Comparta el dashboard, el Genie Space o el reporte con `andres.calvo5@ulatina.net` en modo lectura. Si su capa de consumo lee directo de la tabla, otorgue también el permiso sobre Gold:

```sql
GRANT SELECT ON TABLE workspace.default.<apellido>_pf_gold
TO `andres.calvo5@ulatina.net`;
```

- **Repositorio de GitHub:** agregue a `AndresACV` como colaborador con permiso de lectura (Read), o deje el repositorio público.

### 8. GitHub: versionado obligatorio

- Todo el código del proyecto vive en un repositorio de GitHub: los archivos de ingesta, los de cada capa y la definición del pipeline o del Job.
- Trabaje con commits durante el desarrollo, no con una sola subida al final. La historia del repositorio es parte de lo que se ve.
- Puede conectarlo desde Databricks con Git Folders (Repos) y trabajar directamente contra el repo.
- Acceso de lectura para `AndresACV`.

### 9. Genie como copiloto (obligatorio)

Use Genie durante el desarrollo para guiarse: explorar el dataset, generar consultas, entender errores y proponer transformaciones. La idea es que se apoye en el LLM para construir el pipeline más rápido y aprenda a dirigirlo con buenos prompts en lugar de escribir todo a mano.

Si encuentra un error y no lo puede resolver: intente diagnosticar leyendo el mensaje, consultando la documentación de Databricks, preguntándole a Genie y probando variaciones. Si lo resuelve, documente la solución en una celda Markdown. Si NO lo resuelve, documente: (a) el error exacto, (b) qué intentó, (c) su hipótesis, y continúe con lo siguiente.

**No se quede pegado. Esa documentación cuenta para la evaluación!!!**

### Celdas Markdown en el código

No hay documento de decisiones ni informe que redactar. La explicación va dentro del código: cada archivo lleva celdas Markdown que expliquen qué hace esa parte y por qué es importante para el pipeline. Eso es lo que se revisa en el repositorio y lo que usted explica en la presentación.

---

## Opcionales (puntos extra)

- Programar el Job o el pipeline con un schedule (trigger por tiempo).
- Expectations de calidad de datos en el pipeline declarativo.
- Una segunda tabla Gold con otra perspectiva de negocio y su visualización.
- Permisos a nivel de esquema en vez de tabla, mostrando dominio de la jerarquía de Unity Catalog.
- Combinar dos capas de consumo (por ejemplo dashboard y Genie Space sobre las mismas tablas Gold).

---

## Cronograma

| Hito | Fecha | Detalle |
|---|---|---|
| Kick-off | Lunes 17 de agosto (Clase 13) | Entrega de este documento, elección de fuente y caso, arranque del taller |
| Taller | Clase 13 (17 Ago) y Clase 14 (19 Ago) | Trabajo guiado en clase con el docente disponible |
| Check-in 1:1 | Lunes 24 de agosto (Clase 15) | Revisión de avance: fuente ingerida, silver y gold corriendo, repositorio creado. Obligatorio (su ausencia resta de la nota de presentación) |
| Presentación y entrega | Miércoles 26 de agosto (Clase 16) | Demo en vivo del pipeline, la orquestación, la capa de consumo y los accesos otorgados |

---

## Presentación Individual (10 a 15 min)

La presentación es la entrega: no hay documento que enviar aparte.

1. **Caso de negocio (2 min):** qué fuente eligió, qué pregunta responde, quién consumiría el resultado.
2. **Recorrido del pipeline (4 min):** mostrar la ingesta a Bronze, Silver y Gold, explicando las decisiones de limpieza, enriquecimiento y agregación.
3. **Orquestación en vivo (2 a 3 min):** ejecutar el Job o el pipeline declarativo, mostrar que termina bien y que re-ejecutarlo no duplica datos.
4. **Capa de consumo (3 min):** mostrar el dashboard, el Genie Space, el reporte o el agente respondiendo la pregunta de negocio con los datos.
5. **Accesos y repositorio (1 min):** mostrar el repositorio de GitHub y confirmar el acceso otorgado a la capa de consumo o a Gold.
6. **Preguntas (1 a 2 min):** responder dudas del docente y del grupo.

---

## Criterios de Evaluación

| Criterio | Peso |
|---|---|
| Bronze: ingesta de la fuente elegida hacia tablas Delta crudas | 20% |
| Silver: limpieza, validación y enriquecimiento correctos | 20% |
| Gold: métricas de negocio que responden la pregunta | 20% |
| Orquestación con Job o Lakeflow Declarative Pipelines, idempotente | 15% |
| Presentación final: capa de consumo funcionando, accesos otorgados (visualización o Gold y repositorio GitHub) y explicación del pipeline | 25% |
