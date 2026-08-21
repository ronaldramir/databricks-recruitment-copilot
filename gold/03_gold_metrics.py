# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — métricas de negocio
# MAGIC
# MAGIC Lee `silver_resumes` (y, para la tabla de balance del dataset, también `bronze_resumes` y
# MAGIC `silver_resumes_quarantine`) y produce cinco tablas, cada una respondiendo una pregunta de
# MAGIC negocio distinta para el equipo de reclutamiento:
# MAGIC
# MAGIC - `gold_category_stats` (obligatoria): señales de liderazgo y seniority por categoría.
# MAGIC - `gold_top_candidates` (extra): ranking de candidatos por `leadership_score` dentro de cada
# MAGIC   categoría — a quién revisar primero.
# MAGIC - `gold_contact_quality` (extra): qué % de candidatos por categoría están listos para
# MAGIC   contactar de inmediato (traen email y teléfono detectables).
# MAGIC - `gold_length_vs_leadership` (extra): ¿un currículum más largo realmente comunica más
# MAGIC   liderazgo, o el `word_count` solo mide verborragia?
# MAGIC - `gold_category_health` (extra): tasa de cuarentena por categoría — dónde el pipeline está
# MAGIC   perdiendo candidatos por PDFs no legibles.
# MAGIC
# MAGIC Escritura con `mode("overwrite")`, mismo criterio de idempotencia que Silver: se recalcula
# MAGIC entero desde Silver (y Bronze) en cada corrida, así que dos corridas seguidas dan el mismo
# MAGIC resultado.

# COMMAND ----------

CATALOG = "recruitment_copilot"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_resumes"
SILVER_TABLE = f"{CATALOG}.silver.silver_resumes"
QUARANTINE_TABLE = f"{CATALOG}.silver.silver_resumes_quarantine"

GOLD_CATEGORY_STATS_TABLE = f"{CATALOG}.gold.gold_category_stats"
GOLD_TOP_CANDIDATES_TABLE = f"{CATALOG}.gold.gold_top_candidates"
GOLD_CONTACT_QUALITY_TABLE = f"{CATALOG}.gold.gold_contact_quality"
GOLD_LENGTH_VS_LEADERSHIP_TABLE = f"{CATALOG}.gold.gold_length_vs_leadership"
GOLD_CATEGORY_HEALTH_TABLE = f"{CATALOG}.gold.gold_category_health"

TOP_N_PER_CATEGORY = 10

silver_df = spark.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `gold_category_stats`
# MAGIC
# MAGIC ¿Qué categorías de currículums muestran más señales de experiencia y liderazgo — y de
# MAGIC seniority declarada?
# MAGIC
# MAGIC `avg_leadership_score` mide *lenguaje* de liderazgo (verbos de acción como "led"/"managed");
# MAGIC `avg_seniority_score` mide *título* declarado ("senior"/"director"/"vp"). Mostrarlas juntas
# MAGIC deja ver cuándo coinciden (categorías con seniority y lenguaje de liderazgo alineados) y
# MAGIC cuándo no (títulos senior sin lenguaje de logro, o al revés).

# COMMAND ----------

from pyspark.sql import functions as F

category_stats_df = (
    silver_df.groupBy("category")
    .agg(
        F.count("*").alias("resume_count"),
        F.round(F.avg("word_count"), 1).alias("avg_word_count"),
        F.round(F.avg("leadership_score"), 2).alias("avg_leadership_score"),
        F.round(F.avg("seniority_score"), 2).alias("avg_seniority_score"),
    )
    .orderBy(F.desc("avg_leadership_score"))
)

category_stats_df.write.mode("overwrite").saveAsTable(GOLD_CATEGORY_STATS_TABLE)

display(spark.table(GOLD_CATEGORY_STATS_TABLE).orderBy(F.desc("avg_leadership_score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `gold_top_candidates`
# MAGIC
# MAGIC Ranking de los currículums con mayor `leadership_score` dentro de cada categoría, top
# MAGIC `TOP_N_PER_CATEGORY` — candidatos a shortlistear primero.

# COMMAND ----------

from pyspark.sql import Window

ranking_window = Window.partitionBy("category").orderBy(F.desc("leadership_score"))

top_candidates_df = (
    silver_df
    .withColumn("rank", F.rank().over(ranking_window))
    .filter(F.col("rank") <= TOP_N_PER_CATEGORY)
    .select("category", "resume_id", "leadership_score", "rank")
    .orderBy("category", "rank")
)

top_candidates_df.write.mode("overwrite").saveAsTable(GOLD_TOP_CANDIDATES_TABLE)

display(spark.table(GOLD_TOP_CANDIDATES_TABLE).orderBy("category", "rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `gold_contact_quality`
# MAGIC
# MAGIC ¿Qué categorías tienen currículums listos para contactar de inmediato, sin tener que abrir
# MAGIC el PDF a mano? Pregunta operativa directa para un reclutador con poco tiempo.

# COMMAND ----------

contact_quality_df = (
    silver_df.groupBy("category")
    .agg(
        F.count("*").alias("resume_count"),
        F.round(F.avg(F.col("has_email").cast("int")) * 100, 1).alias("pct_with_email"),
        F.round(F.avg(F.col("has_phone").cast("int")) * 100, 1).alias("pct_with_phone"),
        F.round(F.avg(F.col("contact_complete").cast("int")) * 100, 1).alias(
            "pct_contact_complete"
        ),
    )
    .orderBy(F.desc("pct_contact_complete"))
)

contact_quality_df.write.mode("overwrite").saveAsTable(GOLD_CONTACT_QUALITY_TABLE)

display(spark.table(GOLD_CONTACT_QUALITY_TABLE).orderBy(F.desc("pct_contact_complete")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `gold_length_vs_leadership`
# MAGIC
# MAGIC ¿Un currículum más largo realmente comunica más liderazgo, o `word_count` solo mide
# MAGIC verborragia? Se parte cada categoría en terciles por `word_count` (corto/medio/largo,
# MAGIC tamaños relativos a esa misma categoría, no al dataset entero) y se compara el
# MAGIC `avg_leadership_score` de cada tercil.

# COMMAND ----------

length_window = Window.partitionBy("category").orderBy("word_count")

length_bucket_order = (
    F.when(F.col("length_bucket") == "short", 1)
    .when(F.col("length_bucket") == "medium", 2)
    .otherwise(3)
)

length_vs_leadership_df = (
    silver_df
    .withColumn("length_tercile", F.ntile(3).over(length_window))
    .withColumn(
        "length_bucket",
        F.when(F.col("length_tercile") == 1, "short")
        .when(F.col("length_tercile") == 2, "medium")
        .otherwise("long"),
    )
    .groupBy("category", "length_bucket")
    .agg(
        F.count("*").alias("resume_count"),
        F.round(F.avg("word_count"), 1).alias("avg_word_count"),
        F.round(F.avg("leadership_score"), 2).alias("avg_leadership_score"),
    )
    .orderBy("category", length_bucket_order)
)

length_vs_leadership_df.write.mode("overwrite").saveAsTable(GOLD_LENGTH_VS_LEADERSHIP_TABLE)

overall_corr = silver_df.select(F.corr("word_count", "leadership_score")).first()[0]
print(f"Correlación word_count vs. leadership_score (dataset completo): {overall_corr:.3f}")

display(spark.table(GOLD_LENGTH_VS_LEADERSHIP_TABLE).orderBy("category", length_bucket_order))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `gold_category_health`
# MAGIC
# MAGIC ¿En qué categorías el pipeline está perdiendo candidatos por PDFs no legibles? Cruza el
# MAGIC conteo de ingesta en Bronze con el conteo en cuarentena de Silver — una tasa de cuarentena
# MAGIC alta en una categoría significa que el reclutador tiene menos candidatos disponibles ahí de
# MAGIC lo que el volumen de origen sugeriría, y es una señal de calidad de datos, no solo de
# MAGIC negocio.

# COMMAND ----------

bronze_counts_df = spark.table(BRONZE_TABLE).groupBy("category").agg(
    F.count("*").alias("ingested_count")
)
silver_counts_df = silver_df.groupBy("category").agg(F.count("*").alias("silver_count"))
quarantine_counts_df = spark.table(QUARANTINE_TABLE).groupBy("category").agg(
    F.count("*").alias("quarantine_count")
)

category_health_df = (
    bronze_counts_df
    .join(silver_counts_df, "category", "left")
    .join(quarantine_counts_df, "category", "left")
    .na.fill(0, ["silver_count", "quarantine_count"])
    .withColumn(
        "quarantine_rate_pct",
        F.round(F.col("quarantine_count") / F.col("ingested_count") * 100, 1),
    )
    .orderBy(F.desc("quarantine_rate_pct"))
)

category_health_df.write.mode("overwrite").saveAsTable(GOLD_CATEGORY_HEALTH_TABLE)

display(spark.table(GOLD_CATEGORY_HEALTH_TABLE).orderBy(F.desc("quarantine_rate_pct")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Respuesta a las preguntas de negocio
# MAGIC
# MAGIC _Completar estos cinco párrafos con los números reales después de correr este notebook en
# MAGIC Databricks — los valores de abajo son placeholders, no resultados de una corrida real._
# MAGIC
# MAGIC **1. ¿Qué categorías muestran más señales de experiencia y liderazgo, y qué candidatos
# MAGIC destacan dentro de cada una?** _(pregunta de negocio principal — `gold_category_stats` +
# MAGIC `gold_top_candidates`)_
# MAGIC
# MAGIC _(completar tras correr en Databricks)_
# MAGIC
# MAGIC **2. ¿La seniority declarada (títulos "senior"/"director"/"vp") coincide con el lenguaje de
# MAGIC liderazgo, o son señales distintas?** _(`gold_category_stats`, comparando
# MAGIC `avg_leadership_score` vs. `avg_seniority_score` por categoría)_
# MAGIC
# MAGIC _(completar tras correr en Databricks)_
# MAGIC
# MAGIC **3. ¿Qué categorías tienen currículums listos para contactar de inmediato?**
# MAGIC (`gold_contact_quality`)
# MAGIC
# MAGIC _(completar tras correr en Databricks)_
# MAGIC
# MAGIC **4. ¿Un currículum más largo realmente comunica más liderazgo?**
# MAGIC (`gold_length_vs_leadership` + la correlación impresa arriba)
# MAGIC
# MAGIC _(completar tras correr en Databricks)_
# MAGIC
# MAGIC **5. ¿En qué categorías el pipeline pierde más candidatos por PDFs no legibles?**
# MAGIC (`gold_category_health`)
# MAGIC
# MAGIC _(completar tras correr en Databricks)_
