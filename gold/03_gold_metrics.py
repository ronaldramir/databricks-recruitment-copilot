# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — métricas de negocio
# MAGIC
# MAGIC Lee `silver_resumes` y produce dos tablas:
# MAGIC - `gold_category_stats` (obligatoria): conteo, `word_count` y `leadership_score` promedio por
# MAGIC   categoría — responde directamente la pregunta de negocio.
# MAGIC - `gold_top_candidates` (extra): ranking de los currículums con mayor `leadership_score`
# MAGIC   dentro de cada categoría — es lo que el agente de la capa de consumo usa para recomendar a
# MAGIC   quién revisar primero.
# MAGIC
# MAGIC Escritura con `mode("overwrite")`, mismo criterio de idempotencia que Silver: se recalcula
# MAGIC entero desde Silver en cada corrida, así que dos corridas seguidas dan el mismo resultado.

# COMMAND ----------

CATALOG = "recruitment_copilot"
SILVER_TABLE = f"{CATALOG}.silver.silver_resumes"
GOLD_CATEGORY_STATS_TABLE = f"{CATALOG}.gold.gold_category_stats"
GOLD_TOP_CANDIDATES_TABLE = f"{CATALOG}.gold.gold_top_candidates"
TOP_N_PER_CATEGORY = 10

silver_df = spark.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `gold_category_stats`
# MAGIC
# MAGIC ¿Qué categorías de currículums muestran más señales de experiencia y liderazgo?

# COMMAND ----------

from pyspark.sql import functions as F

category_stats_df = (
    silver_df.groupBy("category")
    .agg(
        F.count("*").alias("resume_count"),
        F.round(F.avg("word_count"), 1).alias("avg_word_count"),
        F.round(F.avg("leadership_score"), 2).alias("avg_leadership_score"),
    )
    .orderBy(F.desc("avg_leadership_score"))
)

category_stats_df.write.mode("overwrite").saveAsTable(GOLD_CATEGORY_STATS_TABLE)

display(spark.table(GOLD_CATEGORY_STATS_TABLE).orderBy(F.desc("avg_leadership_score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `gold_top_candidates` (opcional)
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
# MAGIC ## 3. Respuesta a la pregunta de negocio
# MAGIC
# MAGIC **¿Qué categorías de currículums muestran más señales de experiencia y liderazgo, y qué
# MAGIC candidatos destacan dentro de cada una?**
# MAGIC
# MAGIC Sobre 2483 currículums en 24 categorías (1 currículum en cuarentena por no dar texto
# MAGIC extraíble): las categorías con mayor `avg_leadership_score` son **Digital-Media**
# MAGIC (1.85), **Consultant** (1.67) y **Business-Development** (1.54) — currículums con más "led",
# MAGIC "managed", "architected" y "founded" que "assisted", "supported" o "helped", consistente con
# MAGIC roles orientados a gestión y liderazgo de iniciativas. En el otro extremo, **Teacher** (-0.90),
# MAGIC **Arts** (-0.40) y **Sales** (-0.34) promedian negativo — más lenguaje de rol de apoyo que de liderazgo,
# MAGIC esperable en posiciones más operativas o individuales.
# MAGIC
# MAGIC Para un equipo de reclutamiento, esto sugiere priorizar la revisión manual en las categorías
# MAGIC de mayor `avg_leadership_score` cuando el objetivo es cubrir posiciones que requieren
# MAGIC experiencia de liderazgo — y dentro de esas categorías, `gold_top_candidates` ya trae el
# MAGIC ranking de por quién empezar.
# MAGIC