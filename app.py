from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, sum as _sum
from pyspark.sql.types import IntegerType

# ==============================
# 1. Crear sesión de Spark
# ==============================
spark = SparkSession.builder \
    .appName("IRAG_Batch_Processing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================
# 2. Cargar datos
# ==============================
ruta = "data/ira_datos.csv"

df = spark.read.csv(
    ruta,
    header=True,
    inferSchema=True,
    sep=","
)

print("=== ESQUEMA ORIGINAL ===")
df.printSchema()

# ==============================
# 3. LIMPIEZA DE DATOS
# ==============================

# Renombrar columnas
for col_name in df.columns:
    new_col = col_name.replace(" ", "_") \
                      .replace("Ã‘", "N") \
                      .replace("Ã", "A") \
                      .replace("Ó", "O") \
                      .replace("Í", "I") \
                      .replace("É", "E") \
                      .replace("Á", "A") \
                      .replace("(", "") \
                      .replace(")", "") \
                      .replace(",", "") \
                      .replace("__", "_")
    df = df.withColumnRenamed(col_name, new_col)

print("=== COLUMNAS LIMPIAS ===")
print(df.columns)

# Convertir fecha
df = df.withColumn(
    "FECHA_NOTIFICACION",
    to_timestamp(col("FECHA_NOTIFICACION"))
)

# Eliminar nulos
df = df.dropna()

# ==============================
# 4. TRANSFORMACIÓN DE DATOS
# ==============================

df = df.withColumn("ANIO", year(col("FECHA_NOTIFICACION")))
df = df.withColumn("MES", month(col("FECHA_NOTIFICACION")))

columnas_numericas = [
    "TOTAL_CASOS_DE_HOSPITALIZACIONES_POR_IRAG",
    "TOTAL_DE_MUERTES_POR_IRAG",
    "TOTAL_DE_EVENTOS_DE_MORBILIDAD_DE_IRAG_POR_CONSULTA_EXTERNA_Y_URGENCIAS"
]

for c in columnas_numericas:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast(IntegerType()))

# ==============================
# 5. ANÁLISIS EXPLORATORIO (EDA)
# ==============================

print("=== TOTAL REGISTROS ===")
print(df.count())

print("=== MUESTRA DE DATOS ===")
df.show(5)

# Hospitalizaciones por año
hospitalizaciones_anio = df.groupBy("ANIO") \
    .agg(_sum("TOTAL_CASOS_DE_HOSPITALIZACIONES_POR_IRAG")
    .alias("TOTAL_HOSPITALIZACIONES"))

print("=== HOSPITALIZACIONES POR AÑO ===")
hospitalizaciones_anio.show()

# Muertes por año
muertes_anio = df.groupBy("ANIO") \
    .agg(_sum("TOTAL_DE_MUERTES_POR_IRAG")
    .alias("TOTAL_MUERTES"))

print("=== MUERTES POR AÑO ===")
muertes_anio.show()

# Top IPS
top_ips = df.groupBy("UNIDAD_PRIMARIA_GENERADORA_DEL_DATO_QUE_NOTIFICA_EL_EVENTO") \
    .agg(_sum("TOTAL_CASOS_DE_HOSPITALIZACIONES_POR_IRAG")
    .alias("TOTAL")) \
    .orderBy(col("TOTAL").desc()) \
    .limit(10)

print("=== TOP 10 IPS CON MÁS CASOS ===")
top_ips.show(truncate=False)

# Casos por mes
casos_mes = df.groupBy("ANIO", "MES") \
    .agg(_sum("TOTAL_CASOS_DE_HOSPITALIZACIONES_POR_IRAG")
    .alias("TOTAL")) \
    .orderBy("ANIO", "MES")

print("=== CASOS POR MES ===")
casos_mes.show()

# ==============================
# 6. EXPORTAR RESULTADOS
# ==============================

hospitalizaciones_anio.write.mode("overwrite").csv("output/hospitalizaciones_anio", header=True)
muertes_anio.write.mode("overwrite").csv("output/muertes_anio", header=True)
top_ips.write.mode("overwrite").csv("output/top_ips", header=True)
casos_mes.write.mode("overwrite").csv("output/casos_mes", header=True)

print("=== PROCESO FINALIZADO ===")

spark.stop()
