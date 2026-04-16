# 🦠 Análisis de Infecciones Respiratorias Agudas Graves (IRAG) con Apache Spark

## 📌 Descripción del Proyecto

Este proyecto tiene como objetivo analizar datos de **Infecciones Respiratorias Agudas Graves (IRAG)** utilizando **Apache Spark** en modo batch, con el fin de identificar patrones, tendencias y métricas clave como hospitalizaciones, muertes y distribución temporal de los casos.

El procesamiento se realizó en un entorno Linux (Ubuntu) mediante conexión remota usando PuTTY y ejecutando scripts en PySpark.

---

## 🎯 Problemática

Las infecciones respiratorias agudas representan un problema de salud pública importante, ya que pueden generar:

* Alta demanda en servicios hospitalarios
* Incremento en tasas de mortalidad
* Saturación de instituciones de salud
* Dificultad en la toma de decisiones sin análisis de datos

Por ello, es fundamental analizar los datos históricos para:

* Detectar tendencias
* Identificar periodos críticos
* Evaluar impacto por institución
* Apoyar decisiones en salud pública

---

## ⚙️ Tecnologías Utilizadas

* 🐍 Python
* ⚡ Apache Spark (PySpark)
* 🐧 Ubuntu (Máquina Virtual)
* 💻 PuTTY (conexión remota)

---

## 📂 Estructura del Proyecto

```
.
├── ira_batch_processing.py
├── data/
│   └── ira_datos.csv
├── output/
│   ├── hospitalizaciones_anio/
│   ├── muertes_anio/
│   ├── top_ips/
│   └── casos_mes/
└── README.md
```

---

## 📥 Fuente de Datos

El dataset contiene información sobre:

* Fecha de notificación
* Número de hospitalizaciones
* Número de muertes
* Eventos de atención médica
* Instituciones de salud (IPS)

---

## 🧹 Proceso de Limpieza de Datos

Se realizaron las siguientes transformaciones:

* Renombrado de columnas (eliminando caracteres especiales)
* Conversión de fechas a formato `timestamp`
* Eliminación de valores nulos
* Conversión de columnas numéricas

---

## 🔄 Transformación de Datos

Se generaron nuevas variables:

* 📅 Año (`ANIO`)
* 📆 Mes (`MES`)

Esto permitió agrupar y analizar los datos de forma temporal.

---

## 📊 Análisis Realizado

Se ejecutaron los siguientes análisis:

### 1. Total de registros

Cantidad total de datos procesados.

### 2. Hospitalizaciones por año

Permite identificar los años con mayor carga hospitalaria.

### 3. Muertes por año

Muestra la evolución de la mortalidad.

### 4. Top 10 IPS con más casos

Identifica las instituciones con mayor número de hospitalizaciones.

### 5. Casos por mes

Permite detectar patrones estacionales (ej: picos en ciertos meses).

---

## 💾 Resultados

Los resultados se almacenan automáticamente en formato CSV dentro de la carpeta:

```
output/
```

Cada análisis genera un conjunto de archivos particionados listos para su uso en visualización o análisis posterior.

---

## ▶️ Ejecución del Proyecto

### 1. Clonar el repositorio

```bash
git clone <URL_DEL_REPOSITORIO>
cd <NOMBRE_DEL_REPOSITORIO>
```

---

### 2. Ubicar el dataset

Colocar el archivo en:

```
data/ira_datos.csv
```

---

### 3. Ejecutar el script

```bash
spark-submit ira_batch_processing.py
```

---

## 📈 Ejemplo de Salida

Durante la ejecución, se muestran resultados como:

* Número total de registros
* Primeras filas del dataset
* Agregaciones por año y mes

---

## 🧠 Conclusiones

* Apache Spark permite procesar grandes volúmenes de datos de forma eficiente
* El análisis batch facilita la identificación de tendencias históricas
* Los resultados pueden apoyar la toma de decisiones en el sector salud
* La distribución temporal ayuda a detectar temporadas críticas

---

## 🚀 Mejoras Futuras

* Implementar visualización con herramientas como Power BI o Tableau
* Integrar procesamiento en tiempo real con Spark Streaming
* Automatizar carga de datos desde APIs
* Aplicar modelos predictivos

---

## 👩‍💻 Autor

Proyecto desarrollado como práctica de análisis de datos con Apache Spark.

---

## 📌 Notas

* Asegúrate de tener correctamente configurado Apache Spark
* Verifica la ruta del archivo CSV antes de ejecutar
* Los resultados se generan en múltiples archivos debido al procesamiento distribuido

---
