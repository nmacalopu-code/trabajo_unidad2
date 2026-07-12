# Trabajo Unidad 2 — Pipeline reproducible con Nextflow, PySpark y Bash

Pipeline reproducible que permite descarga automatica de datos publicos,
procesamiento distribuido con PySpark, automatizacion con Bash e integracion
de procesos externos, todo empleando **Nextflow**.

> Estado del proyecto: en construccion (Fase 1 — estructura inicial).

---

## 1. Descripcion general

El pipeline esta compuesto por tres procesos principales:

1. **Proceso 1** — Descarga automatica de archivos FASTA desde Figshare e
   integracion del analisis de secuencias biologicas del Problema 1 usando PySpark.
2. **Proceso 2** — Integracion y adaptacion de procesos externos
   (`calculoBashTask` y `generaReporteTask`) del repositorio `tap_pipeline_gen`.
3. **Proceso 3** — Reporte final.

Todos los resultados se generan dentro de la carpeta `Results/`.

---

## 2. Requisitos

- Ubuntu
- Java (OpenJDK 17)
- Nextflow
- Python 3.10+ con: `pyspark`, `pandas`, `numpy`, `matplotlib`, `scipy`, `requests`
- Git y una cuenta de GitHub

La instalacion recomendada del entorno esta en `env/environment.yml`.

---

## 3. Estructura del repositorio

```
trabajo_unidad2/
├── README.md                 # este archivo
├── main.nf                   # pipeline de Nextflow
├── nextflow.config           # parametros y recursos
├── scripts/                  # scripts usados por el pipeline
│   ├── get_figshare_urls.py
│   ├── problem1_pyspark_pipeline.py
│   ├── run_external_tasks.sh
│   └── final_report.py
├── Data/                     # datos descargados (se ignoran en git)
├── Results/                  # resultados generados por el pipeline
├── Reports/                  # reportes generados
├── env/
│   └── environment.yml       # entorno reproducible
├── informe/
│   └── informe_unidad2.qmd   # informe final
├── .gitignore
└── LICENSE
```

---

## 4. Instalacion

```bash
# 1. Clonar el repositorio
git clone git@github.com:USUARIO/trabajo_unidad2.git
cd trabajo_unidad2

# 2. Opcional - Crear el entorno con conda
conda env create -f env/environment.yml
conda activate tap_unidad2
```

---

## 5. Ejecucion

```bash
nextflow run main.nf \
  --figshare_id ID_DEL_DATASET \
  --reference input1.fasta \
  --query input2.fasta \
  --partitions 4 \
  --outdir Results \
  -profile local \
  -with-report Results/nextflow_report.html \
  -with-trace Results/trace.txt \
  -with-timeline Results/timeline.html \
  -with-dag Results/pipeline_dag.html
```

---

## 6. Datos de entrada (Figshare)

- **Enlace al registro:** https://doi.org/10.6084/m9.figshare.32968955
- **ID del registro:** `32968955`
- **DOI:** 10.6084/m9.figshare.32968955
- **Licencia de los datos:** CC BY 4.0

La descarga es automatica dentro del pipeline. 

---

## 7. Declaracion de uso de IA

Este trabajo fue desarrollado con apoyo de Claude.AI, para la corrección de la redacción, 
aplicacion de algunos conceptos o definiciones, así como comandos que fui aprendiendo en el avance de programación, además de permitirme dudas de validación manual en algunos resultos.

