# Trabajo Unidad 2 — Pipeline reproducible con Nextflow, PySpark y Bash

Pipeline reproducible que descarga datos públicos desde Figshare, ejecuta un
análisis de secuencias biológicas con **PySpark**, integra procesos externos
adaptados en **Bash** y genera un reporte final todo empleando con **Nextflow**.

**Autora:** Neyling Yuriko Teresa Macalopú Rimachi
**Curso:** Técnicas Avanzadas de Programación (TAP) 
**Repositorio:** https://github.com/nmacalopu-code/trabajo_unidad2

---

## 1. Descripción general

El pipeline ejecuta **cinco procesos de Nextflow**: 
| Proceso Nextflow | Rol | Salidas |
|---|---|---|
| `DOWNLOAD_FIGSHARE` | Descarga verificada desde Figshare | `Results/Problem1/downloaded_files*.txt` |
| `RUN_PROBLEM1_PYSPARK` | **Proceso 1** — análisis del Problema 1 con PySpark | `Results/Problem1/` (6 CSV + 4 figuras) |
| `CALCULO_BASH` | **Proceso 2** — `calculoBashTask` | `Results/External/calculo_bash_resultados.csv` |
| `GENERA_REPORTE` | **Proceso 2** — `generaReporteTask` | `Reports/External/` |
| `FINAL_REPORT` | **Proceso 3** — reporte final | `Reports/final_report.html`, `Results/Figures/final_summary_plot.png` |

`RUN_PROBLEM1_PYSPARK` y `CALCULO_BASH` corren **en paralelo**: ambos dependen
solo de la descarga. `GENERA_REPORTE` y `FINAL_REPORT` consumen la salida del
cálculo Bash. La topología completa está en `Results/pipeline_dag.html`.

**Convención de carpetas:** los datos van a `Results/`, los reportes a `Reports/`.

---

## 2. Requisitos

Este es el **entorno exacto** con el que se desarrolló y verificó el pipeline:

| Componente | Versión utilizada |
|---|---|
| Sistema operativo | `Linux-6.18.33.1-microsoft-standard-WSL2-x86_64-with-glibc2.43` |
| Java (OpenJDK) | `17.0.19` |
| Nextflow | `26.04.4.12445` |
| Git | `2.53.0` |
| Python | `3.14.4` |
| PySpark | `4.1.2` |
| PyTorch | `2.13.0+cpu` |
| NumPy | `2.5.1` |
| pandas | `3.0.3` |
| matplotlib | `3.11.0` |

> **Sobre la versión de Java.** El pipeline se probó únicamente con **Java 17**.
> Spark 4.x admite oficialmente Java 17 y 21, pero aquí no se verificó con 21.
> Con **Java 25 no funciona**: el fallo ocurre en tiempo de ejecución, no al
> arrancar, de modo que el error aparece a mitad del análisis. Si `java -version`
> reporta otra versión:
>
> ```bash
> sudo apt install openjdk-17-jdk
> export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
> export PATH="$JAVA_HOME/bin:$PATH"
> ```

> Las versiones de esta tabla se registran automáticamente en cada corrida dentro
> de `Results/Problem1/run_parameters.csv`, junto con el commit de git. No se
> transcriben a mano.

## 3.Instalación

```bash
git clone https://github.com/nmacalopu-code/trabajo_unidad2.git
cd trabajo_unidad2
```

**Opción A — venv.** Es el entorno con el que se desarrolló y
ejecutó el pipeline:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r env/requirements.txt
```

**Opción B — conda (alternativa por si se desea utilizar con esta opcion).** `env/environment.yml` replica las mismas versiones, pero esa ruta no fue ejecutada:

```bash
conda env create -f env/environment.yml
conda activate tap_unidad2
```

Las versiones de `env/requirements.txt` se generaron desde el entorno real de
ejecución. No hay que descargar datos: los baja el pipeline.

## 4. Ejecución

```bash
nextflow run main.nf --figshare_id 32968955 -profile local \
  -with-report   Results/nextflow_report.html \
  -with-trace    Results/trace.txt \
  -with-timeline Results/timeline.html \
  -with-dag      Results/pipeline_dag.html
```

Duración aproximada: 2–3 minutos.

### Parámetros

Todos se definen en `nextflow.config` y se pueden sobreescribir por línea de comandos.

| Parámetro | Por defecto | Descripción |
|---|---|---|
| `--figshare_id` | (obligatorio) | ID del registro de Figshare |
| `--reference` | `Genoma_referencia.fasta` | FASTA de referencia |
| `--query` | `Secuenciacion4_correctedshortReads.fasta` | FASTA de consulta |
| `--download_all` | `false` | Si es `true`, descarga los 7 archivos (~3.4 GB) |
| `--partitions` | `4` | Particiones de Spark del análisis principal |
| `--benchmark_partitions` | `1,2,4,8,16` | Configuraciones del benchmark |
| `--repeats` | `3` | Repeticiones por configuración |
| `--n_reads` | `20` | Reads leídos del query |
| `--semilla` | `16` | Largo de la semilla en la búsqueda Bash |
| `--cnn_epochs` | `300` | Épocas de la CNN 1D |
| `--cnn_samples` | `5000` | Ventanas de entrenamiento |
| `--outdir` | `Results` | Carpeta de resultados |
| `--reports_dir` | `Reports` | Carpeta de reportes |

---

## 5. Datos de entrada (Figshare)

- **Registro:** https://doi.org/10.6084/m9.figshare.32968955
- **ID:** `32968955`
- **DOI:** `10.6084/m9.figshare.32968955`
- **Licencia:** CC BY 4.0
- **Contenido:** 7 archivos FASTA (~3.4 GB)

**Descarga selectiva.** Por defecto el pipeline baja solo los **dos** archivos que
el análisis necesita (aproximadamente 122 MB de 3.4 GB). Descargar el registro completo en cada
ejecución sería un proceso que demoraría más; el parámetro `--download_all` permite el
comportamiento exhaustivo cuando se requiera.

**Verificación de integridad.** La API de Figshare publica el `supplied_md5` de
cada archivo. El pipeline compara el MD5 de lo descargado contra ese valor y
**aborta** si no coincide. Hay que comprobar que es el archivo correcto.

---

## 6. Adaptaciones del Proceso 2

> Se adoptó `calculoBashTask` y `generaReporteTask` de
> [`tap_pipeline_gen`](https://github.com/DavidCastroSalinas/tap_pipeline_gen)
> (autor: David A. Castro S.), **clonar sin adaptar no se acepta**.
> Se documentan a continuación las adaptaciones realizadas.

### 6.1 Dirección de búsqueda invertida

El proceso original busca el genoma de referencia dentro de las secuencias de
consulta. Aquí se **invirtió la dirección del objetivo**: se buscaron los reads del archivo de
consulta dentro del genoma de referencia.

**Justificación.** Es lo que pide la sección 8.2(a) de la guía ("buscar secuencias
o subsecuencias dentro del genoma de referencia"), y es lo que hace el Proceso 1
con PySpark. Esto se hizo para poder realizar comparaciones, es decir, mantener las dos 
direcciones habría producido dos análisis que no se pueden comparar entre sí, y el Proceso 3 se apoya justamente en esa comparación.

### 6.2 Reimplementación de `generaReporteTask` de R a Python

El `generaReporteTask` original está escrito en **R** y lee archivos CSV
producidos por procesos (`02`, `04`) que no forman parte de este pipeline. Se
reimplementó en **Python**, conservando su objetivo de resumir la búsqueda Bash y
producir un reporte con una figura.

**Justificación.** Un proceso en R obligaría a agregar todo un entorno de R al
`environment.yml` para un único script, degradando la reproducibilidad del
proyecto; y en todo caso el original no era ejecutable tal cual, porque sus
entradas no existían, por eso se adaptó el proceso, no solo el lenguaje.

### 6.3 Otras correcciones sobre el código externo

- **Rutas absolutas eliminadas.** El original apunta a
  `/home/dabits/proyectos/tap_pipeline_gen/...`. Todos los scripts se pasan como
  canales con `Channel.fromPath("${projectDir}/scripts/...")`.
- **`grep -o -b -F` reemplazado por `awk index()`.** El genoma ocupa una sola
  línea de aproximadamente 3 millones de caracteres, donde `grep -F` devolvía cero coincidencias
  aunque existieran.
- **`awk -v ref="$genoma"` reemplazado por lectura desde archivo.** Pasar 3 MB
  como argumento provoca `Argument list too long`.
- **Cronómetro corregido.** `date +%s%3N` devuelve un valor corrupto en WSL; se
  sustituyó por `date +%s.%N` con conversión en `awk`.
- **Largo de semilla justificado posteriormente.** En la sección 7.

---

## 7. Decisiones técnicas

**Semilla de 16 bases.** Un read completo casi nunca calza exacto contra la
referencia (errores de secuenciación): 0 de 20 reads daban
resultado. Se busca una semilla del inicio del read. El largo se eligió por especificidad: las apariciones esperadas por azar son `G/4^L`, con `G` = 2.982.397 bases. Con L=10 se esperan ~2.8 coincidencias aleatorias; con **L=16 la probabilidad baja a 0.0007**, de modo que un calce es
casi con certeza real. Parametrizable con `--semilla`.

**`broadcast` del genoma en Spark.** El genoma se envía una sola vez a cada
worker en lugar de replicarse en cada tarea.

**`AdaptiveAvgPool1d` en la CNN 1D, no `AdaptiveMaxPool1d`.** La etiqueta depende
del **conteo** de bases G/C en la ventana, y el max-pooling conserva solo la
activación máxima, descartando el conteo. La primera versión con MaxPool colapsaba
a la clase mayoritaria. No fue un ajuste de hiperparámetro, fue corregir un error
conceptual de arquitectura.

**Codificación one-hot de 4 canales.** Codificar las bases como enteros 0–3
sugirió que T está "más lejos" de A que C, lo cual es biológicamente falso.

**k-meros de largo 4.** 4⁴ = 256 valores, que caben exactos en `uint8` (0–255).

**Semilla aleatoria fija (`SEED = 42`).** Sin ella cada ejecución daría números
distintos, que la reproducibilidad es el defecto más grave.

**Cero rutas absolutas.** Verificable con `grep -rn "/home/" main.nf scripts/`.

**Auto-registro del entorno.** `run_parameters.csv` captura 26 parámetros en cada
corrida (versiones de Python, Java, Spark, PyTorch, NumPy, pandas, matplotlib;
CPUs, semilla, hiperparámetros) **incluido el commit de git y un aviso si el
código tenía cambios sin confirmar**. El chequeo excluye `Results/` y `Reports/`
mediante el pathspec `:(top,exclude)`, porque esas carpetas las genera la propia
corrida y porque el proceso se ejecuta dentro del `work/` de Nextflow, donde git
interpreta los pathspec relativos al directorio actual.

---

## 8. Resultados y evidencias

Las cifras exactas de la última corrida están en
**`Reports/final_report.html`** y en `Results/Problem1/run_parameters.csv`. Los
tiempos varían aproximadamente 10% entre corridas por tratarse de mediciones sobre una máquina
compartida con el sistema operativo; por eso el benchmark promedia 3 repeticiones
y reporta la desviación estándar.

Hallazgos:

- **Regex distribuida:** 360 patrones (20 reads × 6 largos × 3 formas),
  **912.469 calces totales**. Se usa lookahead `(?=(...))` para contar
  ocurrencias solapadas.
- **Correctitud de la paralelización:** los 912.469 calces son **idénticos** en
  las 5 configuraciones del benchmark. La paralelización cambió el tiempo, no el
  resultado.
- **Escalamiento:** el speedup crece hasta aproximadamente 7x, pero la eficiencia
  (`speedup / particiones`) cae del aproximadamente 95% con 2 particiones a 43% 
  aproximadamente con 16. Spark: acelera mientras el trabajo por partición supere el
  costo de coordinarla.
- **Recurrencia:** densidad de autosimilaridad 0.2583 y cruzada 0.2476, contra
  0.2500 esperado al azar. La figura aparenta estructura, pero la medición dice
  que es ruido. Se reporta como **resultado nulo**.
- **Composición del genoma:** 2.982.397 bases, GC = 58.77% (verificado de forma
  independiente con `grep` y `awk`), sin bases `N`.
- **CNN 1D:** línea base 0.6780, **accuracy 0.9520**, precision 0.9802,
  recall 0.9484. F1 de la clase mayoritaria 0.9640; **F1 de la clase minoritaria
  0.9279 y macro-F1 0.9460**, al tener clases desbalanceadas, el número relevante es
  el de la clase difícil.
- **Búsqueda Bash:** 4 de 20 reads localizados con semilla de 16 bases.

### Nota sobre `regex_matches.csv`

La columna `position` guarda **las primeras 10 posiciones** de cada patrón; el
conteo completo está en `match_count`. Registrar las 912.469 posiciones haría el
archivo inmanejable sin aportar información adicional. La columna
`sequence_length` corresponde al **largo del patrón buscado**.

### Evidencias de reproducibilidad
---

## 9. Estructura del repositorio
---

## 10. Licencia

Código bajo licencia MIT (ver `LICENSE`). Los datos de Figshare están bajo
CC BY 4.0.

---

## 11. Declaración de uso de inteligencia artificial

Este trabajo fue desarrollado con apoyo de **Claude (Anthropic)**. El análisis
original del Problema 1 es de autoría propia. La asistencia de IA se empleó para:
depurar errores de Nextflow, Bash y Spark y revisar la redacción.
Cada sugerencia fue comprendida y verificada antes de incorporarse; varios de los
errores documentados en este README se encontraron precisamente en ese proceso de
verificación.
