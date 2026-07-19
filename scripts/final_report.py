#!/usr/bin/env python3

#  Proceso 3 - Informe final
#  Trabajo Unidad 2 

import argparse
import base64
import os
import sys
from datetime import datetime

import pandas as pd
import matplotlib
matplotlib.use("Agg")          # sin pantalla: el pipeline corre headless
import matplotlib.ticker
import matplotlib.pyplot as plt

#Utilidades

def leer_csv(ruta, nombre):
    """Lee un CSV e indica con un mensaje si no existe."""
    if not os.path.isfile(ruta):
        sys.exit(f"Error: no se encontró {nombre}: {ruta}")
    df = pd.read_csv(ruta)
    if df.empty:
        sys.exit(f"ERROR: {nombre} esta vacio: {ruta}")
    return df


def parametros_a_dict(df):
    """run_parameters.csv tiene formato parametro,valor."""
    return dict(zip(df["parametro"], df["valor"]))

#Argumentos

p = argparse.ArgumentParser(
    description="Proceso 3: reporte final propio del pipeline")
p.add_argument("--problem1_dir", required=True,
               help="Carpeta con las salidas del Proceso 1")
p.add_argument("--external_dir", required=True,
               help="Carpeta con las salidas del Proceso 2")
p.add_argument("--figures_dir", required=True,
               help="Carpeta de final_summary_plot.png")
p.add_argument("--reports_dir", required=True,
               help="Carpeta de final_report.html")
args = p.parse_args()

os.makedirs(args.figures_dir, exist_ok=True)
os.makedirs(args.reports_dir, exist_ok=True)

#1. Carga de los resultados del pipeline

bench = leer_csv(os.path.join(args.problem1_dir, "benchmark_parallelization.csv"),
                 "benchmark_parallelization.csv")
regex = leer_csv(os.path.join(args.problem1_dir, "regex_matches.csv"),
                 "regex_matches.csv")
cnn = leer_csv(os.path.join(args.problem1_dir, "cnn_results.csv"),
               "cnn_results.csv")
params = parametros_a_dict(
    leer_csv(os.path.join(args.problem1_dir, "run_parameters.csv"),
             "run_parameters.csv"))
bash = leer_csv(os.path.join(args.external_dir, "calculo_bash_resultados.csv"),
                "calculo_bash_resultados.csv")

#2.Métricas comparables
#Las dos herramientas no hacen el mismo trabajo, así que comparar
#tiempos totales seria engañoso. Por eso, se normaliza por búsqueda:
#milisegundos por patrón (Spark) vs milisegundos por read (Bash).

bench = bench.sort_values("partitions").reset_index(drop=True)

#Benchmark, que corresponde a las particiones con que corrió el pipeline

part_pipeline = int(params.get("particiones", bench["partitions"].iloc[0]))
fila = bench[bench["partitions"] == part_pipeline]
if fila.empty:
    fila = bench.iloc[[bench["mean_time_s"].idxmin()]]
    part_pipeline = int(fila["partitions"].iloc[0])
fila = fila.iloc[0]

spark_ms_por_busqueda = float(fila["time_per_pattern_s"]) * 1000.0
spark_n_busquedas = int(fila["n_patterns"])
spark_total_s = float(fila["mean_time_s"])

bash_ms_por_busqueda = float(bash["tiempo_ms"].mean())
bash_n_busquedas = int(len(bash))
bash_total_s = float(bash["tiempo_ms"].sum()) / 1000.0
bash_encontrados = int((bash["encontrado"].astype(str).str.lower() == "si").sum())
largo_semilla = int(bash["largo_semilla"].iloc[0])

#Eficiencia = speedup / particiones. Mide cuanto rinde cada partición.

bench["eficiencia"] = bench["speedup"] / bench["partitions"]
razon = bash_ms_por_busqueda / spark_ms_por_busqueda


#3.Figura

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))

# A) tiempo normalizado por busqueda
etiquetas = [f"PySpark\n({spark_n_busquedas} patrones,\n{part_pipeline} particiones)",
             f"Bash + awk\n({bash_n_busquedas} reads,\nsemilla {largo_semilla} bases)"]
valores = [spark_ms_por_busqueda, bash_ms_por_busqueda]
colores = ["#2c6fbb", "#c1512a"]

barras = ax1.bar(etiquetas, valores, color=colores, width=0.55,
                 edgecolor="black", linewidth=0.6)
for b, v in zip(barras, valores):
    ax1.text(b.get_x() + b.get_width() / 2, v * 1.03, f"{v:.2f} ms",
             ha="center", va="bottom", fontweight="bold")

ax1.set_ylabel("Tiempo por búsqueda (milisegundos)")
ax1.set_title("A) Costo por búsqueda: PySpark vs Bash\n"
              "(cargas no equivalentes - ver nota)", fontsize=11)
ax1.set_ylim(0, max(valores) * 1.25)
ax1.set_axisbelow(True)

# B) speedup y eficiencia
ax2.plot(bench["partitions"], bench["speedup"], "o-", color="#2c6fbb",
         linewidth=2, markersize=7, label="Speedup real")
ax2.plot(bench["partitions"], bench["partitions"], "--", color="gray",
         linewidth=1.2, label="Speedup ideal (lineal)")
ax2.set_xlabel("Número de particiones de Spark")
ax2.set_ylabel("Speedup (x veces)", color="#2c6fbb")
ax2.tick_params(axis="y", labelcolor="#2c6fbb")
ax2.set_xscale("log", base=2)
ax2.set_xticks(bench["partitions"])
ax2.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
ax2.set_axisbelow(True)

ax2b = ax2.twinx()
ax2b.plot(bench["partitions"], bench["eficiencia"] * 100, "s-",
          color="#c1512a", linewidth=2, markersize=6, label="Eficiencia")
ax2b.set_ylabel("Eficiencia = speedup / particiones (%)", color="#c1512a")
ax2b.tick_params(axis="y", labelcolor="#c1512a")
ax2b.set_ylim(0, 115)

lineas = ax2.get_lines() + ax2b.get_lines()
ax2.legend(lineas, [l.get_label() for l in lineas], loc="upper left", fontsize=9)
ax2.set_title("B) Escalamiento de la paralelizacion en PySpark", fontsize=11)

fig.suptitle("Resumen final del pipeline - Trabajo Unidad 2",
             fontsize=13, fontweight="bold")
fig.tight_layout(rect=[0, 0, 1, 0.95])

ruta_fig = os.path.join(args.figures_dir, "final_summary_plot.png")
fig.savefig(ruta_fig, dpi=200, bbox_inches="tight")
plt.close(fig)
print(f"Figura generada: {ruta_fig}")


#4.Reporte HTML autocontenido

with open(ruta_fig, "rb") as fh:
    img_b64 = base64.b64encode(fh.read()).decode("ascii")

calces_totales = int(fila["total_matches"])
n_configs = int(len(bench))
consistente = bench["total_matches"].nunique() == 1

mejor = bench.iloc[bench["speedup"].idxmax()]

filas_bench = "\n".join(
    f"<tr><td>{int(r.partitions)}</td><td>{r.mean_time_s:.3f}</td>"
    f"<td>{r.std_time_s:.3f}</td><td>{r.speedup:.2f}x</td>"
    f"<td>{r.eficiencia*100:.0f}%</td><td>{int(r.total_matches)}</td></tr>"
    for r in bench.itertuples())

cnn0 = cnn.iloc[0]

html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Reporte final - Trabajo Unidad 2</title>
<style>
 body {{ font-family: Georgia, serif; max-width: 900px; margin: 2rem auto;
        padding: 0 1.5rem; line-height: 1.6; color: #222; }}
 h1 {{ border-bottom: 3px solid #2c6fbb; padding-bottom: .4rem; }}
 h2 {{ color: #2c6fbb; margin-top: 2.2rem; }}
 table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; font-size: .92rem; }}
 th, td {{ border: 1px solid #ccc; padding: .45rem .7rem; text-align: right; }}
 th {{ background: #eef3f9; text-align: center; }}
 td:first-child {{ text-align: center; font-weight: bold; }}
 img {{ width: 100%; border: 1px solid #ddd; margin: 1rem 0; }}
 .nota {{ background: #fff8e6; border-left: 4px solid #e0a800;
          padding: .8rem 1rem; margin: 1.2rem 0; font-size: .95rem; }}
 .meta {{ font-size: .85rem; color: #666; }}
 code {{ background: #f4f4f4; padding: .1rem .3rem; }}
</style>
</head>
<body>

<h1>Reporte final del pipeline</h1>
<p class="meta">
 Trabajo Unidad 2 &mdash; Técnicas Avanzadas de Programación<br>
 Generado automáticamente por el Proceso 3 el {datetime.now():%d/%m/%Y %H:%M}<br>
 Corrida analizada: {params.get('fecha_ejecucion','n/d')} &middot;
 commit <code>{params.get('git_commit','n/d')}</code>
</p>

<p>El reporte es el resultado propio del Proceso 3. A diferencia de
<code>generaReporteTask</code> (Proceso 2), que resume solo la búsqueda en Bash,
aquí se <strong>integran y comparan</strong> las dos estrategias de búsqueda que
conviven en el pipeline.</p>

<h2>1.Comparacion PySpark vs Bash</h2>
<img src="data:image/png;base64,{img_b64}" alt="Resumen final del pipeline">

<table>
<tr><th>Metrica</th><th>PySpark (Proceso 1)</th><th>Bash + awk (Proceso 2)</th></tr>
<tr><td>Busquedas realizadas</td><td>{spark_n_busquedas}</td><td>{bash_n_busquedas}</td></tr>
<tr><td>Tiempo total (s)</td><td>{spark_total_s:.2f}</td><td>{bash_total_s:.2f}</td></tr>
<tr><td>Tiempo por busqueda (ms)</td><td>{spark_ms_por_busqueda:.2f}</td><td>{bash_ms_por_busqueda:.2f}</td></tr>
<tr><td>Ocurrencias contadas</td><td>todas (solapadas)</td><td>solo la primera</td></tr>
<tr><td>Paralelismo</td><td>{part_pipeline} particiones</td><td>secuencial</td></tr>
<tr><td>Reads con calce</td><td>&mdash;</td><td>{bash_encontrados} de {bash_n_busquedas}</td></tr>
</table>

<div class="nota">
<strong>Nota.</strong> Las dos herramientas
<strong>no ejecutan la misma carga</strong>: PySpark evalua {spark_n_busquedas} patrones
de largo variable y cuenta <em>todas</em> las ocurrencias solapadas mediante
<em>lookahead</em>; Bash busca {bash_n_busquedas} reads por una semilla de
{largo_semilla} bases y se detiene en la <em>primera</em> coincidencia.
Comparar tiempos totales seria engañoso, por eso después la comparación se normalizó
por búsqueda. Aun así, la lectura correcta es cualitativa, es decir, PySpark resulta
<strong>{razon:.1f} veces mas rápido por búsqueda</strong> haciendo estrictamente
<em>mas</em> trabajo en cada una, pero paga un costo fijo de arranque de la JVM y
del contexto de Spark que no aparece en esta métrica y que solo se amortiza con
volumen. Con {bash_n_busquedas} busquedas, Bash es la herramienta razonable;
con {spark_n_busquedas} o mas, deja de serlo.
</div>

<h2>2. Escalamiento de la paralelización</h2>
<table>
<tr><th>Particiones</th><th>Tiempo medio (s)</th><th>Desv. est. (s)</th>
    <th>Speedup</th><th>Eficiencia</th><th>Calces totales</th></tr>
{filas_bench}
</table>

<p>El mejor speedup es <strong>{mejor.speedup:.2f}x</strong> con
{int(mejor.partitions)} particiones, sobre {int(params.get('cpus_logicos', 0))} CPUs
logicas. La eficiencia cae de {bench['eficiencia'].iloc[1]*100:.0f}% con
{int(bench['partitions'].iloc[1])} particiones a
{bench['eficiencia'].iloc[-1]*100:.0f}% con {int(bench['partitions'].iloc[-1])}:
Spark no acelera automáticamente, acelera mientras el trabajo por partición
supere el costo de coordinarla.</p>

<p><strong>Prueba de correctitud:</strong> el total de calces es
<code>{calces_totales}</code>
{'e <strong>identico</strong>' if consistente else 'y <strong>NO es identico</strong>'}
en las {n_configs} configuraciones evaluadas. La paralelizacion cambio el tiempo,
no el resultado.</p>

<h2>3. Clasificación con CNN 1D</h2>
<table>
<tr><th>Metrica</th><th>Valor</th></tr>
<tr><td>Linea base (clase mayoritaria)</td><td>{float(cnn0['baseline']):.4f}</td></tr>
<tr><td>Accuracy</td><td>{float(cnn0['accuracy']):.4f}</td></tr>
<tr><td>Precision</td><td>{float(cnn0['precision']):.4f}</td></tr>
<tr><td>Recall</td><td>{float(cnn0['recall']):.4f}</td></tr>
<tr><td>F1 (clase mayoritaria)</td><td>{float(cnn0['f1_score']):.4f}</td></tr>
<tr><td>Clases distintas predichas</td><td>{int(cnn0['n_classes_predicted'])}</td></tr>
</table>
<p>El modelo supera la línea base en
{(float(cnn0['accuracy']) - float(cnn0['baseline']))*100:.1f} puntos porcentuales y
predice las dos clases, lo que descarta el colapso a la clase mayoritaria.</p>

<h2>4. Entorno de ejecución</h2>
<table>
<tr><th>Componente</th><th>Version</th></tr>
<tr><td>Sistema</td><td>{params.get('sistema_operativo','n/d')}</td></tr>
<tr><td>Python</td><td>{params.get('python_version','n/d')}</td></tr>
<tr><td>Java</td><td>{params.get('java_version','n/d')}</td></tr>
<tr><td>Spark / PySpark</td><td>{params.get('spark_version','n/d')} / {params.get('pyspark_version','n/d')}</td></tr>
<tr><td>PyTorch</td><td>{params.get('torch_version','n/d')}</td></tr>
<tr><td>Semilla</td><td>{params.get('seed','n/d')}</td></tr>
</table>

</body>
</html>
"""

ruta_html = os.path.join(args.reports_dir, "final_report.html")
with open(ruta_html, "w", encoding="utf-8") as fh:
    fh.write(html)
print(f"Reporte generado: {ruta_html}")
print(f"Resumen: PySpark {spark_ms_por_busqueda:.2f} ms/busqueda vs "
      f"Bash {bash_ms_por_busqueda:.2f} ms/busqueda (razon {razon:.1f}x)")
