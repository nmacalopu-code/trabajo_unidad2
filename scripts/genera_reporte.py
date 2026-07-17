#!/usr/bin/env python3
"""genera_reporte.py - Proceso 2, adaptado de generaReporteTask

El archivo original, generaReporteTask del repo tap_pipeline_gen (en R Markdown).
Se adaptó reescrito en Python, usando como entrada
el CSV real que produce el calculoBashTask adaptado (calculo_bash_resultados.csv),
en vez de los CSV de procesos que no se integraron.

Se generó unas figuras con los resultados de la búsqueda en Bash y el reporte en HTML.
Se usó python3 genera_reporte.py <csv_entrada> <dir_salida>
"""

import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def main():
    if len(sys.argv) < 3:
        sys.exit("Se usó python3 genera_reporte.py <csv_entrada> <dir_salida>")

    csv_entrada = sys.argv[1]
    dir_salida = sys.argv[2]
    os.makedirs(dir_salida, exist_ok=True)
    figdir = os.path.join(dir_salida, "Figures")
    os.makedirs(figdir, exist_ok=True)

    df = pd.read_csv(csv_entrada)
    print(f"Reporte: leidas {len(df)} filas de {csv_entrada}")

    n_total = len(df)
    n_si = int((df["encontrado"] == "si").sum())
    n_no = int((df["encontrado"] == "no").sum())
    tiempo_medio = df["tiempo_ms"].mean()
    tiempo_total = df["tiempo_ms"].sum()
    largo_semilla = int(df["largo_semilla"].iloc[0]) if "largo_semilla" in df.columns else "?"

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    ax1.bar(["Encontrados", "No encontrados"], [n_si, n_no],
            color=["#2a9d8f", "#e76f51"])
    ax1.set_title(f"Reads localizados en el genoma (semilla={largo_semilla})")
    ax1.set_ylabel("Numero de reads")
    for i, v in enumerate([n_si, n_no]):
        ax1.text(i, v, str(v), ha="center", va="bottom", fontweight="bold")

    ax2.bar(range(len(df)), df["tiempo_ms"], color="#264653")
    ax2.axhline(tiempo_medio, color="#e9c46a", linestyle="--",
                label=f"Media: {tiempo_medio:.1f} ms")
    ax2.set_title("Tiempo de búsqueda por read (Bash)")
    ax2.set_xlabel("Read (índice)")
    ax2.set_ylabel("Tiempo [ms]")
    ax2.legend()

    plt.tight_layout()
    ruta_fig = os.path.join(figdir, "reporte_bash.png")
    plt.savefig(ruta_fig, dpi=150)
    plt.close()

    html = f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<title>Reporte del Proceso 2 - Búsqueda en Bash</title>
<style>
  body {{ font-family: sans-serif; margin: 40px; color: #264653; }}
  h1 {{ border-bottom: 3px solid #2a9d8f; padding-bottom: 8px; }}
  table {{ border-collapse: collapse; margin: 20px 0; }}
  td, th {{ border: 1px solid #ccc; padding: 8px 16px; text-align: left; }}
  th {{ background: #2a9d8f; color: white; }}
  img {{ max-width: 100%; margin-top: 20px; }}
</style></head><body>
<h1>Reporte del proceso 2 - Búsqueda de reads en Bash</h1>
<p>Adaptación del <code>generaReporteTask</code> del repositorio
<code>tap_pipeline_gen</code>, reescrito en Python.
A continuacion se resumen los resultados del <code>calculoBashTask</code> que fue adaptado.</p>
<h2>Resumen</h2>
<table>
  <tr><th>Metrica</th><th>Valor</th></tr>
  <tr><td>Reads procesados</td><td>{n_total}</td></tr>
  <tr><td>Encontrados en el genoma</td><td>{n_si}</td></tr>
  <tr><td>No encontrados</td><td>{n_no}</td></tr>
  <tr><td>Largo de semilla</td><td>{largo_semilla} bases</td></tr>
  <tr><td>Tiempo medio por búsqueda</td><td>{tiempo_medio:.2f} ms</td></tr>
  <tr><td>Tiempo total</td><td>{tiempo_total:.0f} ms</td></tr>
</table>
<h2>Visualización</h2>
<img src="Figures/reporte_bash.png" alt="Resultados de la búsqueda en Bash">
</body></html>"""

    ruta_html = os.path.join(dir_salida, "reporte_bash.html")
    with open(ruta_html, "w", encoding="utf-8") as f:
        f.write(html)

    print(f" Encontrados: {n_si} / {n_total}")
    print(f" Tiempo medio: {tiempo_medio:.2f} ms")
    print(f" {ruta_fig}")
    print(f" {ruta_html}")


if __name__ == "__main__":
    main()
