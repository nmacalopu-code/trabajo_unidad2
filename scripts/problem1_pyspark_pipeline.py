#!/usr/bin/env python3
"""Problema 1 - Análisis de secuencias biológicas con PySpark."""

import argparse
import os
import re
import time

import matplotlib
matplotlib.use("Agg")   # backend sin ventana: obligatorio dentro de un pipeline
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

SEED = 42
LARGOS = [4, 8, 12, 16, 24, 32]


def leer_fasta_completo(path):
    """Lee un FASTA y devuelve UNA cadena con toda la secuencia."""
    partes = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for linea in f:
            linea = linea.strip()
            if linea and not linea.startswith(">"):
                partes.append(linea.upper())
    return "".join(partes)


def leer_fasta_reads(path, n_reads):
    """Lee unicamente los primeros n_reads del archivo y corta ahi.
    El query tiene 725.228 reads (aprox 114 MB) y solo necesito 20: no tiene
    sentido cargarlo entero en memoria."""
    reads = []
    id_actual = None
    partes = []

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue

            if linea.startswith(">"):
                if id_actual is not None:
                    reads.append((id_actual, "".join(partes).upper()))
                    if len(reads) >= n_reads:
                        return reads
                id_actual = linea[1:]
                partes = []
            else:
                partes.append(linea)

    if id_actual is not None and len(reads) < n_reads:
        reads.append((id_actual, "".join(partes).upper()))

    return reads


def generar_patrones(reads, archivo_query):
    """De cada read extrae 6 largos x 3 posiciones (inicio, centro, final)."""
    patrones = []
    for id_seq, seq in reads:
        for L in LARGOS:
            if len(seq) < L:
                continue
            centro_i = max((len(seq) - L) // 2, 0)
            for forma, patron in [
                ("inicio", seq[:L]),
                ("centro", seq[centro_i:centro_i + L]),
                ("final",  seq[-L:]),
            ]:
                patrones.append({
                    "query_id": id_seq,
                    "target_file": archivo_query,
                    "forma": forma,
                    "largo": L,
                    "pattern": patron,
                })
    return patrones


def buscar_patron(row, referencia):
    """Busca UN patron en el genoma. Corre dentro de cada worker de Spark."""
    t0 = time.perf_counter()

    # El lookahead (?=(...)) cuenta ocurrencias SOLAPADAS.
    # En "AAAA", buscar "AA" da 2 sin lookahead, pero 3 con lookahead.
    regex = f"(?=({re.escape(row['pattern'])}))"
    posiciones = [m.start() for m in re.finditer(regex, referencia)]

    t1 = time.perf_counter()

    return {
        "query_id": row["query_id"],
        "target_file": row["target_file"],
        "pattern": row["pattern"],
        "forma": row["forma"],
        "match_found": len(posiciones) > 0,
        "match_count": len(posiciones),
        "position": ";".join(str(p) for p in posiciones[:10]),
        "sequence_length": row["largo"],
        "execution_time": t1 - t0,
    }


def actividad_regex(sc, patrones, bc_ref, particiones, outdir):
    print("\n[a] Busqueda regex distribuida en Spark")

    t0 = time.perf_counter()
    rdd = sc.parallelize(patrones, particiones)
    resultados = rdd.map(lambda r: buscar_patron(r, bc_ref.value)).collect()
    t1 = time.perf_counter()

    df = pd.DataFrame(resultados)
    df["n_partitions"] = particiones

    ruta = os.path.join(outdir, "regex_matches.csv")
    df.to_csv(ruta, index=False)

    print(f"    Patrones evaluados : {len(df)}")
    print(f"    Calces totales     : {int(df['match_count'].sum())}")
    print(f"    Patrones con calce : {int(df['match_found'].sum())}")
    print(f"    Tiempo total       : {t1 - t0:.2f} s")
    print(f"    -> {ruta}")

    return df


def actividad_benchmark(sc, patrones, bc_ref, lista_particiones, repeticiones, outdir):
    print("\n[b] Benchmark de paralelizacion")

    filas = []
    for n_part in lista_particiones:
        tiempos = []
        for _ in range(repeticiones):
            rdd = sc.parallelize(patrones, n_part)
            t0 = time.perf_counter()
            salida = rdd.map(lambda r: buscar_patron(r, bc_ref.value)).collect()
            t1 = time.perf_counter()
            tiempos.append(t1 - t0)

        total_calces = sum(s["match_count"] for s in salida)
        media = float(np.mean(tiempos))

        filas.append({
            "partitions": n_part,
            "n_patterns": len(patrones),
            "repeats": repeticiones,
            "mean_time_s": media,
            "std_time_s": float(np.std(tiempos)),
            "time_per_pattern_s": media / len(patrones),
            "total_matches": total_calces,
            "matches_per_second": total_calces / media,
        })
        print(f"    particiones={n_part:2d}  tiempo={media:6.2f}s  +/-{np.std(tiempos):.2f}  calces={total_calces}")

    df = pd.DataFrame(filas)

    # speedup = tiempo_base / tiempo_actual. La base es la configuración con menos particiones.
    t_base = df.loc[df["partitions"].idxmin(), "mean_time_s"]
    df["speedup"] = t_base / df["mean_time_s"]

    ruta = os.path.join(outdir, "benchmark_parallelization.csv")
    df.to_csv(ruta, index=False)

    # Prueba de correctitud: si paralelizar cambiara el resultado, seria un bug.
    identicos = df["total_matches"].nunique() == 1
    print(f"    Calces identicos en todas las configuraciones: {identicos}")

    mejor = df.loc[df["speedup"].idxmax()]
    print(f"    Mejor speedup: {mejor['speedup']:.2f}x con {int(mejor['partitions'])} particiones")

    graficar_benchmark(df, outdir)
    return df


def graficar_benchmark(df, outdir):
    figdir = os.path.join(outdir, "Figures")
    os.makedirs(figdir, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

    ax1.errorbar(df["partitions"], df["mean_time_s"], yerr=df["std_time_s"],
                 marker="o", capsize=4, linewidth=2)
    ax1.set_xlabel("Numero de particiones")
    ax1.set_ylabel("Tiempo de ejecucion [s]")
    ax1.set_title("Tiempo segun numero de particiones")
    ax1.grid(True, alpha=0.3)

    ax2.plot(df["partitions"], df["speedup"], marker="o", linewidth=2,
             label="Speedup obtenido")
    ax2.plot(df["partitions"], df["partitions"], "--", alpha=0.5,
             label="Speedup ideal (lineal)")
    ax2.axhline(1.0, color="gray", linestyle=":", alpha=0.7,
                label="Sin ganancia (1x)")
    ax2.set_xlabel("Numero de particiones")
    ax2.set_ylabel("Speedup")
    ax2.set_title("Speedup relativo a la ejecucion base")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    ruta = os.path.join(figdir, "benchmark_parallelization.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"    -> {ruta}")


def main():
    p = argparse.ArgumentParser(description="Problema 1 con PySpark")
    p.add_argument("--reference",  required=True)
    p.add_argument("--query",      required=True)
    p.add_argument("--outdir",     required=True)
    p.add_argument("--partitions", type=int, default=4)
    p.add_argument("--n_reads",    type=int, default=20)
    p.add_argument("--benchmark_partitions", default="1,2,4,8,16",
                   help="lista separada por comas, ej: 1,2,4,8,16")
    p.add_argument("--repeats", type=int, default=3,
                   help="repeticiones por configuración, para promediar")
    args = p.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Problema1_Bioinformatica_PySpark")
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )
    sc = spark.sparkContext
    print("Spark version :", spark.version)

    genoma_ref = leer_fasta_completo(args.reference)
    print("Genoma        :", len(genoma_ref), "bases")

    reads = leer_fasta_reads(args.query, args.n_reads)
    print("Reads leidos  :", len(reads))

    patrones = generar_patrones(reads, os.path.basename(args.query))
    print("Patrones      :", len(patrones))

    # broadcast: envia el genoma UNA vez a cada worker.
    # Sin esto Spark serializaria los ~3 MB en CADA una de las 360 tareas.
    bc_ref = sc.broadcast(genoma_ref)

    actividad_regex(sc, patrones, bc_ref, args.partitions, args.outdir)

    lista_particiones = [int(x) for x in args.benchmark_partitions.split(",")]
    actividad_benchmark(sc, patrones, bc_ref, lista_particiones, args.repeats, args.outdir)

    spark.stop()
    print("\nListo.")


if __name__ == "__main__":
    main()
