#!/usr/bin/env python3
"""Problema 1 - Análisis de secuencias biológicas con PySpark."""

import argparse
import os
import re
import time

import matplotlib
matplotlib.use("Agg")   
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

SEED = 42
LARGOS = [4, 8, 12, 16, 24, 32]
BASE_MAP = {"A": 0, "C": 1, "G": 2, "T": 3}
K = 4   # largo del k-mero: 4^4 = 256 valores

def leer_fasta_completo(path):
    """Lee un fasta  y devuelve una cadena con toda la secuencia."""
    partes = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for linea in f:
            linea = linea.strip()
            if linea and not linea.startswith(">"):
                partes.append(linea.upper())
    return "".join(partes)


def leer_fasta_reads(path, n_reads):
    """Lee solo  los primeros n_reads del archivo y se detiene ahí"""
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
    """De cada read extrae 6 largos por  3 posiciones (inicio, centro, final)."""
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
    """Busca un  patrón en el genoma, corre dentro de cada worker de Spark."""
    t0 = time.perf_counter()

    # El lookahead (?=(...)) cuenta ocurrencias, como que haya solapaciones.
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

    print(f" Patrones evaluados : {len(df)}")
    print(f" Calces totales     : {int(df['match_count'].sum())}")
    print(f" Patrones con calce : {int(df['match_found'].sum())}")
    print(f" Tiempo total       : {t1 - t0:.2f} s")
    print(f" {ruta}")

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

    # Prueba de corrección, es decir si paralelizar cambiara el resultado, seria un bug.
    identicos = df["total_matches"].nunique() == 1
    print(f"    Calces idénticos en todas las configuraciones: {identicos}")

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
    ax1.set_title("Tiempo según número de particiones")
    ax1.grid(True, alpha=0.3)

    ax2.plot(df["partitions"], df["speedup"], marker="o", linewidth=2,
             label="Speedup obtenido")
    ax2.plot(df["partitions"], df["partitions"], "--", alpha=0.5,
             label="Speedup ideal (lineal)")
    ax2.axhline(1.0, color="gray", linestyle=":", alpha=0.7,
                label="Sin ganancia (1x)")
    ax2.set_xlabel("Número de particiones")
    ax2.set_ylabel("Speedup")
    ax2.set_title("Speedup relativo a la ejecución base")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    ruta = os.path.join(figdir, "benchmark_parallelization.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  {ruta}")


def codificar_secuencia(seq):
    """Convierte bases en números, se descantan bases ambiguas (N)."""
    return np.array([BASE_MAP[b] for b in seq.upper() if b in BASE_MAP],
                    dtype=np.int8)


def actividad_recurrencia(genoma_ref, reads, outdir, n_bases=500):
    print("\n[c] Gráficos de recurrencia")

    figdir = os.path.join(outdir, "Figures")
    os.makedirs(figdir, exist_ok=True)

    # Autosimilaridad, es decir la referencia consigo misma.
    # R[i,j] = True si la base i es igual a la base j.
    frag_ref = codificar_secuencia(genoma_ref[:n_bases])
    R_auto = (frag_ref.reshape(-1, 1) == frag_ref.reshape(1, -1))

    # Recurrencia cruzada: un read contra la referencia.
    id_read, seq_read = reads[0]
    frag_read = codificar_secuencia(seq_read[:min(len(seq_read), n_bases)])
    R_cross = (frag_read.reshape(-1, 1) == frag_ref.reshape(1, -1))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5))

    ax1.imshow(R_auto, cmap="binary", origin="lower", interpolation="nearest")
    ax1.set_title(f"Autosimilaridad: genoma de referencia\n(primeras {n_bases} bases)")
    ax1.set_xlabel("Posición en la referencia")
    ax1.set_ylabel("Posición en la referencia")

    ax2.imshow(R_cross, cmap="binary", origin="lower", aspect="auto",
               interpolation="nearest")
    ax2.set_title("Recurrencia cruzada: read vs referencia")
    ax2.set_xlabel("Posición en la referencia")
    ax2.set_ylabel("Posición en el read")

    plt.tight_layout()
    ruta = os.path.join(figdir, "recurrence_plot.png")
    plt.savefig(ruta, dpi=150)
    plt.close()

    # La densidad es la fracción de celdas en True. Con 4 bases equiprobables el azar
    # da 1/4 = 0.25. Comparar con ese valor  nos diría si hay hay estructura real.
    dens_auto = float(R_auto.mean())
    dens_cross = float(R_cross.mean())

    print(f" Densidad autosimilaridad: {dens_auto:.4f}")
    print(f" Densidad cruzada : {dens_cross:.4f}")
    print(f" Densidad esperada al azar: 0.2500")
    print(f" {ruta}")

    return dens_auto, dens_cross

def codificar_4mer(kmer):
    """Lo que hace es agrupar 4 bases en un  entero de 8 bits.
    4 bases x 2 bits = 8 bits = 0..255."""
    valor = 0
    for b in kmer:
        valor = valor * 4 + BASE_MAP[b]
    return valor


def codificar_kmers(seq):
    """Esto devuelve posición, k-mero y código construidos.
    Los tres juntos con el fin de que  si un genoma trae bases ambiguas (N), esos
    k-meros se descartan. Si se reconstruyeran las posiciones aparte, quedarian
    corridas respecto de los códigos y el CSV indicaría que tiene un código que no le corresponde.
    """
    seq = seq.upper()
    posiciones, kmers, codigos = [], [], []

    for i in range(len(seq) - K + 1):
        kmer = seq[i:i + K]
        if all(b in BASE_MAP for b in kmer):
            posiciones.append(i)
            kmers.append(kmer)
            codigos.append(codificar_4mer(kmer))

    return posiciones, kmers, np.array(codigos, dtype=np.uint8)


def actividad_codificacion_conv(genoma_ref, outdir, n_bases=2000):
    print("\n[d] Codificación 8-bit y convolucion 1D")

    figdir = os.path.join(outdir, "Figures")
    os.makedirs(figdir, exist_ok=True)

    posiciones, kmers, serie = codificar_kmers(genoma_ref[:n_bases])

    df_enc = pd.DataFrame({
        "position": posiciones,
        "kmer": kmers,
        "code_8bit": serie.astype(int),
    })
    df_enc.to_csv(os.path.join(outdir, "encoded_sequences.csv"), index=False)

    print(f" Códigos generados : {len(serie)}")
    print(f" Rango             : {serie.min()} .. {serie.max()}")
    print(f" Códigos distintos : {len(np.unique(serie))} de 256 posibles")

    # Kernel alternante, esto por si códigos vecinos se alternan,
    # y cerca de 0 cuando la señal es plana, funciona como un detector de cambio.
    kernel = np.array([1, -1, 1, -1], dtype=float)
    conv = np.convolve(serie.astype(float), kernel, mode="valid")

    df_conv = pd.DataFrame({"index": np.arange(len(conv)), "conv_value": conv})
    df_conv.to_csv(os.path.join(outdir, "conv1d_timeseries.csv"), index=False)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7))

    ax1.plot(serie[:300], linewidth=1)
    ax1.set_title("Serie codificada en 8 bits (k-meros de largo 4)")
    ax1.set_xlabel("Posición en el genoma")
    ax1.set_ylabel("Codigo (0-255)")
    ax1.grid(True, alpha=0.3)

    ax2.plot(conv[:300], linewidth=1, color="darkorange")
    ax2.axhline(0, color="gray", linestyle=":", alpha=0.7)
    ax2.set_title("Serie de tiempo tras convolución 1D con kernel [1,-1,1,-1]")
    ax2.set_xlabel("Posición en el genoma")
    ax2.set_ylabel("Respuesta de la convolución")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    ruta = os.path.join(figdir, "conv1d_signal.png")
    plt.savefig(ruta, dpi=150)
    plt.close()

    print(f" Convolucion: media={conv.mean():.2f}  std={conv.std():.2f}")
    print(f" {ruta}")

    return df_enc, df_conv


def main():
    p = argparse.ArgumentParser(description="Problema 1 con PySpark")
    p.add_argument("--reference",  required=True)
    p.add_argument("--query",      required=True)
    p.add_argument("--outdir",     required=True)
    p.add_argument("--partitions", type=int, default=4)
    p.add_argument("--n_reads",    type=int, default=20)
    p.add_argument("--benchmark_partitions", default="1,2,4,8,16",
                   help="lista separada por comas, por ejemplo  1,2,4,8,16")
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

    # broadcast,  envía el genoma una vez a cada worker.
    # Sin esto Spark serializaria los aprox 3 MB en cada una de las 360 tareas.
    bc_ref = sc.broadcast(genoma_ref)

    actividad_regex(sc, patrones, bc_ref, args.partitions, args.outdir)

    lista_particiones = [int(x) for x in args.benchmark_partitions.split(",")]
    actividad_benchmark(sc, patrones, bc_ref, lista_particiones, args.repeats, args.outdir)
    
    actividad_recurrencia(genoma_ref, reads, args.outdir)
    actividad_codificacion_conv(genoma_ref, args.outdir)

    spark.stop()
    print("\nListo.")


if __name__ == "__main__":
    main()
