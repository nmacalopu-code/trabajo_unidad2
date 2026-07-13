#!/usr/bin/env python3
"""Problema 1 - Análisis de secuencias biológicas con PySpark"""

import argparse
import os
import re
import time

import pandas as pd
from pyspark.sql import SparkSession

SEED = 42
LARGOS = [4, 8, 12, 16, 24, 32]


def leer_fasta_completo(path):
    """Lee un fasta y devuelve una cadena con toda la secuencia"""
    partes = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for linea in f:
            linea = linea.strip()
            if linea and not linea.startswith(">"):
                partes.append(linea.upper())
    return "".join(partes)


def leer_fasta_reads(path, n_reads):
    """Lee solo los primeros n_reads."""
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
    """De cada read extrae 6 largos por  3 posiciones (inicio, centro, final)"""
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
    """Busca un patrón en el genoma"""
    t0 = time.perf_counter()

    # El lookahead (?=(...)) cuenta ocurrencias, como la solapación de patrones de genomas
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


def actividad_regex(sc, patrones, genoma_ref, particiones, outdir):
    print("\n[a] Busqueda regex distribuida en spark")

    # broadcast, envia el genoma una vez a cada worker.
    # Sin esto spark serializaría y el proceso tardaría más
    bc_ref = sc.broadcast(genoma_ref)

    t0 = time.perf_counter()
    rdd = sc.parallelize(patrones, particiones)
    resultados = rdd.map(lambda r: buscar_patron(r, bc_ref.value)).collect()
    t1 = time.perf_counter()

    df = pd.DataFrame(resultados)
    df["n_partitions"] = particiones

    ruta = os.path.join(outdir, "regex_matches.csv")
    df.to_csv(ruta, index=False)

    print(f" Patrones evaluados: {len(df)}")
    print(f" Calces totales: {int(df['match_count'].sum())}")
    print(f" Patrones con calce: {int(df['match_found'].sum())}")
    print(f" Tiempo total: {t1 - t0:.2f} s")
    print(f" {ruta}")

    return df


def main():
    p = argparse.ArgumentParser(description="Problema 1 con PySpark")
    p.add_argument("reference", required=True)
    p.add_argument("query", required=True)
    p.add_argument("outdir", required=True)
    p.add_argument("partitions", type=int, default=4)
    p.add_argument("n_reads", type=int, default=20)
    args = p.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Problema1_Bioinformatica_PySpark")
        .getOrCreate()
    )
    sc = spark.sparkContext
    print("Spark version :", spark.version)

    genoma_ref = leer_fasta_completo(args.reference)
    print("Genoma:", len(genoma_ref), "bases")

    reads = leer_fasta_reads(args.query, args.n_reads)
    print("Reads leidos:", len(reads))

    patrones = generar_patrones(reads, os.path.basename(args.query))
    print("Patrones:", len(patrones))

    actividad_regex(sc, patrones, genoma_ref, args.partitions, args.outdir)

    spark.stop()
    print("\nListo.")


if __name__ == "__main__":
    main()
