#!/usr/bin/env python3
"""Problema 1: Análisis de secuencias biológicas con PySpark"""

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

import torch
import torch.nn as nn
import torch.optim as optim

import platform
import subprocess
import sys


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
    # R[i,j], True si la base i es igual a la base j.
    frag_ref = codificar_secuencia(genoma_ref[:n_bases])
    R_auto = (frag_ref.reshape(-1, 1) == frag_ref.reshape(1, -1))

    # Recurrencia cruzada, es decir  un read contra la referencia.
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



class CNN1D(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv1d(in_channels=4, out_channels=8, kernel_size=5)
        self.relu  = nn.ReLU()
        # AdaptiveAvgPool1d, la etiqueta depende de cuantas bases G/C hay en la ventana, o sea de un conteo. El máximo conserva solo la
        # activación mas alta y descarta el conteo; el promedio si lo mantiene.
        self.pool  = nn.AdaptiveAvgPool1d(1)
        self.fc    = nn.Linear(8, 2)

    def forward(self, x):
        x = self.relu(self.conv1(x))
        x = self.pool(x).squeeze(-1)
        return self.fc(x)


def one_hot(X_int):
    """Convierte (n, L) de enteros a (n, 4, L) one-hot.
    Al ser las bases categorías, al momento de codificarlas 0,1,2,3 le sugeriría
    a la red que T esta más lejos de A que C, lo cual es biológicamente falso."""
    n, L = X_int.shape
    out = np.zeros((n, 4, L), dtype=np.float32)
    for base in range(4):
        out[:, base, :] = (X_int == base)
    return out


def crear_dataset_cnn(genoma_ref, window_size, n_samples):
    """Ventanas aleatorias del genoma, etiquetadas por su contenido GC."""
    seq_num = codificar_secuencia(genoma_ref)
    rng = np.random.default_rng(SEED)
    max_start = len(seq_num) - window_size

    X, y = [], []
    for _ in range(n_samples):
        i = int(rng.integers(0, max_start))
        v = seq_num[i:i + window_size]
        # Con BASE_MAP {A:0, C:1, G:2, T:3}, las bases G y C son los códigos 1 y 2.
        gc = float(np.mean((v == 1) | (v == 2)))
        X.append(v)
        y.append(1 if gc > 0.55 else 0)

    return np.array(X, dtype=np.int64), np.array(y, dtype=np.int64)


def actividad_cnn(genoma_ref, outdir, epochs, n_samples, window=64):
    print("\n[e] Búsqueda jerárquica con CNN 1D")

    figdir = os.path.join(outdir, "Figures")
    os.makedirs(figdir, exist_ok=True)
    torch.manual_seed(SEED)

    X, y = crear_dataset_cnn(genoma_ref, window, n_samples)
    n0, n1 = int((y == 0).sum()), int((y == 1).sum())
    print(f" Clase 0 (GC bajo) : {n0}")
    print(f" Clase 1 (GC alto) : {n1}")

    rng = np.random.default_rng(SEED)
    idx = rng.permutation(len(X))
    ntr = int(0.8 * len(X))
    itr, ite = idx[:ntr], idx[ntr:]

    Xtr = torch.tensor(one_hot(X[itr])); ytr = torch.tensor(y[itr])
    Xte = torch.tensor(one_hot(X[ite])); yte = torch.tensor(y[ite])

    # Linea base, accuracy, solo para predecir siempre la clase mayoritaria.
    # Todo modelo debe superarla; si la iguala, no se aprendió nada.
    baseline = max((yte == 0).float().mean().item(),
                   (yte == 1).float().mean().item())
    print(f" Linea base, solo de referencia (clase mayoritaria): {baseline:.4f}")

    model = CNN1D()

    # Pesos de clase, compensan el desbalance. accuracy = linea base, F1 = 0.
    peso1 = (ytr == 0).sum().item() / max((ytr == 1).sum().item(), 1)
    pesos = torch.tensor([1.0, peso1], dtype=torch.float32)
    clase_mayoritaria = 0 if n0 > n1 else 1
    print(f" Clase mayoritaria: {clase_mayoritaria}")
    print(f" Peso aplicado a la clase 1: {peso1:.3f})")

    criterion = nn.CrossEntropyLoss(weight=pesos)
    optimizer = optim.Adam(model.parameters(), lr=1e-2)

    for ep in range(epochs):
        model.train()
        optimizer.zero_grad()
        loss = criterion(model(Xtr), ytr)
        loss.backward()
        optimizer.step()

    model.eval()
    with torch.no_grad():
        pred = model(Xte).argmax(1)

    vp = int(((pred == 1) & (yte == 1)).sum())
    fp = int(((pred == 1) & (yte == 0)).sum())
    fn = int(((pred == 0) & (yte == 1)).sum())
    vn = int(((pred == 0) & (yte == 0)).sum())

    prec = vp / (vp + fp) if (vp + fp) else 0.0
    rec  = vp / (vp + fn) if (vp + fn) else 0.0
    f1   = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    acc  = (vp + vn) / len(yte)
    nclases = int(len(torch.unique(pred)))

    print(f" Accuracy  : {acc:.4f}   (linea base {baseline:.4f})")
    print(f" Precision : {prec:.4f}")
    print(f" Recall    : {rec:.4f}")
    print(f" F1-score  : {f1:.4f}")
    print(f" Clases distintas predichas: {nclases}")
    print(f" Matriz: Real0=[{vn},{fp}]  Real1=[{fn},{vp}]")

    pd.DataFrame([{
        "baseline": baseline, "accuracy": acc, "precision": prec, "recall": rec,
        "f1_score": f1, "true_neg": vn, "false_pos": fp, "false_neg": fn,
        "true_pos": vp, "n_classes_predicted": nclases, "epochs": epochs,
        "n_samples": n_samples, "window_size": window,
        "class_0": n0, "class_1": n1, "minority_weight": peso1,
    }]).to_csv(os.path.join(outdir, "cnn_results.csv"), index=False)

    # Visualizacion de los filtros.
    filtros = model.conv1.weight.detach().numpy()
    fig, axes = plt.subplots(2, 4, figsize=(14, 5))
    for k, ax in enumerate(axes.flat):
        ax.imshow(filtros[k], cmap="RdBu_r", aspect="auto",
                  vmin=-abs(filtros).max(), vmax=abs(filtros).max())
        ax.set_title(f"Filtro {k}", fontsize=9)
        ax.set_yticks(range(4))
        ax.set_yticklabels(["A", "C", "G", "T"], fontsize=8)
        ax.set_xlabel("Posicion", fontsize=8)
    fig.suptitle("Filtros aprendidos por la CNN 1D (rojo = peso +, azul = peso -)")
    plt.tight_layout()
    ruta = os.path.join(figdir, "cnn_filters_or_activations.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  {ruta}")


def version_de(pkg):
    """Pregunta la versión instalada de un paquete"""
    try:
        import importlib.metadata as md
        return md.version(pkg)
    except Exception:
        return "No está instalado"


def version_java():
    try:
        out = subprocess.run(["java", "-version"], capture_output=True,
                             text=True, timeout=10)
        return (out.stderr or out.stdout).splitlines()[0].split('"')[1]
    except Exception:
        return "Se desconoce"


def commit_git():
    """Registra el commit exacto que produjo estos resultados.
    Si hay cambios sin guardar o confirmar, se avisará, en este caso los resultados
    no se podrían reproducir desde el repositorio.

    Se excluyen Results/ y Reports/ del chequeo: los genera esta misma corrida.
    La pregunta que interesa es si el código estaba versionado.

    Se usa la sintaxis :(top,exclude) y no :!  porque el proceso se ejecuta
    dentro del work/ de Nextflow, y git interpreta los pathspec relativos al
    directorio actual. El modificador 'top' los coloca a la raíz del repositorio."""
    try:
        h = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                           capture_output=True, text=True, timeout=10)
        sucio = subprocess.run(["git", "status", "--porcelain", "--",
                               ":(top,exclude)Results",
                               ":(top,exclude)Reports"],
                               capture_output=True, text=True, timeout=10)
        c = h.stdout.strip() or "Sin guardar"
        return c + (" (Con cambios sin guardar)" if sucio.stdout.strip()
                    else " (árbol limpio)")
    except Exception:
        return "No es repositorio git"


def escribir_run_parameters(spark, args, genoma_ref, df_bench, outdir):
    print("\n[f] Registro de parámetros y entorno")

    mejor = df_bench.loc[df_bench["speedup"].idxmax()]

    filas = [
        ("fecha_ejecucion",       pd.Timestamp.now().isoformat(timespec="seconds")),
        ("sistema_operativo",     platform.platform()),
        ("python_version",        sys.version.split()[0]),
        ("java_version",          version_java()),
        ("spark_version",         spark.version),
        ("pyspark_version",       version_de("pyspark")),
        ("torch_version",         version_de("torch")),
        ("numpy_version",         version_de("numpy")),
        ("pandas_version",        version_de("pandas")),
        ("matplotlib_version",    version_de("matplotlib")),
        ("git_commit",            commit_git()),
        ("cpus_logicos",          os.cpu_count()),
        ("seed",                  SEED),
        ("archivo_referencia",    os.path.basename(args.reference)),
        ("archivo_query",         os.path.basename(args.query)),
        ("largo_genoma_bases",    len(genoma_ref)),
        ("n_reads",               args.n_reads),
        ("largos_patron",         ",".join(str(x) for x in LARGOS)),
        ("k_mer",                 K),
        ("particiones",           args.partitions),
        ("benchmark_particiones", args.benchmark_partitions),
        ("repeticiones",          args.repeats),
        ("mejor_speedup",         round(float(mejor["speedup"]), 3)),
        ("mejor_n_particiones",   int(mejor["partitions"])),
        ("cnn_epochs",            args.cnn_epochs),
        ("cnn_samples",           args.cnn_samples),
    ]

    df = pd.DataFrame(filas, columns=["parametro", "valor"])
    ruta = os.path.join(outdir, "run_parameters.csv")
    df.to_csv(ruta, index=False)

    print(f"  {len(df)} Parámetros registrados automáticamente")
    print(f"  {ruta}")
    return df


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
    p.add_argument("--cnn_epochs",  type=int, default=300)
    p.add_argument("--cnn_samples", type=int, default=5000)
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
    print("Genoma :", len(genoma_ref), "bases")

    reads = leer_fasta_reads(args.query, args.n_reads)
    print("Reads leidos :", len(reads))

    patrones = generar_patrones(reads, os.path.basename(args.query))
    print("Patrones :", len(patrones))

    # Broadcast, se encarga de enviar el genoma una vez a cada worker.
    # si no se considera broadcast, spark serializaría 
    # es decir, los aprox 3 MB (que pesan algunos archivos) en cada una de las 360 tareas.
    bc_ref = sc.broadcast(genoma_ref)

    actividad_regex(sc, patrones, bc_ref, args.partitions, args.outdir)

    lista_particiones = [int(x) for x in args.benchmark_partitions.split(",")]

    df_bench = actividad_benchmark(sc, patrones, bc_ref, lista_particiones, args.repeats, args.outdir)

    actividad_recurrencia(genoma_ref, reads, args.outdir)
    actividad_codificacion_conv(genoma_ref, args.outdir)
    actividad_cnn(genoma_ref, args.outdir, args.cnn_epochs, args.cnn_samples)

    escribir_run_parameters(spark, args, genoma_ref, df_bench, args.outdir)

    spark.stop()
    print("\nListo.")


if __name__ == "__main__":
    main()
