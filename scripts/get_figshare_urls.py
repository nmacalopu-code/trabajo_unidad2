#!/usr/bin/env python3
"""
get_figshare_urls.py
====================
Lee la metadata publica de un registro de Figshare y entrega, por salida
estandar, la lista de archivos a descargar en formato TSV:

    <download_url>    <nombre_archivo>    <md5_oficial>    <tamano_bytes>

El md5 se conserva para verificar despues que lo descargado corresponde
exactamente a lo publicado en Figshare. Esa es la evidencia de
reproducibilidad de los datos.

Uso:
    python3 get_figshare_urls.py figshare_metadata.json > file_urls.tsv
    python3 get_figshare_urls.py figshare_metadata.json --only A.fasta B.fasta
"""

import argparse
import json
import sys


def parsear_argumentos():
    parser = argparse.ArgumentParser(
        description="Extrae las URLs de descarga de un registro publico de Figshare."
    )
    parser.add_argument("metadata", help="JSON con la metadata publica del registro.")
    parser.add_argument(
        "--only", nargs="*", default=None, metavar="ARCHIVO",
        help="Descargar solo estos archivos. Si se omite, se listan todos.",
    )
    return parser.parse_args()


def cargar_metadata(ruta):
    """Lee el JSON de metadata y devuelve la lista de archivos del registro."""
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except FileNotFoundError:
        sys.exit(f"ERROR: no se encontro el archivo de metadata '{ruta}'.")
    except json.JSONDecodeError:
        sys.exit(f"ERROR: '{ruta}' no es un JSON valido. Verifique el figshare_id.")

    archivos = metadata.get("files")

    if not archivos:
        sys.exit(
            "ERROR: la metadata no contiene archivos. Causas posibles:\n"
            "  - el registro de Figshare no esta publicado;\n"
            "  - el registro esta bajo embargo;\n"
            "  - el figshare_id es incorrecto."
        )

    return archivos


def main():
    args = parsear_argumentos()
    archivos = cargar_metadata(args.metadata)

    # Filtrado opcional: el registro completo pesa ~3.4 GB, por lo que el
    # pipeline descarga por defecto solo los archivos que necesita.
    if args.only:
        solicitados = set(args.only)
        archivos = [a for a in archivos if a["name"] in solicitados]

        encontrados = {a["name"] for a in archivos}
        faltantes = solicitados - encontrados
        if faltantes:
            sys.exit(
                "ERROR: estos archivos no existen en el registro de Figshare: "
                + ", ".join(sorted(faltantes))
            )

    for archivo in archivos:
        nombre = archivo["name"]
        url = archivo["download_url"]
        md5 = archivo.get("supplied_md5") or archivo.get("computed_md5") or "NA"
        size = archivo.get("size", 0)
        print(f"{url}\t{nombre}\t{md5}\t{size}")

    # Los mensajes informativos van a stderr para no contaminar el TSV.
    print(f"[get_figshare_urls] {len(archivos)} archivo(s) a descargar.", file=sys.stderr)


if __name__ == "__main__":
    main()
