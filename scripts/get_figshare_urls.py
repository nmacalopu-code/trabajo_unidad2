#!/usr/bin/env python3
"""Lee la metadata publica de Figshare y escribe un TSV
"""
import argparse
import json
import sys


def main():
    p = argparse.ArgumentParser()
    p.add_argument("metadata", help="archivo JSON con la metadata de Figshare")
    p.add_argument("--only", nargs="*", default=None,
                   help="Descargar solo estos archivos, si se omite, se descargarán todos")
    args = p.parse_args()

    with open(args.metadata, "r", encoding="utf-8") as f:
        meta = json.load(f)

    archivos = meta.get("files", [])
    if not archivos:
        sys.exit("Error: la metadata no tiene archivos o revisar el figshare_id.")

    if args.only:
        disponibles = {a["name"] for a in archivos}
        faltan = [n for n in args.only if n not in disponibles]
        if faltan:
            sys.exit("Error: no existen en el registro: " + ", ".join(faltan)
                     + "\n       Disponibles: " + ", ".join(sorted(disponibles)))
        archivos = [a for a in archivos if a["name"] in args.only]

    for a in archivos:
        url    = a.get("download_url", "")
        nombre = a.get("name", "")
        md5    = a.get("supplied_md5", "") or a.get("computed_md5", "")
        print(f"{url}\t{nombre}\t{md5}")


if __name__ == "__main__":
    main()
