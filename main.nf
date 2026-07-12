#!/usr/bin/env nextflow

// ============================================================
//  Trabajo Unidad 2 - Pipeline reproducible
//  Nextflow + PySpark + Bash
// ============================================================
//  Fase 2: descarga automatica y verificada desde Figshare.
//
//  Los parametros (figshare_id, reference, query, partitions,
//  outdir, download_all) se definen en nextflow.config.
// ============================================================

// ============================================================
//  DOWNLOAD_FIGSHARE
//  Descarga los FASTA desde el registro publico de Figshare y
//  verifica su integridad contra el MD5 que la propia API publica.
// ============================================================
process DOWNLOAD_FIGSHARE {

    tag "figshare:${params.figshare_id}"

    publishDir "${params.outdir}/Problem1", mode: 'copy', pattern: "downloaded_files*.txt"

    input:
    path get_urls_script

    output:
    path "Data/*.fasta",             emit: fastas
    path "downloaded_files.txt",     emit: listado
    path "downloaded_files_md5.txt", emit: checksums
    path "figshare_metadata.json",   emit: metadata

    script:
    // El registro completo pesa ~3.4 GB. Por defecto se descargan solo los
    // dos archivos que el analisis necesita.
    def filtro = params.download_all ? "" : "--only '${params.reference}' '${params.query}'"

    """
    mkdir -p Data

    echo "Consultando metadata del registro Figshare ${params.figshare_id}..."
    curl -sSL --fail "https://api.figshare.com/v2/articles/${params.figshare_id}" -o figshare_metadata.json

    python3 ${get_urls_script} figshare_metadata.json ${filtro} > file_urls.tsv

    while IFS=\$'\\t' read -r url nombre md5_esperado size
    do
        echo "Descargando \$nombre (\$size bytes)..."
        curl -sSL --fail "\$url" -o "Data/\$nombre"

        md5_obtenido=\$(md5sum "Data/\$nombre" | cut -d' ' -f1)

        if [ "\$md5_esperado" != "NA" ] && [ "\$md5_obtenido" != "\$md5_esperado" ]; then
            echo "ERROR: checksum incorrecto en \$nombre" >&2
            echo "  esperado: \$md5_esperado" >&2
            echo "  obtenido: \$md5_obtenido" >&2
            exit 1
        fi

        echo "  MD5 verificado: \$md5_obtenido"
    done < file_urls.tsv

    ls -lh Data/ > downloaded_files.txt
    md5sum Data/*.fasta > downloaded_files_md5.txt

    echo "Descarga completada y verificada."
    """
}

// ============================================================
//  WORKFLOW PRINCIPAL
// ============================================================
workflow {

    // Validacion de parametros. Es preferible fallar de inmediato con un
    // mensaje claro que fallar a mitad del pipeline con un error incomprensible.
    if (!params.figshare_id) {
        error "Falta el parametro --figshare_id. Uso: nextflow run main.nf --figshare_id ID -profile local"
    }

    // Los scripts se pasan como canal, nunca con rutas absolutas.
    ch_get_urls = Channel.fromPath("${projectDir}/scripts/get_figshare_urls.py")

    DOWNLOAD_FIGSHARE(ch_get_urls)

    DOWNLOAD_FIGSHARE.out.fastas
        .flatten()
        .view { archivo -> "Archivo descargado: ${archivo.name}" }
}
