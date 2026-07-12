#!/usr/bin/env nextflow

process DOWNLOAD_FIGSHARE {

    tag "figshare:${params.figshare_id}"

    publishDir "${params.outdir}/Problem1", mode: 'copy', pattern: "downloaded_files*.txt"

    input:
    path get_urls

    output:
    path "Data/*.fasta",             emit: fastas
    path "downloaded_files.txt",     emit: listado
    path "downloaded_files_md5.txt", emit: checksums

    script:
    def solo = params.download_all ? "" : "--only ${params.reference} ${params.query}"
    """
    mkdir -p Data

    curl -sL "https://api.figshare.com/v2/articles/${params.figshare_id}" > metadata.json

    python3 ${get_urls} metadata.json ${solo} > file_urls.tsv

    if [ ! -s file_urls.tsv ]; then
        echo "Error: no se obtuvo ninguna URL desde Figshare."
        exit 1
    fi

    while IFS= read -r linea; do
        url=\$(echo "\$linea" | cut -f1)
        nombre=\$(echo "\$linea" | cut -f2)
        md5_esperado=\$(echo "\$linea" | cut -f3)

        echo ">>> Descargando \$nombre"
        curl -sL "\$url" -o "Data/\$nombre"

        md5_real=\$(md5sum "Data/\$nombre" | cut -d' ' -f1)

        if [ "\$md5_real" != "\$md5_esperado" ]; then
            echo "Error: MD5 no coincide para \$nombre"
            echo "  esperado: \$md5_esperado"
            echo "  obtenido: \$md5_real"
            exit 1
        fi
        echo "    MD5 verificado OK"
    done < file_urls.tsv

    ls -lh Data/ > downloaded_files.txt
    md5sum Data/*.fasta > downloaded_files_md5.txt
    """
}

workflow {

    if( !params.figshare_id )
        error "Falta --figshare_id. Ejemplo: nextflow run main.nf --figshare_id 32968955 -profile local"

    log.info """
    PIPELINE - Neyling Yuriko Teresa Macalopú Rimachi
     figshare_id  : ${params.figshare_id}
     reference    : ${params.reference}
     query        : ${params.query}
     download_all : ${params.download_all}
     outdir       : ${params.outdir}
    """

    ch_get_urls = Channel.fromPath("${projectDir}/scripts/get_figshare_urls.py")

    DOWNLOAD_FIGSHARE(ch_get_urls)
}
