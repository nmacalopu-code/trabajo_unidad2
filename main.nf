#!/usr/bin/env nextflow

// ============================================================
//  Trabajo Unidad 2 - Pipeline reproducible
//  Nextflow + PySpark + Bash
// ============================================================
//  Esta es la version inicial (Fase 1).
//  Por ahora solo tiene un proceso de arranque (SETUP) para
//  comprobar que el pipeline se ejecuta correctamente.
//  En las siguientes fases agregaremos:
//    - DOWNLOAD_FIGSHARE   (descarga automatica de datos)
//    - RUN_PROBLEM1_PYSPARK (analisis con PySpark)
//    - RUN_EXTERNAL_TASKS  (procesos externos: Bash + reporte)
//    - FINAL_REPORT        (reporte y grafico propio)
// ============================================================

nextflow.enable.dsl = 2

// ------------------------------------------------------------
//  Parametros por defecto (se pueden cambiar desde la terminal)
// ------------------------------------------------------------
params.figshare_id = null            // ID del registro publico de Figshare
params.reference   = "input1.fasta"  // archivo FASTA de referencia
params.query       = "input2.fasta"  // archivo FASTA de consulta
params.partitions  = 4               // numero de particiones de Spark
params.outdir      = "Results"       // carpeta de salida

// ------------------------------------------------------------
//  Proceso de arranque
//  Solo escribe un archivo con la informacion de la ejecucion.
//  Sirve para verificar que Nextflow funciona antes de agregar
//  la logica pesada.
// ------------------------------------------------------------
process SETUP {

    publishDir "${params.outdir}", mode: 'copy'

    output:
    path "pipeline_info.txt"

    script:
    """
    echo "Pipeline Unidad 2 - inicializado correctamente" > pipeline_info.txt
    echo "Fecha de ejecucion : \$(date)"                  >> pipeline_info.txt
    echo "figshare_id        : ${params.figshare_id}"     >> pipeline_info.txt
    echo "referencia         : ${params.reference}"       >> pipeline_info.txt
    echo "query              : ${params.query}"           >> pipeline_info.txt
    echo "particiones (Spark): ${params.partitions}"      >> pipeline_info.txt
    echo "carpeta de salida  : ${params.outdir}"          >> pipeline_info.txt
    """
}

// ------------------------------------------------------------
//  Workflow principal: define el orden de los procesos.
//  Por ahora solo ejecuta SETUP.
// ------------------------------------------------------------
workflow {
    SETUP()
}
