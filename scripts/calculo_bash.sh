#!/usr/bin/env bash
# calculo_bash.sh  para proceso 2 adaptado de calculoBashTask
# David A. Castro S., repositorio tap_pipeline_gen original
# Adaptacion: sin rutas absolutas /home/dabits/, parametrizado por argumentos,
# conectado a los fastas descargados en el proceso 1.
#
# Busca en el genoma una semilla del inicio de cada read con awk index(),
# midiendo el tiempo. Es el equivalente en bash del proceso 1 (que busca con
# PySpark), lo que permite compararlos después en el proceso 3.
#
# Uso: bash calculo_bash.sh <referencia.fasta> <query.fasta> <salida.csv> <n_reads> [semilla]

archivo_referencia="$1"
archivo_query="$2"
archivo_csv="$3"
n_reads="${4:-20}"
largo_semilla="${5:-16}" 
#se consideró una semilla de 16 por que en un genoma de aproximadamente 3M bases, el número esperado de
# apariciones por azar (G/4^L) es 0.0007, asi que una coincidencia es casi con certeza
# real y no ruido. Por debajo de 12 la semilla aparece por azar (con 10, el
# valor del config original, se esperan aproximadamente 2.8 apariciones aleatorias).

# Cronómetro robusto en ms. date +%s%3N falla en algunos WSL (19 digitos salían restas negativas # muy grandes). date +%s.%N pasado a ms con awk es estable.
get_time_ms() { date +%s.%N | awk '{ printf "%.0f", $1 * 1000 }'; }

echo "id_read,longitud_read,largo_semilla,posicion,encontrado,tiempo_ms" > "$archivo_csv"

genoma_plano=$(mktemp)
# Genoma en una sola linea y en mayúsculas.
grep -v "^>" "$archivo_referencia" | tr -d '\r\n' | tr 'a-z' 'A-Z' > "$genoma_plano"

buscar_posicion() {
    awk -v pat="$1" '{ p = index($0, pat); if (p > 0) { print p; exit } }' "$genoma_plano"
}

id_read=""
seq=""
procesados=0

procesar_read() {
    local id="$1"
    local secuencia="$2"
    [ -z "$secuencia" ] && return

    local inicio fin elapsed longitud semilla posicion encontrado
    longitud=${#secuencia}
    semilla=$(echo "${secuencia:0:$largo_semilla}" | tr 'a-z' 'A-Z')

    inicio=$(get_time_ms)
    posicion=$(buscar_posicion "$semilla")
    fin=$(get_time_ms)
    elapsed=$((fin - inicio))

    if [ -n "$posicion" ]; then
        encontrado="si"
    else
        encontrado="no"
        posicion="-1"
    fi

    echo "${id},${longitud},${largo_semilla},${posicion},${encontrado},${elapsed}" >> "$archivo_csv"
}

while IFS= read -r linea; do
    linea=$(echo "$linea" | tr -d '\r')
    if [[ "$linea" == ">"* ]]; then
        if [ -n "$id_read" ]; then
            procesar_read "$id_read" "$seq"
            procesados=$((procesados + 1))
            [ "$procesados" -ge "$n_reads" ] && break
        fi
        id_read=$(echo "$linea" | sed 's/^>//' | awk '{print $1}')
        seq=""
    else
        seq="${seq}${linea}"
    fi
done < "$archivo_query"

if [ -n "$id_read" ] && [ "$procesados" -lt "$n_reads" ]; then
    procesar_read "$id_read" "$seq"
fi

rm -f "$genoma_plano"
echo "calculo_bash.sh: terminado. Resultados en $archivo_csv"
