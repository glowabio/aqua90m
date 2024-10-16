#!/bin/sh

export DATA=$1
export LON=$2
export LAT=$3
export VAR_LAYER=$4
export VAR_NAME=$5
export TMPDIR=$6
export IDSDIR=$7

export RAND_STRING=$(xxd -l 8 -c 32 -p < /dev/random)

echo "Start..."
echo "Writing as header: "${VAR_NAME}
echo ${VAR_NAME} > $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt
echo "Running gdallocationinfo..."
awk -v LON=$LON -v LAT=$LAT '
NR==1 {
    for (i=1; i<=NF; i++) {
        f[$i] = i
    }
}
{ if(NR>1) {print $(f[LON]), $(f[LAT]) }}
' $DATA   | gdallocationinfo -valonly -geoloc  $VAR_LAYER >> $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt

echo "Running gdallocationinfo done..."
echo "Result (hopefully) written to "$TMPDIR/${VAR_NAME}_${RAND_STRING}.txt
paste -d" " $DATA $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt > $IDSDIR
echo "Result (hopefully) pasted to "$IDSDIR
rm  $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt
echo "now rm ran"
