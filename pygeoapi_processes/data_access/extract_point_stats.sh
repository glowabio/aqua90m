#!/bin/sh

export DATA=$1
export LON=$2
export LAT=$3
export VAR_LAYER=$4
export VAR_NAME=$5
export TMPDIR=$6
export RES_FILE=$7
export RAND_STRING=$(xxd -l 8 -c 32 -p < /dev/random)

# Documentation of input params:
# DATA: Text file with two space-separated, named columns for longitude and latitude.
# LON:  Name of the longitude column.
# LAT:  Name of the latitude column.
# VAR_LAYER:  Path to the raster file from which to get the variable values.
# VAR_NAME: Name of the variable (will be used for column header and result file name).
# TMPDIR: Path to a directory where gdal will store its immediate results, i.e. the extracted subcatchment_ids and/or basin_ids will be stored temporarily (will be removed before finishing).
# RES_FILE: Path to the file where to store the final result, which is the text file in DATA with one or two columns added ("subcatchment" and/or "basin").
#
# Example DATA:
#cat ~/test_inputs.txt
#lon lat
#8.4 51.9
#7.9 50.2
#
# Example temporary result written by gdal:
#cat /tmp/soil_type_c54966d7faeb70f0.txt
#soil_type
#4
#5
#
# Example RES_FILE:
#cat ~/test_results.txt
#lon lat soil_type
#8.4 51.9 4
#7.9 50.2 5



# add header to temporary result file:
echo ${VAR_NAME} > $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt

# run gdallocationinfo (will write result to temporary result file):
awk -v LON=$LON -v LAT=$LAT '
NR==1 {
    for (i=1; i<=NF; i++) {
        f[$i] = i
    }
}
{ if(NR>1) {print $(f[LON]), $(f[LAT]) }}
' $DATA   | gdallocationinfo -valonly -geoloc $VAR_LAYER >> $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt

# merge input lon-lat columns with temporary result file to one result file
#echo "Result (hopefully) written to "$TMPDIR/${VAR_NAME}_${RAND_STRING}.txt
paste -d" " $DATA $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt > $RES_FILE
#echo "Result (hopefully) pasted to "$RES_FILE

# remove temporary result file
rm $TMPDIR/${VAR_NAME}_${RAND_STRING}.txt

