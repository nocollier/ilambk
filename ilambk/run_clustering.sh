#!/bin/bash

RUN_CLUSTERING=1
clust=("8" "16" "32" "64")
nclust=${#clust[@]}

if [ $RUN_CLUSTERING -eq 1 ]
then
 for((k=0; k<${nclust}; k++)) do
  mkdir -p k${clust[k]}
  cd k${clust[k]}
   cp ../job.sh .
   sh job.sh ${clust[k]}
  cd ..
 done
fi
