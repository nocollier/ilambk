#!/bin/bash                                                                                                             

CASE="CASENAME"
DATA_LOCATION=DATALOCATION
mkdir -p results
mkdir -p logfiles
mkdir -p profiles
ROWS=`wc -l ${DATA_LOCATION}/obs.raw.${CASE} | awk '{print $1}'`
COLS=`head -n 1 ${DATA_LOCATION}/obs.raw.${CASE}| wc -w`
NUMCLUST=$1
NALIQUOT=128
SEED_PROCS=8
SEED_PROCS_P2=4
NUMTRIALS=10
NUMPARTS=$((1*${NUMTRIALS}))
SAMPLE=$((${ROWS} / ${NUMPARTS}))
MACHINE=andes
STANDARDIZE=CLUSTERBIN/standardize.${MACHINE}
RAND_SEEDS=CLUSTERBIN/rand_seeds.${MACHINE}
PRAND_SAMPLE=CLUSTERBIN/prand_sample_bj.${MACHINE}
PSAMPLE=CLUSTERBIN/psample_dataset_new2.${MACHINE}
SEED_BINARY=CLUSTERBIN/pcluster_Ndist_accel.${MACHINE}
CLUSTER_PROCS=16
CLUSTER_BINARY=CLUSTERBIN/pcluster_Ndist_accel.${MACHINE}

ln -s ${DATA_LOCATION}/obs.bin.${CASE}
ln -s ${DATA_LOCATION}/mean.${CASE}.orig
ln -s ${DATA_LOCATION}/stddev.${CASE}.orig

###############################################################################                                         
# Standardize the ASCII input data and write binary file                                                                
###############################################################################                                         
if [ ! -f obs.bin.${CASE} ]; then
   echo "`date` -- Standardizing obs.raw.${CASE}"
   time ${STANDARDIZE} -b -v -r ${ROWS} -c ${COLS} -m mean.${CASE}.orig -s stddev.${CASE}.orig -o obs.bin.${CASE} ${DAT\
A_LOCATION}/obs.raw.${CASE}
fi
###############################################################################                                         
# Find initial seeds using the Bradley method                                                                           
###############################################################################                                         
if [ ! -f seeds.out.${CASE}.${NUMCLUST} ]; then
   echo "`date` -- Finding ${NUMCLUST} seeds using the Bradley method"
   ################################                                                                                     
   # PHASE 1: Loop over t trials                                                                                        
   ################################                                                                                     
   echo "`date` -- PHASE 1: Loop over t=${NUMTRIALS} trials"
   # Random samples should be 1/${NUMTRIALS} % of original data                                                         
   echo "Random samples will be ${SAMPLE} obs long"
   for (( t=0 ; t < ${NUMTRIALS}; t++ )) ; do
      if [ ! -f cluster.stats.${CASE}_sample${t}.${NUMCLUST} ]; then
         # Create a random subset of the data                                                                           
         echo "Extracting a random subset of data for trial ${t}"
         set -x
         time mpirun -n ${SEED_PROCS} ${PSAMPLE} --rand_sampler -r ${ROWS} -c ${COLS} -k ${SAMPLE} -x ${NUMPARTS} -o ob\
s.bin.${CASE}_sample${t} obs.bin.${CASE}
         set +x
         echo "Generating random seeds for trial ${t}"
         time ${RAND_SEEDS} -k ${NUMCLUST} -r ${SAMPLE} -c ${COLS} -o seeds.out.${CASE}_sample${t}.${NUMCLUST} obs.bin.\
${CASE}_sample${t}
         echo "Clustering random subset of data for trial ${t}"
         SUB_NALIQUOT=$((${SEED_PROCS} * 2))
         LOGFILE=pcluster_Ndist.${CASE}_sample${t}.${NUMCLUST}.`date "+%Y%m%d.%02k%M%S"`.log
         time mpirun -n ${SEED_PROCS} ${SEED_BINARY} -z clust.res -p seeds.res -k ${NUMCLUST} -r ${SAMPLE} -c ${COLS} -\
b ${SUB_NALIQUOT} -i seeds.out.${CASE}_sample${t}.${NUMCLUST} -o seeds.out.${CASE}_sample${t}.${NUMCLUST}.final -a clus\
ters.out.${CASE}_sample${t}.${NUMCLUST} obs.bin.${CASE}_sample${t} >& ${LOGFILE}
         mv cluster.stats cluster.stats.${CASE}_sample${t}.${NUMCLUST}
      fi
   done
   ################################                                                                                     
   # PHASE 2: All the prior final centroids become the dataset and then                                                 
   # try on each of those prior final centroids to find the best                                                        
   ################################                                                                                     
   echo "`date` -- PHASE 2: Loop over t=${NUMTRIALS} trials using Phase 1 centroids"
   P2SAMPLE=$((${NUMCLUST} * ${NUMTRIALS}))
   echo "Creating Phase 2 dataset"
   rm -f obs.raw.${CASE}_phase2
   for (( t=0 ; t < ${NUMTRIALS}; t++ )) ; do
      cat seeds.out.${CASE}_sample${t}.${NUMCLUST}.final >> obs.raw.${CASE}_phase2
   done
   echo "Converting Phase 2 dataset to binary"
   time ${STANDARDIZE} -n -b -v -r ${P2SAMPLE} -c ${COLS} -m mean.${CASE}_phase2.orig -s stddev.${CASE}_phase2.orig -o \
obs.bin.${CASE}_phase2 obs.raw.${CASE}_phase2
   for (( t=0 ; t < ${NUMTRIALS}; t++ )) ; do
      echo "Clustering Phase 2 dataset using centroids from Phase 1 trial ${t}"
      SUB_NALIQUOT=$((${SEED_PROCS_P2} * 2))
      LOGFILE=pcluster_Ndist.${CASE}_phase2_sample${t}.${NUMCLUST}.`date "+%Y%m%d.%02k%M%S"`.log
      time mpirun -n ${SEED_PROCS_P2} ${SEED_BINARY} -z clust.res -p seeds.res -k ${NUMCLUST} -r ${P2SAMPLE} -c ${COLS}\
 -b ${SUB_NALIQUOT} -i seeds.out.${CASE}_sample${t}.${NUMCLUST}.final -o seeds.out.${CASE}_phase2_sample${t}.${NUMCLUST\
}.final -a clusters.out.${CASE}_phase2_sample${t}.${NUMCLUST} obs.bin.${CASE}_phase2 >& ${LOGFILE}
      mv cluster.stats cluster.stats.${CASE}_phase2_sample${t}.${NUMCLUST}
   done
   # Determine the winning set of cluster analyses                                                                      
   rm -f stats.${CASE}_phase2
   for (( t=0 ; t < ${NUMTRIALS}; t++ )) ; do
      echo "${t} `grep 'Mean centroid to obs distance:' cluster.stats.${CASE}_phase2_sample${t}.${NUMCLUST} | awk '{pri\
ntf "%s\n", $NF;}'`" >> stats.${CASE}_phase2                                                                            
   done                                                                                                                 
   WINNER=`sort -k 2 -n stats.${CASE}_phase2 | head -1 | awk '{print $1}'`                                              
   echo "`date` --  Winning set of centroids is from trial ${WINNER}"                                                   
   # Setup winning final centroids from Phase 2 as initial centroids for the real cluster analysis of the full dataset  
   ln -s seeds.out.${CASE}_phase2_sample${WINNER}.${NUMCLUST}.final seeds.out.${CASE}.${NUMCLUST}                       
else                                                                                                                    
   echo "`date` -- Warning: Using existing seeds found in seeds.out.${CASE}.${NUMCLUST}"                                
fi                                                                                                                      
###############################################################################                                         
# Prepare files for clustering                                                                                          
rm -f seeds.in                                                                                                          
rm -f pc.in                                                                                                             
export LOGFILE=pcluster_Ndist.${CASE}.${NUMCLUST}.`date "+%Y%m%d.%02k%M%S"`.log                                         
# Cluster                                                                                                               
echo "`date` -- Clustering obs.bin.${CASE} into ${NUMCLUST} clusters; output in ${LOGFILE}"                             
# Penguins                                                                                                              
time mpirun -n ${CLUSTER_PROCS} ${CLUSTER_BINARY} -z clust.res -p seeds.res -k ${NUMCLUST} -r ${ROWS} -c ${COLS} -b ${N\
ALIQUOT} -i seeds.out.${CASE}.${NUMCLUST} -o results/seeds.out.${CASE}.${NUMCLUST}.final -a results/clusters.out.${CASE\
}.${NUMCLUST} obs.bin.${CASE} >& ${LOGFILE}                                                                             
## If using Masterless code                                                                                             
echo "`date` -- Starting the final clustering step"                                                                     
                                                                                                                        
# Unstandardize final centroids                                                                                         
echo "`date` -- Unstandardizing final centroids"                                                                        
# Penguins                                                                                                              
time ${STANDARDIZE} -u -v -r ${NUMCLUST} -c ${COLS} -M mean.${CASE}.orig -S stddev.${CASE}.orig -o results/seeds.out.${\
CASE}.${NUMCLUST}.final.unstd results/seeds.out.${CASE}.${NUMCLUST}.final                                               
echo "`date` -- Done"         
