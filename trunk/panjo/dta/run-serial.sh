#!/bin/sh
#PBS -l nodes=1,walltime=12:00:00
#PBS -N panjo
#PBS -o job.out
#PBS -e job.err
#PBS -q batch16 
#PBS -V

for i in `ls ~/panjo/dta/test_mats/*.mat`; 
do
    for j in 2 4 5 10 20 40;
    do
      ~/panjo/src/panjo-serial -f $i >> ~/panjo/dta/test_mats/run_times_serial.out 
      sleep 2
    done
done


